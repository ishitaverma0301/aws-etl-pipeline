## Pipeline Operations Runbook

Operational reference for the US Flight Data Pipeline. Covers execution modes, watermark semantics, failure handling, and common SRE tasks.

---

### Overview

The pipeline runs as an **AWS Step Functions** state machine that orchestrates three Glue jobs sequentially:

`Bronze (Ingestion) → Silver (DQ Check) → Gold (Transformation)`

Each layer is stateless between runs except for a single SSM watermark and the state markers it writes to its own S3 bucket. **Bronze** uses a **leapfrog watermark** and **Silver** uses a **contiguous high-water mark** in a **two-phase** month-grain execution model; **Gold** uses a **change-aware year loop** that re-processes any year whose Silver frontier has moved. Both patterns self-heal transient failures on the next scheduled run.

---

### Operating Modes

#### Incremental Mode (default)

Triggered with input `{}` or `{"reprocess": "false"}`. This is the mode used by scheduled daily runs.

##### Bronze & Silver — Two-Phase, Leapfrog & Contiguous

**Phase 1 — Gap Fill**
1. Read the layer's SSM watermark.
2. Scan every month between `next_month(BASELINE)` and the watermark.
3. Retry any month whose per-layer completion marker is missing.
4. The watermark is **not touched** in this phase.

**Phase 2 — New Months**
1. Iterate from `next_month(watermark)` to the upstream ceiling:
   - Bronze ceiling = current month (UTC)
   - Silver ceiling = Bronze watermark
2. Process each month independently.
3. Bronze advances its watermark to the highest successful or skipped month (Leapfrog), while Silver advances its watermark contiguously based on sequential successes.
4. Failed or missing months leave behind state markers that Phase 1 picks up on the next run.

##### Gold — Change-Aware Year Loop

1. Iterate every year from `BASELINE` year to the Silver-ceiling year (derived from `last_silver_dtm`).
2. For each year, compare the `latest_silver_month` recorded in that year's `_PROCESSED` marker body against the current latest Silver month in that year:
   - **Marker matches** → skip (year's Gold already reflects current Silver state).
   - **Marker stale or missing** → re-process the full year atomically (read all Silver months for the year, rewrite `year=YYYY/{table}/` for each per-month Gold table).
   - **`CODE:` `_FAILED` marker present** → skip with failure (needs manual intervention).
3. After the year loop, a cross-year `monthly_trend` recompute reads the accumulated `airline_monthly_kpi` across all years, derives MoM / YoY / rank columns, and splits the result back into each year's folder.
4. `last_gold_dtm` advances to `max(latest_silver_month)` across every year that was re-processed or already current.

**Why leapfrog / change-aware?** A transient failure (HTTP 404, Spark OOM, upstream delay) never halts the pipeline. Bronze / Silver watermarks keep moving and their next-run Phase 1 heals the gap; Gold re-evaluates every year on every run and picks up any Silver changes automatically. No operator intervention, no manual watermark rewinds.

#### Reprocess Mode

Triggered with an explicit window. Bronze and Silver use month bounds; Gold uses year bounds:

```json
{
  "reprocess": "true",
  "start_date": "2024-01",
  "end_date": "2024-06",
  "start_year": "2024",
  "end_year": "2024"
}
```

**Behavior**
- Bronze / Silver reload the inclusive `start_date` → `end_date` month range.
- Gold reloads the inclusive `start_year` → `end_year` year range (full-year atomic rewrite per year).
- Watermarks are **not modified** — incremental state is fully preserved.
- Phase 1 gap-fill (Bronze / Silver) is **skipped** and Gold's marker-based skip is bypassed — the listed window is always re-processed.
- `end_date` / `end_year` are optional and default to the upstream ceiling.
- Validated invariants: `start_date ≤ end_date ≤ upstream ceiling` and `start_year ≤ end_year ≤ Silver-ceiling year`.

**Typical use cases**
- Source-side corrections republished by BTS.
- Rerunning after a DQ rule or aggregation change.
- Backfilling historical months.

---

### Watermark Chain

| Layer  | SSM Parameter                                  | Role                           |
|--------|------------------------------------------------|--------------------------------|
| Bronze | `/ishita-project1/state/last_load_dtm`         | Bronze ceiling for Silver      |
| Silver | `/ishita-project1/state/last_silver_dtm`       | Silver ceiling for Gold        |
| Gold   | `/ishita-project1/state/last_gold_dtm`         | Analytics-ready frontier       |

Each layer enforces the invariant **`my_watermark ≤ upstream_watermark`**. Silver never processes beyond Bronze's watermark; Gold never processes beyond Silver's.

---

### Watermark Safety Rules

| Rule                        | Description                                                                                                       |
|-----------------------------|-------------------------------------------------------------------------------------------------------------------|
| Never rewind                | Watermarks only move forward, never backward                                                                      |
| Conditional advance (Phase 2) | Bronze leaps to max success; Silver advances strictly contiguously; gaps tracked via markers                    |
| Never outpace upstream      | Silver cannot exceed Bronze; Gold cannot exceed Silver                                                            |
| Gap-fill isolation          | Phase 1 retries gaps behind the watermark but never modifies it                                                   |
| Reprocess isolation         | Reprocess runs do not touch watermarks; Phase 1 is also skipped                                                   |
| Bootstrap                   | If a watermark is missing from SSM, it initializes from the `baseline` parameter                                  |

---

### Per-Layer Completion Markers

| Layer  | Marker                                                   | Written On              | Notes                                                |
|--------|----------------------------------------------------------|-------------------------|------------------------------------------------------|
| Bronze | `flights/year=YYYY/month=MM/_FAILED`                     | `missing` · `failed`    | Body prefixed `SOURCE:` (auto-retry) or `CODE:` (manual) |
| Silver | `_markers/flights/year=YYYY/month=MM/_FAILED`            | `missing` · `failed`    | Tracks explicit errors; missing `cleaned/` is implicit gap |
| Gold   | `_state/year=YYYY/_PROCESSED`                            | `success` only          | Body stores `latest_silver_month=YYYY-MM`            |
| Gold   | `_markers/gold/year=YYYY/_FAILED`                        | `missing` · `failed`    | Tracks explicit year-level failures                  |

---

### Failure Handling

#### Per-Month Failures

Each month is processed independently; a failure in one month does not abort subsequent months. Failed months are logged and surfaced in the SNS summary.

A layer raises `RuntimeError` (causing the Step Functions execution to fail) **only on total failure** — i.e., every attempted month failed and nothing succeeded or was skipped. Partial failures surface as `PARTIAL SUCCESS` and are left for the next run to recover via Phase 1.

#### Step Functions Retry

- Each Glue job step auto-retries `Glue.ConcurrentRunsExceededException` up to 2 attempts (60s interval, 2× backoff).
- All other errors are routed to the `Pipeline-Failed` terminal state, which emits an SNS alert before failing the execution.

#### Network Resilience (Bronze)

- HTTP downloads retry 3× with exponential backoff (2s → 4s → 8s).
- S3 uploads retry 3× with the same backoff policy.
- HTTP 403 / 404 are classified as `missing` (source-side) and written as `SOURCE:` markers for Phase 1 auto-retry.

---

### SNS Notifications

Every run emits an email summary via SNS with:

- Overall status — `SUCCESS` / `PARTIAL SUCCESS` / `FAILED` / `NO NEW DATA`
- Run mode, duration, and date window
- Watermark delta (old → new)
- Per-month status breakdown with icons
- DQ rejection statistics (Silver only)

**Subject format:** `[STATUS] Glue Job: <job-name>`
A high-rejection-rate month in Silver prefixes the subject with `[ALERT]`.

---

### Monitoring Checklist

| Check                     | Location                           | Frequency         |
|---------------------------|------------------------------------|-------------------|
| Job success / failure     | Step Functions console             | After each run    |
| Rejection rate spikes     | CloudWatch logs (Silver job)       | After each run    |
| Watermark drift           | SSM Parameter Store                | Weekly            |
| Orphan `_FAILED` markers  | Bronze S3 bucket                   | Weekly            |
| S3 object counts / sizes  | S3 bucket metrics                  | Monthly           |
| SNS delivery              | SNS delivery logs                  | On failure        |

---

### Common Operations

#### Check current watermarks

```bash
aws ssm get-parameters --names \
  "/ishita-project1/state/last_load_dtm" \
  "/ishita-project1/state/last_silver_dtm" \
  "/ishita-project1/state/last_gold_dtm" \
  --query "Parameters[*].[Name,Value]" --output table
```

#### Manually advance a watermark

```bash
aws ssm put-parameter \
  --name "/ishita-project1/state/last_silver_dtm" \
  --value "2024-08" \
  --type String \
  --overwrite
```

#### View recent state-machine executions

```bash
aws stepfunctions list-executions \
  --state-machine-arn <ARN> \
  --max-results 5
```

#### List outstanding Bronze source-retry markers

```bash
aws s3api list-objects-v2 \
  --bucket <bronze-bucket> \
  --prefix flights/ \
  --query "Contents[?ends_with(Key, '/_FAILED')].[Key]" \
  --output text
```

#### Force a targeted reprocess

```bash
aws stepfunctions start-execution \
  --state-machine-arn <ARN> \
  --input '{"reprocess":"true","start_date":"2024-03","end_date":"2024-03"}'
```