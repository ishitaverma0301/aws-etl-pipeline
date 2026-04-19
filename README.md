<p align="center">
  <img src="https://img.shields.io/badge/Architecture-Medallion-DAA520?style=for-the-badge&logo=databricks&logoColor=white"/>
  <img src="https://img.shields.io/badge/Step_Functions-Orchestration-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white" />
  <img src="https://img.shields.io/badge/AWS_Glue-ETL-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white" />
  <img src="https://img.shields.io/badge/PySpark-3.x-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/EventBridge-Trigger-232F3E?style=for-the-badge&logo=amazon-eventbridge&logoColor=white"/>
  <img src="https://img.shields.io/badge/SNS-Alerting-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white" />
  <img src="https://img.shields.io/badge/CloudFormation-IaC-232F3E?style=for-the-badge&logo=amazonaws&logoColor=white" />
</p>

---

## SkyLedger: High-Volume ETL & Data Quality Framework

A production-grade, serverless ETL pipeline that ingests, validates, and transforms **US domestic flight data** from the Bureau of Transportation Statistics through a **Bronze → Silver → Gold** medallion architecture on AWS. Powered by **PySpark on AWS Glue** and orchestrated by an **AWS Step Functions** state machine, the pipeline is designed for unattended daily operation with automatic gap recovery and zero manual intervention.

---

### Project Highlights

- **Self-healing execution model** — Bronze and Silver run a two-phase *gap-fill* + *new months* pass; Gold uses a change-aware single-pass loop that re-processes any year whose Silver frontier has moved. Transient failures recover automatically on the next scheduled run across all three layers.
- **Resilient watermarks** — Bronze uses a Leapfrog watermark that advances to the highest successful month, while Silver strictly enforces a contiguous high-water mark. Gaps are tracked via durable state markers (`_FAILED`, `_PROCESSED`) in S3.
- **Single-pass PySpark DQ engine** — 16 data-quality rules plus deduplication applied in one Spark SQL pass for minimum shuffle cost.
- **Year-grain Gold idempotency** — each Gold year stores its last-seen Silver month in the `_PROCESSED` marker body; re-running is a cheap skip unless new Silver months have arrived, in which case the full year is atomically rewritten.
- **Externalized SQL artifacts** — Gold aggregation logic lives in versioned [`.sql` files](sql/gold) loaded from S3 at runtime, so analysts can ship query changes without redeploying the Glue job. See [`sql/README.md`](sql/README.md) for the runtime contract and deployment flow.
- **Atomic writes via temp-staging** — no partial or corrupt outputs are ever visible to downstream consumers.
- **Full Infrastructure-as-Code** — the entire stack (S3, Glue, Step Functions, SSM, SNS, IAM) deploys from a single CloudFormation template.
- **Structured SNS alerting** — every run emits an email summary with per-month breakdown, rejection stats, and watermark deltas.

---

### Architecture

![skyledger-aviation-analytics-etl-pipeline](./docs/images/medallion_architecture.svg)

---

### Step Function | State Machine

The pipeline is orchestrated by a Step Functions state machine with two execution paths — **incremental** (default daily run) and **reprocess** (operator-triggered targeted reload) — plus automatic retry on concurrency limits and centralized failure routing to SNS.

![skyledger-aviation-analytics-etl-pipeline](./docs/images/statemachine.svg)

---

### Execution Model — Leapfrog & Contiguous HWM

Bronze and Silver run a **two-phase** pattern (gap-fill + new months) at month grain. Gold uses a **change-aware year loop** — unit of work is the year, not the month.

**Bronze / Silver — Phase 1 Gap Fill**
Scans months already behind the layer's watermark that are missing their per-layer completion marker and retries them. The watermark is **not touched** in this phase.

**Bronze / Silver — Phase 2 New Months**
Iterates from `next_month(watermark)` to the upstream ceiling (Bronze → current month; Silver → Bronze ceiling). Bronze advances its watermark to the maximum successful or skipped month (Leapfrog), while Silver advances strictly contiguously only if the next sequential month succeeds or is skipped.

**Gold — Year Loop**
Iterates every year from `BASELINE` year through the Silver-ceiling year. For each year, compares the `latest_silver_month` recorded in that year's `_PROCESSED` marker against the current latest Silver month in that year:
- Marker matches → **skip** (year's Gold already reflects current Silver state).
- Marker stale or missing → **re-process** the full year atomically (read all Silver months for the year, rewrite `year=YYYY/{table}/` for each Gold table).
- `CODE:` `_FAILED` marker present → **skip with failure** (needs manual intervention).

After the year loop, a cross-year `monthly_trend` recompute reads the accumulated `airline_monthly_kpi` across all years, derives MoM / YoY / rank columns, and splits the result back into each year's folder.

**Why this matters.** Transient failures (HTTP 404, upstream delays, Spark OOM) no longer block the pipeline. Bronze/Silver watermarks advance safely and their next-run Phase 1 heals gaps left behind; Gold re-evaluates every year on every run and picks up any Silver changes automatically. No operator intervention, no manual watermark rewinds.

**Failure semantics.** A layer raises `RuntimeError` (failing the Step Functions execution) **only on total failure** — every attempted unit failed and nothing succeeded or was skipped. Partial failures surface as `PARTIAL SUCCESS` in the SNS summary and are left for the next run to recover.

#### Per-Layer Completion Markers

| Layer  | Marker Location                                            | Written On              | Semantics                                                       |
|--------|------------------------------------------------------------|-------------------------|-----------------------------------------------------------------|
| Bronze | `flights/year=YYYY/month=MM/_FAILED`                       | `missing` · `failed`    | Body prefixed `SOURCE:` (retryable) or `CODE:` (manual)         |
| Silver | `_markers/flights/year=YYYY/month=MM/_FAILED`              | `missing` · `failed`    | Tracks explicit errors; absence of `cleaned/` triggers Phase 1 retry |
| Gold   | `_state/year=YYYY/_PROCESSED` (body: `latest_silver_month=YYYY-MM`) | `success` only | Change-aware — stale body triggers full-year rewrite on next run |
| Gold   | `_markers/gold/year=YYYY/_FAILED`                          | `missing` · `failed`    | Body prefixed `SOURCE:` (auto-retry up to 14×, then escalates to `CODE:`) |

---

### Key Features

| Feature                              | Description                                                                                                                       |
|--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| **Medallion Architecture** | Bronze (raw) → Silver (validated) → Gold (aggregated) with explicit data contracts between layers                                 |
| **PySpark on AWS Glue** | Silver and Gold layers run distributed Spark SQL for DQ, dedup, and aggregation at scale                                          |
| **Self-Healing Execution** | Bronze / Silver two-phase (gap-fill + new months) and Gold change-aware year loop — transient failures auto-retry on the next run |
| **Watermark State Management** | SSM-backed watermarks enforce strict layer ordering — downstream never outpaces upstream                                          |
| **16 Data Quality Rules** | Null, range, logical consistency, and duplicate checks in a single-pass Spark SQL query                                           |
| **Year-Grain Gold Idempotency** | Each Gold year's `_PROCESSED` marker stores its last-seen Silver month — year rewrites only when Silver has advanced              |
| **Externalized SQL Artifacts** | 8 Gold aggregation queries stored as versioned `.sql` files in S3 — analysts can patch logic without redeploying the Glue job ([`sql/`](sql/)) |
| **Atomic Writes** | Temp-staging pattern guarantees no partial or corrupt Gold outputs are visible to downstream consumers                            |
| **Exponential Backoff Retry** | HTTP downloads and S3 uploads retry 3× with 2s → 4s → 8s backoff                                                                 |
| **Step Functions Orchestration** | Dual-path state machine (incremental / reprocess) with retry policies and centralized SNS failure routing                         |
| **Structured SNS Alerts** | Per-run email summary: overall status, watermark delta, per-month breakdown, DQ rejection stats                                   |
| **Infrastructure as Code** | Single CloudFormation template provisions S3, Glue, Step Functions, SSM parameters, SNS topic, and IAM roles                      |

---

### Gold Layer Output Tables

The Gold layer produces **eight analytical tables** per year using PySpark SQL. Output is laid out as `s3://<gold-bucket>/year=YYYY/{table}/` — each year contains a full set of tables aggregated across every Silver month in that year. Re-processing a year atomically rewrites its folder under a temp-staging path and swaps in on success.

| Table                    | Description                                              | Grain                                     |
|--------------------------|----------------------------------------------------------|-------------------------------------------|
| `airline_monthly_kpi`    | On-time %, cancellation rate, average delays per airline | airline × month                           |
| `route_monthly_kpi`      | Route-level performance per carrier                      | route × airline × month                   |
| `route_summary`          | Aggregate route traffic and performance                  | route × month                             |
| `airport_daily_ops`      | Daily departure / arrival counts per airport             | airport × direction × day                 |
| `delay_cause_analysis`   | Delay breakdown by cause category per airline            | airline × month                           |
| `time_block_performance` | Performance by departure hour block                      | airline × time block × month              |
| `cancellation_analysis`  | Cancellation reasons by airline                          | airline × reason × month                  |
| `monthly_trend`          | Month-over-month + YoY trend (cross-year recompute)      | airline × month                           |

---

### Project Structure

```
skyledger/
├── README.md
├── LICENSE
├── .gitignore
│
├── ingestion.py          # Bronze: BTS API → raw CSV → S3
├── dqcheck.py            # Silver: PySpark — 16 DQ rules + dedup → cleaned / rejected
├── transformation.py     # Gold:   PySpark — 8 aggregate tables with MD5 upsert
├── stepfunction.json     # Step Functions state machine (ASL definition)
│
├── sql/
│   ├── README.md                         # Runtime contract, deployment, edit workflow
│   └── gold/                             # 8 Spark SQL files — one per Gold table
│       ├── airline_monthly_kpi.sql
│       ├── route_monthly_kpi.sql
│       ├── route_summary.sql
│       ├── airport_daily_ops.sql
│       ├── delay_cause_analysis.sql
│       ├── time_block_performance.sql
│       ├── cancellation_analysis.sql
│       └── monthly_trend.sql
│
├── orchestration/
│   └── step_function.json                # State machine mirror for IaC deploys
│
├── infrastructure/
│   └── cloudformation.yaml               # Full IaC: S3, Glue, Step Functions, SSM, SNS, IAM
│
└── docs/
├── images/
│   ├── medallion_architecture.svg    # High-level Medallion architecture diagram
│   └── statemachine.svg              # Step Functions execution flow diagram
├── data-quality-rules.md             # Catalog of the 16 DQ checks with thresholds
└── pipeline-operations.md            # Runbook: modes, watermarks, failure handling
```
---

### Deployment
**Pre-requisites**
- AWS account with access to Glue, S3, Step Functions, SSM, and SNS
- Python 3.10 / PySpark 3.3 (provided by the AWS Glue 4.0 runtime)

**1. Deploy the infrastructure stack**
```bash
aws cloudformation deploy \
  --template-file infrastructure/cloudformation.yaml \
  --stack-name flight-data-pipeline-dev \
  --parameter-overrides \
      Environment=dev \
      AlertEmail=you@example.com \
  --capabilities CAPABILITY_NAMED_IAM
```

**2. Upload ETL Scripts and SQL Artifacts**

```bash
# Retrieve the dynamically provisioned bucket name
ARTIFACT_BUCKET=$(aws ssm get-parameter --name /flight-data-pipeline/dev/buckets/artifacts --query 'Parameter.Value' --output text)

# Upload PySpark scripts (Required before running Glue Jobs)
aws s3 sync src/pyspark/ "s3://${ARTIFACT_BUCKET}/scripts/" --delete --exclude "*" --include "*.py"

# Upload Gold SQL artifacts
aws s3 sync sql/gold/ "s3://${ARTIFACT_BUCKET}/sql/gold/" --delete --exclude "*" --include "*.sql"
```

---

### Data Source

[Bureau of Transportation Statistics (BTS)](https://www.transtats.bts.gov/) — US Domestic Flight On-Time Performance. Published monthly as ZIP archives containing CSV files of roughly 500K+ rows per month.

---

### Tech Stack

| Layer                    | Technology                                                      |
|--------------------------|-----------------------------------------------------------------|
| Distributed Processing   | **PySpark** on AWS Glue (Spark SQL, DataFrames, DynamicFrames)  |
| Ingestion                | Python 3.9 (Glue Python Shell)                                  |
| Storage                  | Amazon S3 — medallion layers (Bronze / Silver / Gold)           |
| Orchestration            | **AWS Step Functions** (ASL state machine)                      |
| Scheduling               | **Amazon EventBridge** (cron rule → Step Functions trigger)     |
| Data Catalog             | AWS Glue Data Catalog (partition pushdown)                      |
| Configuration            | AWS Systems Manager Parameter Store                             |
| Alerting                 | Amazon SNS (email notifications)                                |
| Infrastructure           | AWS CloudFormation (full IaC)                                   |
| Data Format              | CSV — Hive-style partitioning (`year=YYYY/month=MM`)            |

---

### License

This project is licensed under the MIT License — see [LICENSE](LICENSE) for details.
