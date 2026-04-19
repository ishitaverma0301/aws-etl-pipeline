## Data Quality Rules Catalog

The Silver layer applies **16 data quality checks** to every Bronze row in a single Spark SQL pass. Rows failing one or more rules are routed to the `rejected/` partition with a `rejection_reason` column listing every rule that fired; clean rows proceed to `cleaned/` for consumption by the Gold layer.

---

### Design Principles

- **Single-pass evaluation** — rules DQ01 through DQ15 execute in one Spark SQL statement via `CONCAT_WS`, so no row is scanned twice.
- **Non-destructive classification** — rejected rows are persisted (not dropped) so that data stewards can audit them.
- **Multi-rule transparency** — a single row can report multiple violations, separated by ` | ` in the `rejection_reason` column.
- **Post-SQL deduplication** — DQ16 (exact-row duplicates) runs against the clean set only, via `dropDuplicates`.

---

### Severity Taxonomy

| Severity   | Meaning                                                                                     |
|------------|---------------------------------------------------------------------------------------------|
| `Critical` | Row lacks an identifier required by downstream aggregations — always rejected               |
| `Major`    | Row has an out-of-range or implausible value in a measured field                            |
| `Logical`  | Row violates a business-logic invariant between fields (e.g., cancelled flight has arrtime) |

---

### Rule Reference

| Rule ID | Rule Name                        | Logic                                                                 | Severity  |
|---------|----------------------------------|-----------------------------------------------------------------------|-----------|
| `DQ01`  | Null flight date                 | `year`, `month`, or `dayofmonth` is `NULL`                            | Critical  |
| `DQ02`  | Null airline                     | `reporting_airline` is `NULL`                                         | Critical  |
| `DQ03`  | Null route                       | `origin` or `dest` is `NULL`                                          | Critical  |
| `DQ04`  | Null distance                    | `distance` is `NULL`                                                  | Major     |
| `DQ05`  | Invalid month                    | `month` not in `[1, 12]`                                              | Critical  |
| `DQ06`  | Invalid day                      | `dayofmonth` not in `[1, 31]`                                         | Critical  |
| `DQ07`  | Non-positive distance            | `distance` present but `≤ 0`                                          | Major     |
| `DQ08`  | Departure delay out of range     | `depdelay` outside `[-60, 1440]` minutes                              | Major     |
| `DQ09`  | Arrival delay out of range       | `arrdelay` outside `[-60, 1440]` minutes                              | Major     |
| `DQ10`  | Invalid time                     | `deptime` or `arrtime` outside `[0, 2400]`                            | Major     |
| `DQ11`  | Cancelled with arrival time      | `cancelled = 1` but `arrtime` is not `NULL`                           | Logical   |
| `DQ12`  | Cancelled with arrival delay     | `cancelled = 1` but `arrdelay` is not `NULL`                          | Logical   |
| `DQ13`  | Operated flight missing deptime  | `cancelled = 0` and `diverted = 0`, but `deptime` is `NULL`           | Logical   |
| `DQ14`  | Airtime exceeds elapsed time     | `airtime > actualelapsedtime` (physically impossible)                 | Logical   |
| `DQ15`  | Origin equals destination        | `origin = dest` (degenerate same-airport route)                       | Logical   |
| `DQ16`  | Duplicate row                    | Exact duplicate across all columns (applied via `dropDuplicates`)     | Major     |

---

### Thresholds

Thresholds are centralized as constants at the top of the Silver script so that ops can adjust them without touching SQL.

| Constant     | Value        | Purpose                                       |
|--------------|--------------|-----------------------------------------------|
| `MIN_DELAY`  | `-60`  min   | Lower bound for delay fields (early arrivals) |
| `MAX_DELAY`  | `1440` min   | Upper bound for delay fields (24 hours)       |
| `MAX_TIME`   | `2400`       | HHMM ceiling for time-of-day fields           |

---

### Rejection Flow

1. Bronze rows are loaded via the Glue Data Catalog with partition pushdown on `year` and `month`.
2. DQ01 – DQ15 execute in a single Spark SQL statement; each rule contributes a string fragment when it fires.
3. Fragments are concatenated with ` | ` into the `rejection_reason` column (empty string coalesced to `NULL`).
4. Rows with a non-null `rejection_reason` are written to `silver-bucket/rejected/year=YYYY/month=MM/`.
5. Rows with a null `rejection_reason` pass to the clean set.
6. DQ16 deduplicates the clean set via `dropDuplicates`; removed duplicates are counted but not persisted.
7. Final clean output is written to `silver-bucket/cleaned/year=YYYY/month=MM/`.

---

### Rejection Reporting

After each monthly run, the Silver job logs a per-rule breakdown and includes the same breakdown in its SNS notification:

```
[2024-08] Rows: 612,847 | Clean: 608,219 | Rejected: 4,628 | Dupes: 12 | Rate: 0.76%
     2,341  DQ08: depdelay out of range
     1,102  DQ09: arrdelay out of range
       847  DQ13: Operated flight has no deptime
       338  DQ11: Cancelled flight has arrtime
```

A rejection rate above `MAX_REJECTION_PCT` (default **10.0%**, configurable via SSM parameter `/ishita-project1/tuning/max_rejection_pct`) prefixes the SNS subject with `[ALERT]` so that stewards can investigate source-side data drift before the next scheduled run.

---

### Extending the Catalog

To add a new DQ rule:

1. Append a new `CASE WHEN` clause to the `DQ_SQL` template in [`ishita-project1-dqcheck.py`](../ishita-project1-dqcheck.py), following the `DQNN: <reason>` naming convention.
2. If the rule depends on a tunable threshold, add a new constant alongside `MAX_DELAY` / `MIN_DELAY` / `MAX_TIME` (or a new SSM parameter for runtime-adjustable limits).
3. Document the rule here with its ID, logic, and severity.
4. Trigger a reprocess over a representative window to verify the rule fires as expected and that the rejection rate remains within the alert threshold.
