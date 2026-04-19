import sys
import boto3
import traceback
import re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions

# ═════════════════════════════════════════════════════════════════════════════
# CONFIGURATION & SAFE PARAMETER PARSING
# ═════════════════════════════════════════════════════════════════════════════

JOB_NAME = "Local-Run"
if '--JOB_NAME' in sys.argv:
    JOB_NAME = getResolvedOptions(sys.argv, ['JOB_NAME'])['JOB_NAME']

def get_param(key, default=None):
    if f'--{key}' in sys.argv:
        return getResolvedOptions(sys.argv, [key])[key]
    return default

ssm = boto3.client("ssm")
s3  = boto3.client("s3")
sns = boto3.client("sns")

def ssm_get(name, default=None):
    try:
        return ssm.get_parameter(Name=name)["Parameter"]["Value"]
    except ssm.exceptions.ParameterNotFound:
        if default is None: raise
        return default

# ── Dynamic SSM Parameters ───────────────────────────────────────────────
SILVER_BUCKET    = ssm_get("/ishita-project1/buckets/silver")
GOLD_BUCKET      = ssm_get("/ishita-project1/buckets/gold")
SNS_TOPIC_ARN    = ssm_get("/ishita-project1/sns/alert_topic_arn")
ARTIFACTS_BUCKET = ssm_get("/ishita-project1/buckets/artifacts")

SILVER_STATE_KEY = "/ishita-project1/state/last_silver_dtm"
GOLD_STATE_KEY   = "/ishita-project1/state/last_gold_dtm"
BASELINE         = ssm_get("/ishita-project1/state/baseline", default="2025-06")

# YEARLY: reprocess now takes year inputs (YYYY), not month inputs
REPROCESS  = str(get_param("reprocess", "false")).strip().lower() in ("true", "1")
START_YEAR = get_param("start_year")  # YYYY
END_YEAR   = get_param("end_year")    # YYYY

MAX_SOURCE_RETRIES  = 14
GOLD_MARKER_PREFIX  = "_markers/gold/"
GOLD_SUCCESS_PREFIX = "_state/"

PER_MONTH_TABLES = [
    "airline_monthly_kpi", "route_monthly_kpi", "route_summary",
    "airport_daily_ops", "delay_cause_analysis", "time_block_performance",
    "cancellation_analysis"
]
CROSS_YEAR_TABLES = ["monthly_trend"]  # YEARLY: renamed for clarity
SQL_ARTIFACTS = {}

# ═════════════════════════════════════════════════════════════════════════════
# NOTIFICATION & SUMMARY
# ═════════════════════════════════════════════════════════════════════════════

def send_sns_email(subject, body):
    try:
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=body)
        print(f"  [SUCCESS] Email sent: {subject}")
    except Exception as e:
        print(f"  [WARNING] SNS failed: ({type(e).__name__}: {e})")

def send_escalation_alert(year, attempts, reason):
    subject = f"[ESCALATED] Gold Year {year} — CODE action required"
    body = (f"Year {year} failed {attempts} consecutive SOURCE attempts "
            f"and has been escalated to CODE. Reason: {reason}")
    send_sns_email(subject, body)

def build_summary(job_name, status, mode, duration, start, end,
                  gold_wm, new_wm, silver_ceiling, counts, results):
    icon = {"success": "SUCCESS", "skipped": "SKIPPED", "missing": "MISSING", "failed": "ERROR"}
    wm_line = (f"{gold_wm} to {new_wm}" if new_wm != gold_wm else f"{gold_wm} (No change)")
    lines = [
        "════════════════════════════════════════════════════════════",
        "  DATA PIPELINE NOTIFICATION: GOLD TRANSFORMATION",
        "════════════════════════════════════════════════════════════",
        f"  • Job Name:        {job_name}",
        f"  • Overall Result:  {status}",
        f"  • Run Mode:        {mode.upper()}",
        f"  • Execution Time:  {duration:.0f} seconds",
        f"  • Target Years:    {start} to {end}",       # YEARLY
        f"  • Gold Progress:   {wm_line}",
        f"  • Silver Ceiling:  {silver_ceiling}",
        "",
        "  PROCESSING BREAKDOWN:",
        f"  • Years Succeeded: {counts['success']}",    # YEARLY
        f"  • Years Skipped:   {counts['skipped']}",
        f"  • Years Missing:   {counts['missing']}",
        f"  • Years Failed:    {counts['failed']}",
    ]
    if results:
        lines.append("")
        lines.append("  DETAILED YEARLY STATUS:")       # YEARLY
        for y, res, meta in results:
            meta_str = f" (silver→{meta})" if meta else ""
            lines.append(f"    - [{icon[res]}] {y}{meta_str}")
    lines.append("════════════════════════════════════════════════════════════")
    return "\n".join(lines)

# ═════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def validate_ym(ym, label):
    try:
        datetime.strptime(ym, "%Y-%m")
    except (ValueError, TypeError):
        raise ValueError(f"{label} must be YYYY-MM, got {ym!r}")

def validate_year(y, label):                             # YEARLY
    if not re.match(r'^\d{4}$', str(y)):
        raise ValueError(f"{label} must be YYYY, got {y!r}")

def year_of(ym):
    return ym.split("-")[0]

def year_range(start_y, end_y):                          # YEARLY: replaces month_range
    for y in range(int(start_y), int(end_y) + 1):
        yield f"{y:04d}"

def _now_iso():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# ═════════════════════════════════════════════════════════════════════════════
# YEAR-LEVEL MARKERS                                       # YEARLY
# ═════════════════════════════════════════════════════════════════════════════

def _success_key(year):
    return f"{GOLD_SUCCESS_PREFIX}year={year}/_PROCESSED"

def _failure_key(year):
    return f"{GOLD_MARKER_PREFIX}year={year}/_FAILED"

def _read_key_body(key):
    try:
        resp = s3.get_object(Bucket=GOLD_BUCKET, Key=key)
        return resp['Body'].read().decode('utf-8')
    except Exception:
        return None

def _parse_field(body, field):
    if not body: return None
    m = re.search(rf'{field}=([^\s|]+)', body)
    return m.group(1) if m else None

def _parse_attempts(body):
    return int(_parse_field(body, 'attempts') or 0)

def _parse_first_seen(body):
    return _parse_field(body, 'first_seen') or _now_iso()

def gold_year_marker_body(year):
    """Returns body of _PROCESSED marker, or None if missing."""
    return _read_key_body(_success_key(year))

def write_success_marker_year(year, latest_silver_month):
    """Body includes latest_silver_month for change detection on next run."""
    body = (f"latest_silver_month={latest_silver_month}\n"
            f"processed_at={_now_iso()}")
    s3.put_object(Bucket=GOLD_BUCKET, Key=_success_key(year), Body=body.encode('utf-8'))

def delete_failed_marker_year(year):
    try:
        s3.delete_object(Bucket=GOLD_BUCKET, Key=_failure_key(year))
    except Exception:
        pass

def write_failed_marker_year(year, reason):
    """Same SOURCE/CODE escalation pattern as before, now year-scoped."""
    key = _failure_key(year)
    existing = _read_key_body(key)
    if reason.startswith("SOURCE:"):
        if existing and existing.startswith("CODE:"): return
        if existing and existing.startswith("SOURCE:"):
            attempts = _parse_attempts(existing) + 1
            first_seen = _parse_first_seen(existing)
            if attempts >= MAX_SOURCE_RETRIES:
                clean = reason[len("SOURCE: "):].strip()
                reason = (f"CODE: Escalated after {attempts} attempts — {clean} "
                          f"(first_seen={first_seen}, escalated_at={_now_iso()})")
                s3.put_object(Bucket=GOLD_BUCKET, Key=key, Body=reason.encode('utf-8'))
                send_escalation_alert(year, attempts, clean)
                return
            else:
                reason = f"{reason} | attempts={attempts} | first_seen={first_seen}"
        else:
            reason = f"{reason} | attempts=1 | first_seen={_now_iso()}"
    s3.put_object(Bucket=GOLD_BUCKET, Key=key, Body=reason.encode('utf-8'))

# ═════════════════════════════════════════════════════════════════════════════
# SILVER INTROSPECTION                                     # YEARLY
# ═════════════════════════════════════════════════════════════════════════════

def latest_silver_month_for_year(year):
    """Returns 'YYYY-MM' of latest Silver month for the year, or None if no data."""
    prefix = f"cleaned/flights/year={year}/month="
    resp = s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=prefix, Delimiter="/")
    months = []
    for cp in resp.get('CommonPrefixes', []) or []:
        m = re.search(r'month=(\d{2})/', cp['Prefix'])
        if m:
            months.append(f"{year}-{m.group(1)}")
    return max(months) if months else None

# ═════════════════════════════════════════════════════════════════════════════
# WINDOW RESOLUTION                                        # YEARLY
# ═════════════════════════════════════════════════════════════════════════════

def resolve_window(silver_ceiling):
    """Returns (mode, start_year, end_year)."""
    if REPROCESS:
        if not START_YEAR:
            raise ValueError("reprocess=True requires start_year (YYYY)")
        start_y = START_YEAR.strip()
        end_y   = (END_YEAR.strip() if END_YEAR else year_of(silver_ceiling))
        validate_year(start_y, "start_year")
        validate_year(end_y,   "end_year")
        if start_y > end_y:
            raise ValueError(f"start_year {start_y} > end_year {end_y}")
        if end_y > year_of(silver_ceiling):
            raise ValueError(f"end_year {end_y} exceeds Silver ceiling year {year_of(silver_ceiling)}")
        return "reprocess", start_y, end_y
    return "incremental", year_of(BASELINE), year_of(silver_ceiling)

# ═════════════════════════════════════════════════════════════════════════════
# SPARK & CORE PROCESSING
# ═════════════════════════════════════════════════════════════════════════════

spark = SparkSession.builder.appName("FlightTransformation-SilverToGold").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

def load_sql_artifact(table_name):
    key = f"sql_artifacts/gold/{table_name}.sql"
    try:
        resp = s3.get_object(Bucket=ARTIFACTS_BUCKET, Key=key)
        return resp['Body'].read().decode('utf-8')
    except Exception as e:
        raise RuntimeError(f"Failed to load SQL artifact {key}: {e}")


def process_year(year):                                   # YEARLY: replaces process_month
    """Processes a full year of Silver → Gold.
    Reads all available Silver months for the year, transforms, writes to
    s3://{GOLD_BUCKET}/year={year}/{table}/ for each per-month table.
    """
    print(f"\n  [{year}] Processing Silver → Gold (Year batch)")

    try:
        # ── CODE Guard ────────────────────────────────────────────────────
        if not REPROCESS:
            existing_fail = _read_key_body(_failure_key(year))
            if existing_fail and existing_fail.startswith("CODE:"):
                print(f"  [{year}] [FAILED] Skipped due to existing CODE marker.")
                return "failed", None

        # ── Silver Availability Check ─────────────────────────────────────
        latest_silver = latest_silver_month_for_year(year)
        if not latest_silver:
            print(f"  [{year}] Silver data not found.")
            write_failed_marker_year(year, "SOURCE: Silver data missing")
            return "missing", None

        # ── Idempotency: skip if Gold reflects current Silver state ──────
        if not REPROCESS:
            body = gold_year_marker_body(year)
            if body:
                marker_latest = _parse_field(body, 'latest_silver_month')
                if marker_latest == latest_silver:
                    delete_failed_marker_year(year)
                    print(f"  [{year}] [SKIPPED] Gold reflects Silver through {latest_silver}.")
                    return "skipped", latest_silver
                print(f"  [{year}] Gold stale (marker: {marker_latest} → Silver: {latest_silver}). Re-processing.")

        # ── Load Full Year of Silver ──────────────────────────────────────
        silver_path = f"s3://{SILVER_BUCKET}/cleaned/flights/year={year}/"
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(silver_path)

        # ── Schema Evolution Guard ────────────────────────────────────────
        expected_columns = [
            ("cancellationcode", "string"), ("weatherdelay", "double"),
            ("carrierdelay", "double"), ("nasdelay", "double"),
            ("securitydelay", "double"), ("lateaircraftdelay", "double"),
            ("taxiout", "double"), ("taxiin", "double"),
            ("airtime", "double"), ("deptimeblk", "string")
        ]
        existing_cols = [c.lower() for c in df.columns]
        for col_name, col_type in expected_columns:
            if col_name not in existing_cols:
                df = df.withColumn(col_name, F.lit(None).cast(col_type))

        df.createOrReplaceTempView("silver_flights")

        # ── Write Per-Table Under year={year}/{table}/ ───────────────────
        for table in PER_MONTH_TABLES:
            sql_query = SQL_ARTIFACTS[table]
            result_df = spark.sql(sql_query)
            target_path = f"s3://{GOLD_BUCKET}/year={year}/{table}/"
            (result_df.coalesce(1)
             .write.mode("overwrite")
             .option("header", "true")
             .csv(target_path))
            print(f"    - Updated: year={year}/{table}")

        delete_failed_marker_year(year)
        write_success_marker_year(year, latest_silver)
        return "success", latest_silver

    except Exception as e:
        print(f"  [{year}] FAILED — {type(e).__name__}: {e}")
        traceback.print_exc()
        write_failed_marker_year(year, f"CODE: {type(e).__name__}: {e}")
        return "failed", None


def recompute_cross_year_tables():                        # YEARLY
    """Global trend compute across all years, split-written into year=Y/{table}/."""
    print("\n  [Cross-year] Recomputing trend tables from full Gold history")

    # Wildcard across all year partitions
    kpi_path = f"s3://{GOLD_BUCKET}/year=*/airline_monthly_kpi/"
    kpi_df = spark.read.option("header", "true").option("inferSchema", "true").csv(kpi_path)
    kpi_df.createOrReplaceTempView("all_airline_kpi")

    for table in CROSS_YEAR_TABLES:
        sql_query = SQL_ARTIFACTS[table]
        result_df = spark.sql(sql_query).cache()

        years = [int(r["year"]) for r in result_df.select("year").distinct().collect()]
        for y in sorted(years):
            y_str = f"{y:04d}"
            year_df = result_df.filter(F.col("year") == y)
            target_path = f"s3://{GOLD_BUCKET}/year={y_str}/{table}/"
            (year_df.coalesce(1)
             .write.mode("overwrite")
             .option("header", "true")
             .csv(target_path))
            print(f"    - Recomputed: year={y_str}/{table}")
        result_df.unpersist()

# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main():
    job_start = datetime.utcnow()

    # Fail-fast SQL preload
    global SQL_ARTIFACTS
    SQL_ARTIFACTS = {t: load_sql_artifact(t) for t in PER_MONTH_TABLES + CROSS_YEAR_TABLES}
    print(f"  Loaded {len(SQL_ARTIFACTS)} SQL artifacts successfully.")

    gold_wm        = ssm_get(GOLD_STATE_KEY, default=BASELINE)
    silver_ceiling = ssm_get(SILVER_STATE_KEY, default=BASELINE)
    validate_ym(gold_wm, "last_gold_dtm")
    validate_ym(silver_ceiling, "last_silver_dtm")

    mode, start_y, end_y = resolve_window(silver_ceiling)

    print("=" * 60)
    print("  US FLIGHT DATA — GOLD TRANSFORMATION (YEAR-BASED)")
    print("=" * 60)
    print(f"  Job Name:         {JOB_NAME}")
    print(f"  Mode:             {mode.upper()}")
    print(f"  Gold Watermark:   {gold_wm}")
    print(f"  Silver Ceiling:   {silver_ceiling}")
    print(f"  Year Window:      {start_y} → {end_y}")
    print("=" * 60)

    counts = {"success": 0, "skipped": 0, "missing": 0, "failed": 0}
    results = []

    # ── Single Loop: Iterate Years, process_year handles skip/retry/CODE ─
    for y in year_range(start_y, end_y):
        res, meta = process_year(y)
        counts[res] += 1
        results.append((y, res, meta))

    # ── Cross-year Recompute ─────────────────────────────────────────────
    if counts["success"] > 0:
        try:
            recompute_cross_year_tables()
        except Exception as e:
            print(f"  [Cross-year] FAILED — {type(e).__name__}: {e}")
            traceback.print_exc()
            # Non-fatal: per-year writes already succeeded

    # ── Watermark Advance (max latest_silver across processed years) ────
    new_wm = gold_wm
    if mode == "incremental":
        advances = [meta for (_, r, meta) in results
                    if r in ("success", "skipped") and meta]
        if advances:
            candidate = max(advances)
            if candidate > gold_wm:
                new_wm = candidate

    # ── Summary & Status ─────────────────────────────────────────────────
    duration = (datetime.utcnow() - job_start).total_seconds()
    watermark_stalled = (mode == "incremental" and bool(results) and new_wm == gold_wm)

    if not results:
        overall_status = "NO YEARS IN WINDOW"
    elif mode == "reprocess":
        if counts["success"] == 0 and (counts["failed"] + counts["missing"]) > 0:
            overall_status = "REPROCESS FAILED"
        elif (counts["failed"] + counts["missing"]) > 0:
            overall_status = "REPROCESS PARTIAL"
        else:
            overall_status = "REPROCESS COMPLETE"
    else:
        if counts["success"] == 0 and counts["skipped"] == 0:
            overall_status = "FAILED" + (" — WATERMARK STALLED" if watermark_stalled else "")
        elif (counts["failed"] + counts["missing"]) > 0:
            overall_status = "PARTIAL SUCCESS" + (" — WATERMARK STALLED" if watermark_stalled else " — GAPS PRESENT")
        else:
            overall_status = "SUCCESS"

    summary = build_summary(JOB_NAME, overall_status, mode, duration, start_y, end_y,
                            gold_wm, new_wm, silver_ceiling, counts, results)
    print(f"\n{summary}")

    # ── State Update ─────────────────────────────────────────────────────
    print("\n" + "─" * 60)
    if REPROCESS:
        print(f"  last_gold_dtm unchanged (reprocess mode): ({gold_wm})")
    elif new_wm != gold_wm:
        ssm.put_parameter(Name=GOLD_STATE_KEY, Value=new_wm, Type="String", Overwrite=True)
        print(f"  last_gold_dtm advanced: {gold_wm} → {new_wm}")
    else:
        print(f"  last_gold_dtm unchanged: ({gold_wm})")

    send_sns_email(f"[{overall_status}] Gold Transformation: {JOB_NAME}", summary)

    # ── Total Failure Guard ──────────────────────────────────────────────
    if bool(results) and counts["success"] == 0 and counts["skipped"] == 0 and (counts["failed"] + counts["missing"]) > 0:
        raise RuntimeError("TOTAL FAILURE — 0 successful years. Check CloudWatch logs and _FAILED markers.")

if __name__ == "__main__":
    main()