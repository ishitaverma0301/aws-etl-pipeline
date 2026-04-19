import sys
import boto3
import traceback
import re
from datetime import datetime
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions

# ═════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═════════════════════════════════════════════════════════════════════════════

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Init Boto3 clients
ssm = boto3.client("ssm")
s3  = boto3.client("s3")
sns = boto3.client("sns")

def get_param(key, default=None):
    """Optional job parameter with a fallback."""
    try:
        return getResolvedOptions(sys.argv, [key])[key]
    except Exception:
        return default

def ssm_get(name, default=None):
    """Fetch an SSM parameter, with an optional fallback if not found."""
    try:
        return ssm.get_parameter(Name=name)["Parameter"]["Value"]
    except ssm.exceptions.ParameterNotFound:
        if default is None: raise
        return default

# ── Dynamic SSM Parameters ───────────────────────────────────────────────
BRONZE_BUCKET = ssm_get("/ishita-project1/buckets/bronze")
SILVER_BUCKET = ssm_get("/ishita-project1/buckets/silver")
SNS_TOPIC_ARN = ssm_get("/ishita-project1/sns/alert_topic_arn")

# ── State Management Parameters ──────────────────────────────────────────
BRONZE_STATE_KEY = "/ishita-project1/state/last_load_dtm"    # Ceiling
SILVER_STATE_KEY = "/ishita-project1/state/last_silver_dtm"  # Floor
BASELINE         = ssm_get("/ishita-project1/state/baseline", default="2025-06")

CURRENT_YM = datetime.utcnow().strftime("%Y-%m")

# ── Job-level Parameters ─────────────────────────────────────────────────
REPROCESS  = str(get_param("reprocess", "false")).strip().lower() in ("true", "1")
START_DATE = get_param("start_date")  
END_DATE   = get_param("end_date")    

# ── DQ & Ingestion Constants ─────────────────────────────────────────────
MAX_DELAY = 1440    # 24 hours in minutes
MIN_DELAY = -60     # 1 hour early
MAX_TIME  = 2400    # HHMM ceiling

MAX_SOURCE_RETRIES   = 14
SILVER_MARKER_PREFIX = "_markers/flights/"


# ═════════════════════════════════════════════════════════════════════════════
# NOTIFICATION & SUMMARY MODULE
# ═════════════════════════════════════════════════════════════════════════════

def send_sns_email(subject, body):
    """Publish job summary to SNS. Fails safely without crashing the job."""
    try:
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=body)
        print(f"  [SUCCESS] Email sent successfully: {subject}")
    except Exception as e:
        print(f"  [WARNING] Failed to send email notification: ({type(e).__name__}: {e})")

def send_escalation_alert(ym, attempts, reason):
    """Fires when a missing month exceeds the MAX_SOURCE_RETRIES budget."""
    subject = f"[ESCALATED] Silver DQ {ym} — CODE action required"
    body = (f"Month {ym} failed {attempts} consecutive SOURCE attempts "
            f"and has been escalated to CODE. Reason: {reason}")
    send_sns_email(subject, body)

def build_summary(job_name, status, mode, duration, start, end,
                  silver_wm, new_wm, bronze_ceiling, counts, results):
    """Builds a refined summary for the Silver DQ job aligned with Bronze vocabulary."""
    icon = {"success": "SUCCESS", "skipped": "SKIPPED", "missing": "MISSING", "failed": "ERROR"}
    wm_line = (f"{silver_wm} to {new_wm}" if new_wm != silver_wm else f"{silver_wm} (No change)")

    lines = [
        "════════════════════════════════════════════════════════════",
        "  DATA PIPELINE NOTIFICATION: SILVER DQ CHECK",
        "════════════════════════════════════════════════════════════",
        f"  • Job Name:        {job_name}",
        f"  • Overall Result:  {status}",
        f"  • Run Mode:        {mode.upper()}",
        f"  • Execution Time:  {duration:.0f} seconds",
        f"  • Target Period:   {start} to {end}",
        f"  • Silver Progress: {wm_line}",
        f"  • Bronze Ceiling:  {bronze_ceiling}",
        f"  • Bronze Path:     s3://{BRONZE_BUCKET}/flights/",
        "",
        "  PROCESSING BREAKDOWN:",
        f"  • Months Succeeded: {counts['success']}",
        f"  • Months Skipped:   {counts['skipped']}",
        f"  • Months Missing:   {counts['missing']}",
        f"  • Months Failed:    {counts['failed']}",
    ]
    
    # Exclude non-processed escalated gaps from the detailed printout formatting
    # by ensuring only runs that generated stats output rules.
    if results:
        lines.append("")
        lines.append("  DETAILED REJECTION REPORTING:")
        for ym, res, stats in results:
            lines.append(f"    - [{icon[res]}] {ym}")
            if res == "success" and stats:
                lines.append(f"      Rows: {stats['rows']:,} | Clean: {stats['clean']:,} | "
                             f"Rejected: {stats['rej']:,} | Dupes: {stats['dupes']:,} | "
                             f"Rate: {stats['rate']:.2f}%")
                for rule in stats['breakdown']:
                    lines.append(f"      {rule['cnt']:>8,}  {rule['reason']}")
            lines.append("")

    lines.append("════════════════════════════════════════════════════════════")
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# YYYY-MM HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def validate_ym(ym, label):
    try:
        datetime.strptime(ym, "%Y-%m")
    except (ValueError, TypeError):
        raise ValueError(f"{label} must be YYYY-MM, got {ym!r}")

def next_month(ym):
    """Returns the month strictly AFTER ym (e.g., '2025-12' → '2026-01')."""
    y, m = map(int, ym.split("-"))
    return f"{y + m // 12:04d}-{m % 12 + 1:02d}"

def month_range(start, end):
    cur = start
    while cur <= end:
        yield cur
        cur = next_month(cur)


# ═════════════════════════════════════════════════════════════════════════════
# SILVER STATE & MARKER HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def silver_month_is_done(year, month):
    """True if any cleaned CSV exists for the month."""
    prefix = f"cleaned/flights/year={year}/month={month}/"
    try:
        resp = s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=prefix, MaxKeys=1)
        return resp.get("KeyCount", 0) > 0
    except Exception:
        return False

def _marker_key(year, month):
    return f"{SILVER_MARKER_PREFIX}year={year}/month={month}/_FAILED"

def _now_iso():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def _read_marker_body_if_exists(key):
    try:
        resp = s3.get_object(Bucket=SILVER_BUCKET, Key=key)
        return resp['Body'].read().decode('utf-8')
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        raise

def _parse_attempts(body):
    match = re.search(r'attempts=(\d+)', body)
    return int(match.group(1)) if match else 0

def _parse_first_seen(body):
    match = re.search(r'first_seen=([^\s|]+)', body)
    return match.group(1) if match else _now_iso()

def write_failed_marker(year, month, reason):
    """Writes typed failure marker, tracking attempts and triggering escalation."""
    key = _marker_key(year, month)
    existing = _read_marker_body_if_exists(key)

    if reason.startswith("SOURCE:"):
        if existing and existing.startswith("CODE:"):
            return  # Honor analyst-halt state; do not downgrade back to SOURCE
        
        if existing and existing.startswith("SOURCE:"):
            attempts = _parse_attempts(existing) + 1
            first_seen = _parse_first_seen(existing)
            
            if attempts >= MAX_SOURCE_RETRIES:
                clean_reason = reason[len("SOURCE: "):].strip()
                escalated_reason = (f"CODE: Escalated after {attempts} attempts — "
                                    f"{clean_reason} "
                                    f"(first_seen={first_seen}, escalated_at={_now_iso()})")
                s3.put_object(Bucket=SILVER_BUCKET, Key=key, Body=escalated_reason.encode("utf-8"))
                send_escalation_alert(f"{year}-{month}", attempts, clean_reason)
                return
            else:
                reason = f"{reason} | attempts={attempts} | first_seen={first_seen}"
        else:
            reason = f"{reason} | attempts=1 | first_seen={_now_iso()}"

    s3.put_object(Bucket=SILVER_BUCKET, Key=key, Body=reason.encode("utf-8"))

def delete_failed_marker(year, month):
    """Clears any stale marker after a successful run or if skipping."""
    try:
        s3.delete_object(Bucket=SILVER_BUCKET, Key=_marker_key(year, month))
    except Exception:
        pass

def _delete_s3_prefix(bucket, prefix):
    """Helper to wipe S3 prefixes cleanly for REPROCESS mode."""
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            s3.delete_objects(
                Bucket=bucket,
                Delete={'Objects': [{'Key': obj['Key']} for obj in page['Contents']]}
            )

def scan_silver_markers(phase2_start_ym):
    """
    Walks historical gaps strictly below the Phase 2 boundary.
    Returns SOURCE markers to retry, and CODE markers that require visibility.
    """
    retry_list = []
    escalated_gaps = []
    silver_wm = ssm_get(SILVER_STATE_KEY, default=BASELINE)
    
    for ym in month_range(BASELINE, silver_wm):
        if ym >= phase2_start_ym: 
            continue
        
        year, month = ym.split("-")
        
        if not silver_month_is_done(year, month):
            body = _read_marker_body_if_exists(_marker_key(year, month))
            if not body or body.startswith("SOURCE:"):
                retry_list.append(ym)
            elif body.startswith("CODE:"):
                escalated_gaps.append(ym)
                
    if retry_list:
        print(f"  Found {len(retry_list)} Silver gap(s) to fill: {retry_list}")
    if escalated_gaps:
        print(f"  [ACTION REQUIRED] Found {len(escalated_gaps)} escalated CODE gaps blocking full completion: {escalated_gaps}")
        
    return retry_list, escalated_gaps


# ═════════════════════════════════════════════════════════════════════════════
# WINDOW RESOLUTION
# ═════════════════════════════════════════════════════════════════════════════

def resolve_window(silver_watermark, bronze_ceiling):
    """Determines processing window. Silver must never surpass the Bronze ceiling."""
    if REPROCESS:
        if not START_DATE:
            raise ValueError("reprocess=True requires start_date (YYYY-MM)")
        start = START_DATE.strip()
        end   = (END_DATE.strip() if END_DATE else bronze_ceiling)
        validate_ym(start, "start_date")
        validate_ym(end,   "end_date")
        if start > end:
            raise ValueError(f"start_date {start} is after end_date {end}")
        if end > bronze_ceiling:
            raise ValueError(f"end_date {end} exceeds Bronze ceiling {bronze_ceiling}.")
        return "reprocess", start, end

    return "incremental", next_month(silver_watermark), bronze_ceiling


# ═════════════════════════════════════════════════════════════════════════════
# SPARK INIT & DATA QUALITY SQL
# ═════════════════════════════════════════════════════════════════════════════

spark = SparkSession.builder.appName("FlightDQ-BronzeToSilver").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Single-pass checks applied to every row
DQ_SQL = """
SELECT
    *,
    CONCAT_WS(' | ',
        CASE WHEN year IS NULL OR month IS NULL OR dayofmonth IS NULL THEN 'DQ01: Null flight date' END,
        CASE WHEN reporting_airline IS NULL THEN 'DQ02: Null airline' END,
        CASE WHEN origin IS NULL OR dest IS NULL THEN 'DQ03: Null route' END,
        CASE WHEN distance IS NULL THEN 'DQ04: Null distance' END,
        CASE WHEN month < 1 OR month > 12 THEN 'DQ05: Invalid month' END,
        CASE WHEN dayofmonth < 1 OR dayofmonth > 31 THEN 'DQ06: Invalid dayofmonth' END,
        CASE WHEN distance IS NOT NULL AND distance <= 0 THEN 'DQ07: Distance <= 0' END,
        CASE WHEN depdelay IS NOT NULL AND (depdelay < {min_delay} OR depdelay > {max_delay}) THEN 'DQ08: depdelay out of range' END,
        CASE WHEN arrdelay IS NOT NULL AND (arrdelay < {min_delay} OR arrdelay > {max_delay}) THEN 'DQ09: arrdelay out of range' END,
        CASE WHEN deptime IS NOT NULL AND (deptime < 0 OR deptime > {max_time}) THEN 'DQ10: deptime invalid' END,
        CASE WHEN arrtime IS NOT NULL AND (arrtime < 0 OR arrtime > {max_time}) THEN 'DQ10: arrtime invalid' END,
        CASE WHEN cancelled = 1 AND arrtime IS NOT NULL THEN 'DQ11: Cancelled flight has arrtime' END,
        CASE WHEN cancelled = 1 AND arrdelay IS NOT NULL THEN 'DQ12: Cancelled flight has arrdelay' END,
        CASE WHEN cancelled = 0 AND diverted = 0 AND deptime IS NULL THEN 'DQ13: Operated flight has no deptime' END,
        CASE WHEN airtime IS NOT NULL AND actualelapsedtime IS NOT NULL AND airtime > actualelapsedtime THEN 'DQ14: airtime > actualelapsedtime' END,
        CASE WHEN origin IS NOT NULL AND dest IS NOT NULL AND origin = dest THEN 'DQ15: origin == dest' END
    ) AS rejection_reason
FROM bronze_flights
""".format(min_delay=MIN_DELAY, max_delay=MAX_DELAY, max_time=MAX_TIME)

# Breakdown of rejections per rule for reporting
REJECTION_BREAKDOWN_SQL = """
SELECT reason, COUNT(*) AS cnt
FROM (
    SELECT EXPLODE(SPLIT(rejection_reason, ' \\\\| ')) AS reason
    FROM dq_results
    WHERE rejection_reason IS NOT NULL AND rejection_reason != ''
)
GROUP BY reason
ORDER BY cnt DESC
"""


# ═════════════════════════════════════════════════════════════════════════════
# CORE PROCESSING
# ═════════════════════════════════════════════════════════════════════════════

def process_month(ym):
    """Reads raw data directly from Bronze S3, applies DQ SQL, dedups, routes output."""
    year, month = ym.split("-")
    bronze_key    = f"flights/year={year}/month={month}/flights_{year}_{month}.csv"
    bronze_path   = f"s3://{BRONZE_BUCKET}/{bronze_key}"
    cleaned_path  = f"s3://{SILVER_BUCKET}/cleaned/flights/year={year}/month={month}/"
    rejected_path = f"s3://{SILVER_BUCKET}/rejected/flights/year={year}/month={month}/"

    print(f"\n  [{ym}] Reading {bronze_path}")

    try:
        # ── 1. Skip-if-done & State Guards ───────────────────────────────
        if not REPROCESS and silver_month_is_done(year, month):
            delete_failed_marker(year, month)
            print(f"  [{ym}] [SKIPPED] Data already processed.")
            return "skipped", None

        if not REPROCESS:
            existing_marker = _read_marker_body_if_exists(_marker_key(year, month))
            if existing_marker and existing_marker.startswith("CODE:"):
                print(f"  [{ym}] [FAILED] Skipped due to existing CODE marker.")
                return "failed", None

        # ── 2. Guard the read (check Bronze CSV exists first) ────────────
        try:
            s3.head_object(Bucket=BRONZE_BUCKET, Key=bronze_key)
        except ClientError as e:
            err_code = e.response['Error']['Code']
            if err_code in ('404', 'NoSuchKey'):
                print(f"  [{ym}] Bronze CSV not found (404).")
                write_failed_marker(year, month, "SOURCE: Bronze CSV not found or empty")
                return "missing", None
            elif err_code == '403':
                print(f"  [{ym}] AccessDenied (403) reading Bronze.")
                write_failed_marker(year, month, "CODE: HTTP 403 — auth/IAM issue reading Bronze")
                return "failed", None
            else:
                raise

        # ── 3. Execute Spark Read ────────────────────────────────────────
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(bronze_path)
        row_count = df.count()

        if row_count == 0:
            print(f"  [{ym}] Bronze CSV is empty.")
            write_failed_marker(year, month, "SOURCE: Bronze CSV not found or empty")
            return "missing", None

        # ── 3.5 SAFELY WIPE OLD DATA NOW THAT BRONZE IS CONFIRMED ────────
        if REPROCESS:
            _delete_s3_prefix(SILVER_BUCKET, f"cleaned/flights/year={year}/month={month}/")
            _delete_s3_prefix(SILVER_BUCKET, f"rejected/flights/year={year}/month={month}/")
            delete_failed_marker(year, month)
            print(f"  [{ym}] [REPROCESS] Cleared old Silver output to ensure clean state.")

        # ── 4. Apply DQ Checks ───────────────────────────────────────────
        df.createOrReplaceTempView("bronze_flights")
        dq_df = spark.sql(DQ_SQL)
        dq_df = dq_df.withColumn(
            "rejection_reason",
            F.when(F.trim(F.col("rejection_reason")) == "", None).otherwise(F.col("rejection_reason"))
        )
        dq_df.createOrReplaceTempView("dq_results")

        cleaned_df  = dq_df.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")
        rejected_df = dq_df.filter(F.col("rejection_reason").isNotNull())

        # ── 5. Deduplication (DQ16) ──────────────────────────────────────
        cleaned_before_dedup = cleaned_df.count()
        cleaned_df     = cleaned_df.dropDuplicates(cleaned_df.columns)
        cleaned_count  = cleaned_df.count()
        rejected_count = rejected_df.count()
        dupes_removed  = cleaned_before_dedup - cleaned_count
        reject_pct     = 100 * rejected_count / max(row_count, 1)

        print(f"  [{ym}] Rows: {row_count:,} | Clean: {cleaned_count:,} | "
              f"Rejected: {rejected_count:,} | Dupes: {dupes_removed:,} | "
              f"Rate: {reject_pct:.2f}%")

        breakdown = []
        if rejected_count > 0:
            breakdown_rows = spark.sql(REJECTION_BREAKDOWN_SQL).collect()
            for row in breakdown_rows:
                print(f"    {row['cnt']:>8,}  {row['reason']}")
                breakdown.append({"reason": row['reason'], "cnt": row['cnt']})

        # ── 6. Write to Silver ───────────────────────────────────────────
        cleaned_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(cleaned_path)
        
        if rejected_count > 0:
            rejected_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(rejected_path)

        print(f"  [{ym}] Processed → {cleaned_path}")

        stats = {
            "rows": row_count, "clean": cleaned_count, "rej": rejected_count,
            "dupes": dupes_removed, "rate": reject_pct, "breakdown": breakdown
        }
        
        # ── 7. Self-heal and Return ──────────────────────────────────────
        delete_failed_marker(year, month)
        return "success", stats

    except Exception as e:
        print(f"  [{ym}] FAILED — {type(e).__name__}: {e}")
        traceback.print_exc()
        write_failed_marker(year, month, f"CODE: {type(e).__name__}: {e}")
        return "failed", None


# ═════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

def main():
    job_start = datetime.utcnow()
    job_name  = args.get('JOB_NAME', 'Local-Run')

    # Read Watermarks
    silver_watermark = ssm_get(SILVER_STATE_KEY, default=BASELINE)
    bronze_ceiling   = ssm_get(BRONZE_STATE_KEY, default=BASELINE)
    validate_ym(silver_watermark, "last_silver_dtm from SSM")
    validate_ym(bronze_ceiling,   "last_load_dtm from SSM")

    mode, start, end = resolve_window(silver_watermark, bronze_ceiling)

    print("=" * 60)
    print("  US FLIGHT DATA — SILVER DQ PROCESSING")
    print("=" * 60)
    print(f"  Job Name:       {job_name}")
    print(f"  Mode:           {mode.upper()}")
    print(f"  Silver Floor:   {silver_watermark}")
    print(f"  Bronze Ceiling: {bronze_ceiling}")
    print(f"  Window:         {start} → {end}")
    print(f"  Bronze Path:    s3://{BRONZE_BUCKET}/flights/")
    print("=" * 60)

    if start > end:
        print("\n  >>> NO NEW MONTHS TO PROCESS <<<")
        print("  >>> Silver watermark is already at the Bronze ceiling.")
        send_sns_email(f"[UP-TO-DATE] {job_name}", "No new data to process. Pipeline is up to date.")
        return

    counts  = {"success": 0, "skipped": 0, "missing": 0, "failed": 0}
    results = []

    # ── Phase 1 — Gap Fill (incremental mode only) ────────────────────────
    if not REPROCESS:
        phase2_start = next_month(silver_watermark)
        phase1_targets, escalated_gaps = scan_silver_markers(phase2_start)
        
        for ym in phase1_targets:
            print(f"\n  [Phase 1 — Gap Fill] Retrying {ym}")
            result, stats = process_month(ym)
            counts[result] += 1
            results.append((ym, result, stats))
            
        # Ensure CODE gaps force a failure so the pipeline is not silently green
        if escalated_gaps:
            counts["failed"] += len(escalated_gaps)
            for gap in escalated_gaps:
                results.append((gap, "failed", None))

    # ── Phase 2 — New Months OR Reprocess Window ──────────────────────────
    for ym in month_range(start, end):
        result, stats = process_month(ym)
        counts[result] += 1
        results.append((ym, result, stats))

    # ── State Advance (Truthful Contiguous HWM) ───────────────────────────
    new_watermark = silver_watermark
    if mode == "incremental":
        current_check = next_month(silver_watermark)
        outcomes = {ym: res for ym, res, _ in results}
        
        # Advance contiguously only if the *next* sequential month succeeded/skipped
        while current_check in outcomes and outcomes[current_check] in ("success", "skipped"):
            new_watermark = current_check
            current_check = next_month(current_check)

    # ── Job summary & Status Compilation ──────────────────────────────────
    duration = (datetime.utcnow() - job_start).total_seconds()
    
    watermark_stalled = (mode == "incremental" and bool(results)
                         and new_watermark == silver_watermark)

    if not results:
        overall_status = "NO NEW DATA"
    elif mode == "reprocess":
        if counts["success"] == 0 and (counts["failed"] > 0 or counts["missing"] > 0):
            overall_status = "REPROCESS FAILED"
        elif counts["failed"] > 0 or counts["missing"] > 0:
            overall_status = "REPROCESS PARTIAL"
        else:
            overall_status = "REPROCESS COMPLETE"
    else:
        if counts["success"] == 0 and counts["skipped"] == 0:
            overall_status = "FAILED" + (" — WATERMARK STALLED" if watermark_stalled else "")
        elif counts["missing"] > 0 or counts["failed"] > 0:
            overall_status = "PARTIAL SUCCESS" + (" — WATERMARK STALLED" if watermark_stalled else " — GAPS PRESENT")
        else:
            overall_status = "SUCCESS"

    summary = build_summary(
        job_name, overall_status, mode, duration, start, end,
        silver_watermark, new_watermark, bronze_ceiling, counts, results
    )

    print(f"\n{summary}")

    # ── State Update Execution ───────────────────────────────────────────
    print("\n" + "─" * 60)
    if REPROCESS:
        print(f"  last_silver_dtm unchanged (reprocess mode): ({silver_watermark})")
    elif new_watermark != silver_watermark:
        ssm.put_parameter(
            Name=SILVER_STATE_KEY, Value=new_watermark,
            Type="String", Overwrite=True,
        )
        print(f"  last_silver_dtm advanced: {silver_watermark} → {new_watermark}")
    else:
        print(f"  last_silver_dtm unchanged: ({silver_watermark})")
    
    print("─" * 60)
    print(f"  >>> OVERALL STATUS: {overall_status} <<<")
    print("─" * 60)

    send_sns_email(f"[{overall_status}] Glue DQ Job: {job_name}", summary)

    # ── Total Failure Step Function Guard ────────────────────────────────
    if (bool(results)
        and counts["success"] == 0 and counts["skipped"] == 0
        and (counts["failed"] > 0 or counts["missing"] > 0)):
        raise RuntimeError(
            f"TOTAL FAILURE — 0 successful months, "
            f"{counts['failed']} failed / {counts['missing']} missing. "
            f"Check CloudWatch logs and _FAILED markers."
        )

if __name__ == "__main__":
    main()
