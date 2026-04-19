import sys
import os
import time
import urllib.request
import urllib.error
import zipfile
import re
from datetime import datetime

import boto3
from awsglue.utils import getResolvedOptions

# ═════════════════════════════════════════════════════════════════════════════
# Bronze watermark rule: LEAPFROG WATERMARK
# Watermark advances to the maximum month that was 'success' or 'skipped',
# leapfrogging any missing/failed months to ensure pipeline progress.
#
# Two-phase execution:
#   Phase 1 — RETRY: scan SOURCE: markers strictly BELOW Phase 2's start.
#   Phase 2 — NEW MONTHS: incremental from watermark forward.
#
# Gap Escalation: SOURCE gaps auto-escalate to CODE after MAX_SOURCE_RETRIES,
# triggering an SNS alert and halting retries until analyst intervention.
#
# Reprocess Mode: Bypasses the skip-if-done guard and forces an overwrite of 
# the specified window. Does not alter the watermark.
# ═════════════════════════════════════════════════════════════════════════════

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

def get_param(key, default):
    """Read optional Glue job parameter, fall back to default."""
    try:
        return getResolvedOptions(sys.argv, [key])[key]
    except Exception:
        return default

# Init Boto3 clients
ssm_client = boto3.client('ssm')
s3 = boto3.client("s3")
sns = boto3.client("sns")

def get_ssm_param(param_name):
    """Fetch a string parameter from AWS Systems Manager."""
    try:
        response = ssm_client.get_parameter(Name=param_name, WithDecryption=False)
        return response['Parameter']['Value']
    except Exception as e:
        print(f"Error fetching {param_name} from SSM: {e}")
        raise

# ── Dynamic SSM Parameters ───────────────────────────────────────────────
BRONZE_BUCKET = get_ssm_param("/ishita-project1/buckets/bronze")
BASE_URL      = get_ssm_param("/ishita-project1/api/bts_base_url")
FILE_PREFIX   = get_ssm_param("/ishita-project1/api/file_prefix")

CHUNK_SIZE    = int(get_ssm_param("/ishita-project1/tuning/chunk_size_bytes"))
MIN_FILE_SIZE = int(get_ssm_param("/ishita-project1/tuning/min_file_size_bytes"))
HTTP_TIMEOUT  = int(get_ssm_param("/ishita-project1/tuning/http_timeout_sec"))
SNS_TOPIC_ARN = get_ssm_param("/ishita-project1/sns/alert_topic_arn")

# ── Static / Runtime Parameters ──────────────────────────────────────────
MAX_RETRIES  = 3
MAX_SOURCE_RETRIES = 14  # Days/runs before a SOURCE gap escalates to CODE
current_date = datetime.utcnow()
CURRENT_YM   = f"{current_date.year:04d}-{current_date.month:02d}"

# ── State Management Parameters ──────────────────────────────────────────
LAST_LOAD_DTM_SSM = "/ishita-project1/state/last_load_dtm"
BASELINE          = get_ssm_param("/ishita-project1/state/baseline")

# ── Job-level Parameters ─────────────────────────────────────────────────
REPROCESS   = str(get_param('reprocess', 'False')).strip().lower() in ('true', '1')
START_DATE_PARAM = get_param('start_date', None)  
END_DATE_PARAM   = get_param('end_date',   None)  


# ═════════════════════════════════════════════════════════════════════════════
# NOTIFICATION & SUMMARY MODULE
# ═════════════════════════════════════════════════════════════════════════════

def send_sns_email(subject, message_body):
    """Publish job summary to SNS."""
    try:
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=message_body)
        print(f"  [SUCCESS] Notification sent: {subject}")
    except Exception as e:
        print(f"  [WARNING] SNS Email failed: {e}")

def build_summary(job_name, status, mode, duration, start, end,
                  old_wm, new_wm, counts, results):
    """Builds a structured summary for the Bronze job."""
    icon = {"success": "SUCCESS", "skipped": "SKIPPED", "missing": "MISSING", "failed": "ERROR"}
    wm_line = (f"{old_wm} to {new_wm}" if new_wm != old_wm else f"{old_wm} (No change)")
    total = sum(counts.values())

    lines = [
        "════════════════════════════════════════════════════════════",
        "  DATA PIPELINE NOTIFICATION: BRONZE INGESTION",
        "════════════════════════════════════════════════════════════",
        f"  • Job Name:        {job_name}",
        f"  • Overall Result:  {status}",
        f"  • Run Mode:        {mode.upper()}",
        f"  • Execution Time:  {duration:.0f} seconds",
        f"  • Target Period:   {start} to {end}",
        f"  • Bronze Progress: {wm_line}",
        f"  • Months Scanned:  {total}",
        "",
        "  PROCESSING BREAKDOWN:",
        f"  • Months Succeeded: {counts['success']}",
        f"  • Months Skipped:   {counts['skipped']}",
        f"  • Months Missing:   {counts['missing']}",
        f"  • Months Failed:    {counts['failed']}",
    ]

    if results:
        lines.append("")
        lines.append("  DETAILED MONTHLY STATUS:")
        for ym, res in results:
            lines.append(f"    - [{icon[res]}] {ym}")

    lines.append("════════════════════════════════════════════════════════════")
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# STATE MANAGEMENT
# ═════════════════════════════════════════════════════════════════════════════

def get_last_load_dtm():
    """Read last_load_dtm from SSM, or bootstrap with baseline if missing."""
    try:
        response = ssm_client.get_parameter(Name=LAST_LOAD_DTM_SSM)
        value = response['Parameter']['Value'].strip()
        validate_ym(value, "last_load_dtm from SSM")
        return value
    except ssm_client.exceptions.ParameterNotFound:
        print(f"  last_load_dtm not found — bootstrapping with {BASELINE}")
        put_last_load_dtm(BASELINE)
        return BASELINE

def put_last_load_dtm(value):
    """Write updated watermark back to SSM."""
    validate_ym(value, "last_load_dtm to persist")
    ssm_client.put_parameter(
        Name=LAST_LOAD_DTM_SSM,
        Value=value,
        Type="String",
        Overwrite=True,
        Description="Most recently ingested YYYY-MM for Bronze layer."
    )
    print(f"  last_load_dtm updated in SSM → {value}")


# ═════════════════════════════════════════════════════════════════════════════
# YYYY-MM HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def validate_ym(value, label="value"):
    try:
        datetime.strptime(value, "%Y-%m")
    except (ValueError, TypeError):
        raise ValueError(f"{label} must be in YYYY-MM format, got: {value!r}")

def parse_ym(ym):
    y, m = ym.split("-")
    return int(y), int(m)

def next_ym(ym):
    """Returns the month strictly AFTER ym (e.g., '2025-12' → '2026-01')."""
    y, m = parse_ym(ym)
    m += 1
    if m > 12:
        m = 1
        y += 1
    return f"{y:04d}-{m:02d}"

def ym_range(start_ym, end_ym):
    validate_ym(start_ym, "range start")
    validate_ym(end_ym,   "range end")
    cur = start_ym
    while cur <= end_ym:
        yield cur
        cur = next_ym(cur)


# ═════════════════════════════════════════════════════════════════════════════
# DATE RANGE RESOLUTION
# ═════════════════════════════════════════════════════════════════════════════

def resolve_run_window():
    """Determines ingestion window based purely on params and SSM state."""
    last_load_dtm = get_last_load_dtm()

    if REPROCESS:
        if not START_DATE_PARAM:
            raise ValueError("reprocess=True requires start_date (YYYY-MM).")
        start_ym = START_DATE_PARAM.strip()
        end_ym   = (END_DATE_PARAM.strip() if END_DATE_PARAM else CURRENT_YM)

        validate_ym(start_ym, "start_date")
        validate_ym(end_ym,   "end_date")

        if start_ym > end_ym:
            raise ValueError(f"start_date ({start_ym}) is after end_date ({end_ym}).")
        return "reprocess", start_ym, end_ym, last_load_dtm

    # Incremental logic: strictly greater than watermark
    start_ym = next_ym(last_load_dtm)
    end_ym   = CURRENT_YM
    return "incremental", start_ym, end_ym, last_load_dtm


# ═════════════════════════════════════════════════════════════════════════════
# BRONZE COMPLETION MARKERS & ESCALATION
# ═════════════════════════════════════════════════════════════════════════════

def _marker_key(year, month):
    return f"flights/year={year}/month={month}/_FAILED"

def _csv_key(year, month):
    return f"flights/year={year}/month={month}/flights_{year}_{month}.csv"

def _now_iso():
    return datetime.utcnow().isoformat() + "Z"

def _read_marker_body_if_exists(key):
    try:
        resp = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
        return resp['Body'].read().decode("utf-8")
    except Exception:
        return None

def _parse_attempts(body):
    match = re.search(r'attempts=(\d+)', body)
    return int(match.group(1)) if match else 0

def _parse_first_seen(body):
    match = re.search(r'first_seen=([^\s|]+)', body)
    return match.group(1) if match else None

def bronze_csv_exists(year, month):
    try:
        s3.head_object(Bucket=BRONZE_BUCKET, Key=_csv_key(year, month))
        return True
    except Exception:
        return False

def write_failed_marker(year, month, reason):
    """Writes _FAILED marker with auto-escalation for persistent SOURCE gaps."""
    key = _marker_key(year, month)
    existing = _read_marker_body_if_exists(key)

    if reason.startswith("SOURCE:"):
        if existing and existing.startswith("CODE:"):
            # Already escalated — do not downgrade back to SOURCE.
            # Silently retain the existing CODE marker.
            return
            
        if existing and existing.startswith("SOURCE:"):
            attempts = _parse_attempts(existing) + 1
            first_seen = _parse_first_seen(existing) or _now_iso()
            
            if attempts >= MAX_SOURCE_RETRIES:
                clean_reason = reason[len("SOURCE: "):].strip()
                reason = (f"CODE: Escalated after {attempts} attempts — {clean_reason} "
                          f"(first_seen={first_seen}, escalated_at={_now_iso()})")
                print(f"  [ESCALATION] Month {year}-{month} exceeded {MAX_SOURCE_RETRIES} attempts.")
                send_sns_email(
                    subject=f"[ESCALATION] {year}-{month} — Source gap exceeded retry budget",
                    message_body=f"Month: {year}-{month}\nEscalated to CODE after {attempts} failed attempts.\nReason: {clean_reason}\nFirst seen: {first_seen}\n\nPipeline will not auto-retry this month again. Analyst intervention required."
                )
            else:
                reason = f"{reason} | attempts={attempts} | first_seen={first_seen}"
        else:
            reason = f"{reason} | attempts=1 | first_seen={_now_iso()}"

    s3.put_object(Bucket=BRONZE_BUCKET, Key=key, Body=reason.encode("utf-8"))

def delete_failed_marker(year, month):
    try:
        s3.delete_object(Bucket=BRONZE_BUCKET, Key=_marker_key(year, month))
    except Exception:
        pass

def scan_source_markers(phase2_start_ym):
    """
    Deterministically walks historical gaps strictly below the Phase 2 boundary.
    Returns SOURCE markers to retry, and CODE markers that require visibility.
    """
    retry_months = []
    escalated_gaps = []
    last_load_dtm = get_last_load_dtm()
    
    for ym_str in ym_range(BASELINE, last_load_dtm):
        if ym_str >= phase2_start_ym:
            continue
            
        year, month = ym_str.split("-")
        
        # Only check months where the final CSV output is missing
        if not bronze_csv_exists(year, month):
            body = _read_marker_body_if_exists(_marker_key(year, month))
            # Retry if no marker exists (implicit gap) or it's a SOURCE marker
            if not body or body.startswith("SOURCE:"):
                retry_months.append((year, month))
            elif body.startswith("CODE:"):
                escalated_gaps.append((year, month))
                
    if retry_months:
        print(f"  Found {len(retry_months)} Bronze gap(s) to retry.")
    if escalated_gaps:
        print(f"  [ACTION REQUIRED] Found {len(escalated_gaps)} escalated CODE gaps blocking full completion.")
        
    return retry_months, escalated_gaps


# ═════════════════════════════════════════════════════════════════════════════
# S3 & HTTP HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def upload_with_retry(local_path, s3_key):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            s3.upload_file(local_path, BRONZE_BUCKET, s3_key)
            return
        except Exception as e:
            if attempt == MAX_RETRIES: raise
            wait = 2 ** attempt
            print(f"  [RETRY] S3 upload attempt {attempt}/{MAX_RETRIES} failed. Retrying in {wait}s...")
            time.sleep(wait)

def download_with_retry(url, dest_path):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            bytes_written = 0
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp, \
                 open(dest_path, "wb") as out:
                while True:
                    chunk = resp.read(CHUNK_SIZE)
                    if not chunk: break
                    out.write(chunk)
                    bytes_written += len(chunk)
            return bytes_written

        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                raise
            if attempt == MAX_RETRIES: raise
            wait = 2 ** attempt
            print(f"  [RETRY] Download attempt {attempt}/{MAX_RETRIES} failed (HTTP {e.code}). Retrying in {wait}s...")
            time.sleep(wait)
        except (urllib.error.URLError, OSError) as e:
            if attempt == MAX_RETRIES: raise
            wait = 2 ** attempt
            print(f"  [RETRY] Download attempt {attempt}/{MAX_RETRIES} failed. Retrying in {wait}s...")
            time.sleep(wait)

def extract_csv_from_zip(zip_path, csv_dest):
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV file found inside {zip_path}")
        csv_name = csv_names[0]
        print(f"  Extracting: {csv_name}")
        with zf.open(csv_name) as src, open(csv_dest, "wb") as dst:
            while True:
                chunk = src.read(CHUNK_SIZE)
                if not chunk: break
                dst.write(chunk)


# ═════════════════════════════════════════════════════════════════════════════
# CORE INGESTION
# ═════════════════════════════════════════════════════════════════════════════

def ingest_month(year, month):
    month_key = f"{year}-{month}"
    month_int = int(month)         

    if not REPROCESS and bronze_csv_exists(year, month):
        delete_failed_marker(year, month)
        print(f"  [SKIPPED] Data already exists for {month_key}.")
        return "skipped"

    # Honor escalated CODE markers — skip download entirely.
    if not REPROCESS:
        existing = _read_marker_body_if_exists(_marker_key(year, month))
        if existing and existing.startswith("CODE:"):
            print(f"  [FAILED] {month_key} — CODE marker in place, skipping (analyst must clear).")
            return "failed"

    s3_key       = f"flights/year={year}/month={month}/flights_{year}_{month}.csv"
    zip_filename = f"{FILE_PREFIX}_{year}_{month_int}.zip"
    url          = f"{BASE_URL}/{zip_filename}"
    tmp_zip      = f"/tmp/flights_{year}_{month}.zip"
    tmp_csv      = f"/tmp/flights_{year}_{month}.csv"

    print(f"\n  {'─' * 56}")
    print(f"  INGESTING: {month_key}")
    print(f"  Source:    {url}")
    print(f"  Target:    s3://{BRONZE_BUCKET}/{s3_key}")
    print(f"  {'─' * 56}")

    try:
        print(f"  [1/3] Downloading ZIP...")
        bytes_downloaded = download_with_retry(url, tmp_zip)
        zip_mb = bytes_downloaded / 1024 / 1024
        print(f"          Downloaded {zip_mb:.1f} MB")

        if bytes_downloaded < MIN_FILE_SIZE:
            reason = "SOURCE: File too small (source has not published yet)"
            print(f"  [MISSING] Data not available for {month_key}.")
            write_failed_marker(year, month, reason)
            return "missing"

        print(f"  [2/3] Extracting CSV from ZIP...")
        extract_csv_from_zip(tmp_zip, tmp_csv)
        csv_mb = os.path.getsize(tmp_csv) / 1024 / 1024
        print(f"          Extracted {csv_mb:.1f} MB CSV")

        if csv_mb < (MIN_FILE_SIZE / 1024 / 1024):
            reason = f"SOURCE: CSV too small after extraction ({csv_mb:.2f} MB)"
            print(f"  [MISSING] Data not available for {month_key}.")
            write_failed_marker(year, month, reason)
            return "missing"

        # SAFELY WIPE OLD DATA NOW THAT SOURCE IS CONFIRMED (Prevent Destructive Reprocessing)
        if REPROCESS:
            try:
                s3.delete_object(Bucket=BRONZE_BUCKET, Key=s3_key)
                print(f"  [REPROCESS] Cleared any existing S3 CSV to ensure clean state.")
            except Exception:
                pass

        print(f"  [3/3] Uploading raw CSV to Bronze...")
        upload_with_retry(tmp_csv, s3_key)

        delete_failed_marker(year, month)

        print(f"  [SUCCESS] {month_key} — {csv_mb:.1f} MB ingested.")
        return "success"

    except urllib.error.HTTPError as e:
        if e.code == 404 or 500 <= e.code < 600:
            reason = f"SOURCE: HTTP {e.code}"
            print(f"  [MISSING] {month_key} — {reason}")
            write_failed_marker(year, month, reason)
            return "missing"
        elif e.code == 403:
            reason = f"CODE: HTTP 403 Forbidden — likely auth/IAM issue"
            print(f"  [FAILED] {month_key} — {reason}")
            write_failed_marker(year, month, reason)
            return "failed"
        else:
            reason = f"CODE: HTTP {e.code}: {e}"
            print(f"  [FAILED] {month_key} — {reason}")
            write_failed_marker(year, month, reason)
            return "failed"
    except urllib.error.URLError as e:
        reason = f"SOURCE: URLError — {e.reason}"
        print(f"  [MISSING] Network failure for {month_key} — {reason}")
        write_failed_marker(year, month, reason)
        return "missing"
    except zipfile.BadZipFile:
        reason = "SOURCE: Corrupt ZIP at source"
        print(f"  [MISSING] {month_key} — {reason}")
        write_failed_marker(year, month, reason)
        return "missing"
    except Exception as e:
        reason = f"CODE: {type(e).__name__}: {e}"
        print(f"  [FAILED] Unexpected failure for {month_key} — {reason}")
        write_failed_marker(year, month, reason)
        return "failed"
    finally:
        for f in [tmp_zip, tmp_csv]:
            if os.path.exists(f): os.remove(f)


# ═════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

def main():
    job_name  = args.get('JOB_NAME', 'Local-Run')
    job_start = datetime.utcnow()

    mode, start_ym, end_ym, last_load_dtm_at_start = resolve_run_window()

    print("=" * 60)
    print("  US FLIGHT DATA — BRONZE LAYER INGESTION")
    print("=" * 60)
    print(f"  Job Name:         {job_name}")
    print(f"  Started:          {job_start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  Mode:             {mode.upper()}")
    print(f"  last_load_dtm:    {last_load_dtm_at_start}   (SSM: {LAST_LOAD_DTM_SSM})")
    print(f"  Run window:       {start_ym}  →  {end_ym}   (inclusive)")
    print("=" * 60)

    counts        = {"success": 0, "skipped": 0, "missing": 0, "failed": 0}
    results       = []
    new_watermark = last_load_dtm_at_start

    # ── Phase 1 — Gap Fill (incremental mode only) ────────────────────────
    if mode == "incremental":
        source_retries, escalated_gaps = scan_source_markers(phase2_start_ym=start_ym)
        
        if source_retries:
            print(f"\n  PHASE 1: Retrying {len(source_retries)} source-failed month(s)")
            for year, month_str in source_retries:
                month_key = f"{year}-{month_str}"
                try:
                    result = ingest_month(year, month_str)
                except Exception as e:
                    print(f"  [FAILED] Unhandled crash in ingest_month for {month_key}: {e}")
                    result = "failed"
                    try:
                        write_failed_marker(year, month_str, f"CODE: Unhandled crash: {e}")
                    except Exception:
                        pass
                
                counts[result] += 1
                results.append((month_key, result))
                print(f"  [RETRY] {month_key} → {result}")
        else:
            print("\n  PHASE 1: No source-failed markers to retry.")

        # Ensure CODE gaps force a failure so the pipeline is not silently green
        if escalated_gaps:
            counts["failed"] += len(escalated_gaps)
            for gap_year, gap_month in escalated_gaps:
                results.append((f"{gap_year}-{gap_month}", "failed"))

    # ── Phase 2 — New Months ──────────────────────────────────────────────
    if start_ym > end_ym:
        print("\n  PHASE 2: No new months to ingest — pipeline is up to date.")
        if not results:
            send_sns_email(f"[NO NEW DATA] Glue Job: {job_name}", "No new months to ingest. Pipeline is up to date.")
            return
    else:
        print(f"\n  PHASE 2: Processing new months {start_ym} → {end_ym}.")
        for month_key in ym_range(start_ym, end_ym):
            year, month_str = month_key.split("-")
            try:
                result = ingest_month(year, month_str)
            except Exception as e:
                print(f"  [FAILED] Unhandled crash in ingest_month for {month_key}: {e}")
                result = "failed"
                try:
                    write_failed_marker(year, month_str, f"CODE: Unhandled crash: {e}")
                except Exception:
                    pass
            
            counts[result] += 1
            results.append((month_key, result))

    # ── Calculate Leapfrog Watermark ─────────────────────────────────────
    if mode == "incremental":
        advanced_months = [m for (m, r) in results if r in ("success", "skipped")]
        if advanced_months:
            candidate = max(advanced_months)
            if candidate > last_load_dtm_at_start:
                new_watermark = candidate

    # ── Job summary & Status Compilation ──────────────────────────────────
    job_end  = datetime.utcnow()
    duration = (job_end - job_start).total_seconds()

    watermark_stalled = (
        mode == "incremental"
        and bool(results)
        and new_watermark == last_load_dtm_at_start
    )

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

    summary_text = build_summary(
        job_name, overall_status, mode, duration, start_ym, end_ym,
        last_load_dtm_at_start, new_watermark, counts, results
    )

    print(f"\n{summary_text}")

    # ── State update execution ───────────────────────────────────────────
    print("\n" + "─" * 60)
    if REPROCESS:
        print(f"  last_load_dtm unchanged (reprocess mode): ({last_load_dtm_at_start})")
    elif new_watermark != last_load_dtm_at_start:
        put_last_load_dtm(new_watermark)
    else:
        print(f"  last_load_dtm UNCHANGED — no new months in window: ({last_load_dtm_at_start})")

    print("─" * 60)
    print(f"  >>> OVERALL STATUS: {overall_status} <<<")
    print("─" * 60)

    send_sns_email(f"[{overall_status}] Glue Job: {job_name}", summary_text)

    # ── Step Functions Hard Fail ─────────────────────────────────────────
    if results and counts["success"] == 0 and counts["skipped"] == 0 and counts["missing"] == 0 and counts["failed"] > 0:
        raise RuntimeError(f"TOTAL FAILURE — all {counts['failed']} attempted month(s) failed. Check CloudWatch logs.")

if __name__ == "__main__":
    main()