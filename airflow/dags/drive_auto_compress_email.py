"""
Loan data ingestion and ETL pipeline orchestrated with Apache Airflow.

This DAG:
- polls a Google Drive folder for new loan_*.csv files,
- lands each file into a shared Docker volume,
- uploads raw and compressed copies to a MinIO bucket,
- runs a PySpark ETL job to clean and aggregate the loan data,
- sends an email summary with compression metrics and top loan segments.

Author: Aadarsh
"""


import json
import os
import gzip
import logging
import sys
from datetime import timedelta
from pathlib import Path
from typing import Dict, Any, List

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from minio import Minio

# ---------------------------------------------------------------------------
# Here the ../include folder importable (google_drive_download.py lives there)
# ---------------------------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
INCLUDE_DIR = DAGS_DIR.parent / "include"
if str(INCLUDE_DIR) not in sys.path:
    sys.path.insert(0, str(INCLUDE_DIR))

from google_drive_download import (
    list_files_in_folder,
    download_file_from_drive,
)
from loan_spark_etl import run_loan_spark_etl, get_latest_aggregates_summary

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Google Drive folder to watch (set your real folder ID here)
DRIVE_FOLDER_ID = os.getenv("DRIVE_WATCH_FOLDER_ID", "REPLACE_ME_WITH_REAL_FOLDER_ID")

# File pattern
FILE_NAME_PREFIX = "loan_"
FILE_MIME_TYPE = "text/csv"

# Local paths inside the Airflow containers
SHARED_INCOMING_DIR = "/shared_data/incoming"
SHARED_RAW_DIR = "/shared_data/raw"      # optional, for future ETL
SHARED_OUTPUT_DIR = "/shared_data/output"

# Processed file tracking
PROCESSED_JSON_PATH = "/opt/airflow/processed_drive_files.json"

# MinIO configuration (containers talk to "minio" host name)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadminpassword")
MINIO_BUCKET = os.getenv("MINIO_DEFAULT_BUCKET", "uploaded-files")
MINIO_SECURE = False  # we use http, not https, inside docker compose

# File age threshold (in seconds) to consider "fully uploaded" on Drive
MIN_FILE_AGE_SECONDS = int(os.getenv("MIN_FILE_AGE_SECONDS", "60"))

# Email recipients (Airflow SMTP is configured via env vars in docker-compose)
SMTP_TO = os.getenv("SMTP_TO", "recipient@example.com")
SMTP_FROM = os.getenv("SMTP_FROM", os.getenv("SMTP_USER", "no-reply@example.com"))

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper functions: processed files tracking
# ---------------------------------------------------------------------------


def _load_processed_file_ids() -> List[str]:
    """
    Load already-processed Google Drive file IDs from JSON.
    """
    if not os.path.exists(PROCESSED_JSON_PATH):
        return []

    try:
        with open(PROCESSED_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("processed_file_ids", [])
    except Exception as e:
        log.warning("Failed to read processed file JSON: %s", e)
        return []


def _save_processed_file_ids(file_ids: List[str]) -> None:
    """
    Persist processed file IDs to JSON.
    """
    payload = {
        "processed_file_ids": file_ids,
        "last_updated": pendulum.now("UTC").to_iso8601_string(),
    }
    os.makedirs(os.path.dirname(PROCESSED_JSON_PATH), exist_ok=True)
    with open(PROCESSED_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


# ---------------------------------------------------------------------------
# Helper functions: MinIO
# ---------------------------------------------------------------------------


def _get_minio_client() -> Minio:
    """
    Create a MinIO client using env vars from docker-compose .env
    """
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    return client


def _ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    """
    Create bucket if it does not exist.
    """
    found = client.bucket_exists(bucket_name)
    if not found:
        log.info("MinIO bucket '%s' not found, creating...", bucket_name)
        client.make_bucket(bucket_name)
    else:
        log.debug("MinIO bucket '%s' already exists", bucket_name)


def _upload_file_to_minio(
    client: Minio, bucket_name: str, object_name: str, file_path: str
) -> str:
    """
    Upload a local file to MinIO.

    Returns the object URL (path inside the bucket).
    """
    client.fput_object(bucket_name, object_name, file_path)
    return f"s3://{bucket_name}/{object_name}"


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def process_new_drive_files(**context) -> None:
    """
    Main function executed by the PythonOperator.

    It performs the full pipeline:
    - list Drive files
    - filter new + fully uploaded
    - download
    - upload to MinIO (raw + compressed)
    - compute stats
    - update processed JSON
    - send email summary
    """
    if DRIVE_FOLDER_ID.startswith("REPLACE_ME"):
        log.error(
            "DRIVE_FOLDER_ID is not configured. "
            "Set env var DRIVE_WATCH_FOLDER_ID or edit DRIVE_FOLDER_ID constant in the DAG."
        )
        return

    log.info("Starting Drive poll for folder_id=%s", DRIVE_FOLDER_ID)

    # 1. Load already processed file ids
    processed_ids = set(_load_processed_file_ids())
    log.info("Loaded %d processed file_ids", len(processed_ids))

    # 2. List candidate files from Google Drive
    all_files = list_files_in_folder(
        folder_id=DRIVE_FOLDER_ID,
        name_prefix=FILE_NAME_PREFIX,
        mime_type=FILE_MIME_TYPE,
    )
    log.info("Found %d candidate files matching prefix '%s'", len(all_files), FILE_NAME_PREFIX)

    # 3. Filter by name + age + not processed
    now = pendulum.now("UTC")
    new_files: List[Dict[str, Any]] = []

    for f in all_files:
        file_id = f["id"]
        name = f.get("name", "")
        modified_time_str = f.get("modifiedTime")

        if not name.startswith(FILE_NAME_PREFIX) or not name.endswith(".csv"):
            continue

        if file_id in processed_ids:
            log.debug("Skipping already processed file: %s (%s)", name, file_id)
            continue

        if not modified_time_str:
            log.debug("Skipping file without modifiedTime: %s (%s)", name, file_id)
            continue

        try:
            modified_time = pendulum.parse(modified_time_str)
        except Exception as e:
            log.warning(
                "Failed to parse modifiedTime '%s' for file %s (%s): %s",
                modified_time_str,
                name,
                file_id,
                e,
            )
            continue

        age_seconds = (now - modified_time).total_seconds()
        if age_seconds < MIN_FILE_AGE_SECONDS:
            log.info(
                "Skipping file %s (%s) because it is too recent (age=%.1fs, min=%ds)",
                name,
                file_id,
                age_seconds,
                MIN_FILE_AGE_SECONDS,
            )
            continue

        new_files.append(f)

    if not new_files:
        log.info("No new eligible files to process.")
        return

    log.info("Processing %d new file(s)", len(new_files))

    # Ensure local directories exist
    os.makedirs(SHARED_INCOMING_DIR, exist_ok=True)
    os.makedirs(SHARED_OUTPUT_DIR, exist_ok=True)
    os.makedirs(SHARED_RAW_DIR, exist_ok=True)

    # Prepare MinIO client and bucket
    minio_client = _get_minio_client()
    _ensure_bucket_exists(minio_client, MINIO_BUCKET)

    processed_summaries = []

    # 4. Process each file
    for f in new_files:
        file_id = f["id"]
        name = f["name"]

        log.info("Processing file: %s (%s)", name, file_id)

        # Local paths
        local_incoming_path = os.path.join(SHARED_INCOMING_DIR, name)
        local_raw_path = os.path.join(SHARED_RAW_DIR, name)
        compressed_filename = f"{name}.gz"
        local_compressed_path = os.path.join(SHARED_OUTPUT_DIR, compressed_filename)

        # 4.1 Download from Google Drive
        log.info("Downloading from Drive to %s", local_incoming_path)
        download_file_from_drive(file_id, local_incoming_path)

        # copy/move to raw folder for organization
        try:
            # Copy to raw folder (keep incoming as a "landing" zone)
            with open(local_incoming_path, "rb") as src, open(
                local_raw_path, "wb"
            ) as dst:
                dst.write(src.read())
        except Exception as e:
            log.warning("Failed to copy file to raw folder: %s", e)

        # 4.2 Compute original size
        original_size = os.path.getsize(local_incoming_path)

        # 4.3 Upload raw file to MinIO
        raw_object_name = f"raw/{name}"
        raw_object_uri = _upload_file_to_minio(
            minio_client, MINIO_BUCKET, raw_object_name, local_incoming_path
        )

        # 4.4 Compress locally
        log.info("Compressing %s to %s", local_incoming_path, local_compressed_path)
        with open(local_incoming_path, "rb") as src, gzip.open(
            local_compressed_path, "wb"
        ) as gz:
            gz.writelines(src)

        compressed_size = os.path.getsize(local_compressed_path)
        compression_ratio = (
            compressed_size / original_size if original_size > 0 else 0
        )

        # 4.5 Upload compressed file to MinIO
        compressed_object_name = f"compressed/{compressed_filename}"
        compressed_object_uri = _upload_file_to_minio(
            minio_client, MINIO_BUCKET, compressed_object_name, local_compressed_path
        )

        # Track as processed
        processed_ids.add(file_id)

        # Collect summary
        processed_summaries.append(
            {
                "file_id": file_id,
                "filename": name,
                "original_size": original_size,
                "compressed_size": compressed_size,
                "compression_ratio": compression_ratio,
                "raw_object_uri": raw_object_uri,
                "compressed_object_uri": compressed_object_uri,
            }
        )

        log.info(
            "Finished %s: original=%d bytes, compressed=%d bytes, ratio=%.3f",
            name,
            original_size,
            compressed_size,
            compression_ratio,
        )

    # 5. Save processed ids to JSON
    _save_processed_file_ids(sorted(processed_ids))

    # Return summaries so the next task (email) can use them via XCom
    return processed_summaries


def _format_size(bytes_count: int) -> str:
    """
    Human-readable size string.
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_count < 1024:
            return f"{bytes_count:.2f} {unit}"
        bytes_count /= 1024.0
    return f"{bytes_count:.2f} PB"


def _send_summary_email(
    processed_summaries: List[Dict[str, Any]],
    spark_aggregates: List[Dict[str, Any]] | None = None,
) -> None:
    """
    Compose and send a professional-looking HTML email with:
    - File compression summary
    - Optional Spark ETL aggregates
    """
    total_files = len(processed_summaries)
    processed_at = pendulum.now("Asia/Kathmandu").to_datetime_string()

    subject = f"[Loan ETL] {total_files} file(s) processed – drive_auto_compress_email"

    # ------------------------------------------------------------------
    # Compression rows
    # ------------------------------------------------------------------
    compression_rows_html = ""
    for s in processed_summaries:
        compression_rows_html += f"""
            <tr>
                <td>{s['filename']}</td>
                <td style="text-align:right;">{_format_size(s['original_size'])}</td>
                <td style="text-align:right;">{_format_size(s['compressed_size'])}</td>
                <td style="text-align:right;">{s['compression_ratio']:.3f}</td>
                <td style="font-size:11px;"><code>{s['raw_object_uri']}</code></td>
                <td style="font-size:11px;"><code>{s['compressed_object_uri']}</code></td>
            </tr>
        """

    # ------------------------------------------------------------------
    #Spark aggregate rows
    # ------------------------------------------------------------------
    spark_table_html = ""
    if spark_aggregates:
        agg_rows_html = ""
        for a in spark_aggregates:
            status = a.get("status", "-")
            product_type = a.get("product_type", "-")
            branch = a.get("branch", "-")
            loan_count = a.get("loan_count", 0)
            total_amount_val = a.get("total_amount") or 0.0

            agg_rows_html += f"""
                <tr>
                    <td>{status}</td>
                    <td>{product_type}</td>
                    <td>{branch}</td>
                    <td style="text-align:right;">{loan_count}</td>
                    <td style="text-align:right;">{float(total_amount_val):.2f}</td>
                </tr>
            """

        spark_table_html = f"""
            <h3 style="margin:24px 0 8px 0; font-size:16px; color:#333333;">
                Spark ETL – Loan Aggregates
            </h3>
            <p style="margin:0 0 8px 0; font-size:13px; color:#555555;">
                Top segments by <b>loan_count</b> from the latest Spark ETL run.
            </p>
            <table style="border-collapse:collapse; width:100%; font-size:13px;">
                <thead>
                    <tr style="background-color:#f5f5f5;">
                        <th style="border:1px solid #dddddd; padding:6px; text-align:left;">Status</th>
                        <th style="border:1px solid #dddddd; padding:6px; text-align:left;">Product Type</th>
                        <th style="border:1px solid #dddddd; padding:6px; text-align:left;">Branch</th>
                        <th style="border:1px solid #dddddd; padding:6px; text-align:right;">Loan Count</th>
                        <th style="border:1px solid #dddddd; padding:6px; text-align:right;">Total Amount</th>
                    </tr>
                </thead>
                <tbody>
                    {agg_rows_html}
                </tbody>
            </table>
        """

    # ------------------------------------------------------------------
    # Final HTML 
    # ------------------------------------------------------------------
    html_content = f"""
    <div style="font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
                font-size:14px; color:#333333; background-color:#f4f6f8; padding:24px;">
      <div style="max-width:900px; margin:0 auto; background-color:#ffffff; border-radius:6px;
                  box-shadow:0 2px 6px rgba(0,0,0,0.04); overflow:hidden;">

        <!-- Header bar -->
        <div style="background:linear-gradient(90deg,#2563eb,#1d4ed8); padding:16px 20px;">
          <h2 style="margin:0; font-size:18px; color:#ffffff;">
            Loan ETL – Processing Summary
          </h2>
          <p style="margin:4px 0 0 0; font-size:12px; color:#dbeafe;">
            Airflow DAG: <code style="color:#bfdbfe;">drive_auto_compress_email</code>
          </p>
        </div>

        <!-- Body -->
        <div style="padding:20px 20px 24px 20px;">
          <p style="margin-top:0;">Hello,</p>

          <p style="margin:0 0 12px 0;">
            The ETL pipeline has successfully processed
            <b>{total_files}</b> file(s) from Google Drive and stored the results in MinIO and Spark.
          </p>

          <table style="margin:0 0 12px 0; font-size:13px;">
            <tr>
              <td style="padding-right:16px;"><b>Run time:</b></td>
              <td>{processed_at} (Asia/Kathmandu)</td>
            </tr>
            <tr>
              <td style="padding-right:16px;"><b>Drive folder ID:</b></td>
              <td><code>{DRIVE_FOLDER_ID}</code></td>
            </tr>
            <tr>
              <td style="padding-right:16px;"><b>MinIO bucket:</b></td>
              <td><code>{MINIO_BUCKET}</code></td>
            </tr>
          </table>

          <h3 style="margin:16px 0 8px 0; font-size:16px; color:#333333;">
            File Compression Summary
          </h3>

          <table style="border-collapse:collapse; width:100%; font-size:13px;">
            <thead>
              <tr style="background-color:#f5f5f5;">
                <th style="border:1px solid #dddddd; padding:6px; text-align:left;">Filename</th>
                <th style="border:1px solid #dddddd; padding:6px; text-align:right;">Original Size</th>
                <th style="border:1px solid #dddddd; padding:6px; text-align:right;">Compressed Size</th>
                <th style="border:1px solid #dddddd; padding:6px; text-align:right;">Compression Ratio</th>
                <th style="border:1px solid #dddddd; padding:6px; text-align:left;">MinIO Raw Object</th>
                <th style="border:1px solid #dddddd; padding:6px; text-align:left;">MinIO Compressed Object</th>
              </tr>
            </thead>
            <tbody>
              {compression_rows_html}
            </tbody>
          </table>

          {spark_table_html}

          <p style="margin-top:20px; font-size:12px; color:#6b7280;">
            Regards,<br/>
            <span style="font-style:italic;">Airflow – drive_auto_compress_email</span>
          </p>
        </div>
      </div>
    </div>
    """

    try:
        log.info(
            "Sending summary email to %s (files=%d, spark_rows=%d)",
            SMTP_TO,
            total_files,
            len(spark_aggregates or []),
        )
        send_email(
            to=SMTP_TO,
            subject=subject,
            html_content=html_content,
        )
        log.info("Summary email successfully sent.")
    except Exception as e:
        log.warning("Failed to send summary email to %s: %s", SMTP_TO, e)




def run_spark_etl_task(**context) -> None:
    """
    Wrapper to run the PySpark ETL after files have been landed in /shared_data/raw.
    """
    log.info("Starting Spark ETL on raw loan CSV files...")
    cleaned_path = run_loan_spark_etl(
        input_dir=SHARED_RAW_DIR,
        output_dir=SHARED_OUTPUT_DIR,
    )

    if cleaned_path:
        log.info("Spark ETL completed successfully. Cleaned data at: %s", cleaned_path)
    else:
        log.info("Spark ETL found no input data to process.")


def send_combined_email_task(**context) -> None:
    """
    Pull processed file summaries from XCom and combine them
    with Spark aggregates for the final email.
    """
    ti = context["ti"]

    # XCom from poll_and_process_drive_files
    processed_summaries = ti.xcom_pull(task_ids="poll_and_process_drive_files")

    if not processed_summaries:
        log.info("No processed files in this run; skipping email.")
        return

    log.info(
        "Preparing email for %d processed file(s) with Spark aggregates.",
        len(processed_summaries),
    )

    spark_aggregates = get_latest_aggregates_summary()
    if spark_aggregates:
        log.info("Loaded %d Spark aggregate rows for email.", len(spark_aggregates))
    else:
        log.info("No Spark aggregates found; email will include compression summary only.")

    _send_summary_email(processed_summaries, spark_aggregates=spark_aggregates)



# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="drive_auto_compress_email",
    description="Poll Google Drive, compress CSVs, upload to MinIO, run Spark ETL, and email summary.",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Kathmandu"),
    schedule_interval="*/1 * * * *",  # every 1 minute
    catchup=False,
    max_active_runs=1,
    tags=["drive", "minio", "compression", "email", "spark"],
) as dag:
    poll_and_process = PythonOperator(
        task_id="poll_and_process_drive_files",
        python_callable=process_new_drive_files,
    )

    run_spark = PythonOperator(
        task_id="run_spark_etl",
        python_callable=run_spark_etl_task,
    )

    send_email_summary = PythonOperator(
        task_id="send_summary_email",
        python_callable=send_combined_email_task,
    )

    poll_and_process >> run_spark >> send_email_summary
