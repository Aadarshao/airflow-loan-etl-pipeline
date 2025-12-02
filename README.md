# Loan Data ETL Pipeline with Apache Airflow & PySpark

This repository contains an end-to-end automated loan data pipeline built with:

- **Apache Airflow** for scheduling & orchestration  
- **Google Drive** as the ingestion source  
- **MinIO** for object storage (raw + compressed files)  
- **PySpark** for transformation and aggregation  
- **SMTP** for sending summary email reports  

When a CSV file with the `loan_` prefix is uploaded to a configured Google Drive folder, the pipeline automatically:

1. Detects the new file  
2. Downloads it into a shared Docker volume  
3. Uploads raw + compressed versions to MinIO  
4. Executes a PySpark ETL job  
5. Outputs cleaned & aggregated Parquet files  
6. Sends an email summary containing:  
   - File size and compression statistics  
   - Key loan portfolio aggregates  

---

## 1. Architecture Overview

### High-Level Flow

1. A user uploads `loan_*.csv` to a Google Drive folder  
2. **Airflow DAG (`drive_auto_compress_email`)** runs every minute  
3. It:  
   - Polls Google Drive  
   - Downloads new files  
   - Uploads raw + compressed files to MinIO  
   - Executes the Spark ETL  
   - Sends a summary report email  
4. **PySpark ETL (`loan_spark_etl.py`)**:  
   - Reads all CSVs from `/shared_data/raw`  
   - Fills NULL values with column-wise mode  
   - Parses `created_at` into date & time  
   - Computes aggregates by status, product_type, and branch  
   - Writes outputs to `/shared_data/output/`

All containers share a Docker volume named **`shared_data`** for file exchange.

---

## 2. Repository Structure

```text
airflow-loan-etl-pipeline/
├─ airflow/
│  ├─ dags/
│  │  └─ drive_auto_compress_email.py      # Main Airflow DAG
│  ├─ include/
│  │  ├─ google_drive_download.py          # Google Drive utilities
│  │  ├─ loan_spark_etl.py                 # PySpark ETL logic
│  │  └─ __init__.py
│  ├─ shared_data/                         # Shared volume (ignored in Git)
│  ├─ logs/                                # Airflow logs (ignored)
│  ├─ Dockerfile
│  └─ .env                                 # Local environment config (ignored)
├─ docker-compose.yml
├─ .gitignore
└─ README.md
```

> **Note:** Sensitive items such as the Google service account JSON and `.env` are intentionally ignored via `.gitignore`.

---

## 3. Tech Stack

- **Apache Airflow** (Dockerized)
- **PySpark** (local mode)
- **MinIO** (S3-compatible storage)
- **Google Drive API** (service account)
- **SMTP** email backend
- **Docker / Docker Compose**

---

## 4. Prerequisites

- Docker and Docker Compose  
- A Google Cloud project with:
  - Service account with access to the Drive folder  
  - JSON key (placed in `airflow/include/drive_service_account.json`)  
- A working SMTP server  
- 

---

## 5. Configuration

### 5.1 Environment Variables (`.env`)

Key variables include:

#### **Google Drive**
DRIVE_WATCH_FOLDER_ID=<GOOGLE_DRIVE_FOLDER_ID>


#### **MinIO**
MINIO_ENDPOINT=minio:9000
MINIO_ROOT_USER=<user>
MINIO_ROOT_PASSWORD=<pass>
MINIO_DEFAULT_BUCKET=uploaded-files


#### **Email / SMTP**
SMTP_HOST=
SMTP_PORT=
SMTP_USER=
SMTP_PASSWORD=
SMTP_TO=<recipient>
SMTP_FROM=<optional>


#### **Other**

The Google service account JSON file must be placed at:
airflow/include/drive_service_account.json


### 5.2 Airflow Connections (Optional)

This setup uses environment variables for simplicity, but SMTP/MinIO can be migrated to Airflow Connections if needed.

---

## 6. Running the Project

From the project root:

```bash
# One-time initialization
docker compose up airflow-init

# Start the entire stack
docker compose up -d
```

Airflow Web UI: http://localhost:8080

Log in using the credentials defined in .env or docker-compose.yml.

The main DAG: drive_auto_compress_email
Runs every minute, or can be triggered manually.

---

## 7. How the DAG Works

### Task 1 — poll_and_process_drive_files

Lists CSVs in Google Drive with prefix loan_
Skips files that:
are too recent
don’t match name/mime patterns
were already processed (tracked in processed_drive_files.json)
Downloads new files to /shared_data/incoming
Copies files to /shared_data/raw for Spark ETL
Compresses to .gz in /shared_data/output
Uploads raw + compressed files to MinIO
Produces per-file compression metrics (XCom)

### Task 2 — run_spark_etl

Executes run_loan_spark_etl():
Loads raw CSVs
Fills NULLs with column-wise mode
Splits created_at into:created_date,created_time
Computes aggregates grouped by: status, product_type,branch

Writes Parquet outputs:
shared_data/output/cleaned/
shared_data/output/aggregates/

### Task 3 — send_summary_email

Pulls compression stats (XCom)
Fetches Spark aggregate results
Sends a formatted HTML email containing:
File compression table
Top aggregated loan segments

---

## 8. Testing the Pipeline

### 8.1 Ensure all services are running:
docker compose up -d

### 8.2 Upload a test CSV
(example)loan_test1.csv

Expected columns:
loan_id, customer_id, created_at, amount, interest_rate,
tenure_months, status, product_type, branch, credit_score_band

### 8.3 Wait for DAG to run (or trigger manually).

### 8.4 Verify:

Airflow: all tasks succeed

MinIO:
raw/loan_*.csv
compressed/loan_*.csv.gz

Shared volume:
airflow/shared_data/raw/
airflow/shared_data/output/cleaned/
airflow/shared_data/output/aggregates/

Email: summary email received

---

## 9. Limitations & Future Enhancements

Add schema validation
Move Spark to a cluster or standalone service
Enhance alerts (Airflow SLAs, failure handlers)
Add unit tests for ETL and DAG logic
Improve Drive integration with more robust error handling

---

## 10. Author

Developed by Aadarsh as part of a hands-on data engineering project involving Airflow, PySpark, MinIO, and Google Drive.

---