"""
PySpark ETL for standardized loan CSV files.

Expected input schema (columns in CSV):

    loan_id, customer_id, created_at, amount, interest_rate,
    tenure_months, status, product_type, branch, credit_score_band

Requirements:
- Ingest CSV (no ELT) from /shared_data/raw
- Transform:
    * Replace NULL values in each column with that column's mode (most frequent non-null value)
    * Split created_at into created_date (DATE) and created_time (STRING "HH:mm:ss")
- Generate consolidated insights:
    * Aggregated metrics grouped by status, product_type, branch:
        - loan_count
        - total_amount (sum of amount)
- Write outputs to /shared_data/output:
    * cleaned/    -> cleaned loan-level data (Parquet)
    * aggregates/ -> aggregated insights (Parquet)
"""

import os
from typing import Optional, List, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def _build_spark(app_name: str = "LoanSparkETL") -> SparkSession:
    """
    Create a SparkSession in local mode.

    This runs inside the Airflow container and uses the shared_data volume.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # keep all timestamps in UTC for consistency
        .config("spark.sql.session.timeZone", "UTC")
        # avoid SparkUpgradeException for ancient dates when writing parquet
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        # be lenient parsing older/legacy date formats
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    return spark


def _fill_nulls_with_mode(df: DataFrame) -> DataFrame:
    """
    For each column in the DataFrame, compute the mode (most frequent non-null value)
    and fill NULLs in that column with the mode.

    This is a demo-friendly approach for handling missing values.
    """
    for col in df.columns:
        # Compute mode: group by the column, count, order by count desc, ignore nulls
        mode_row = (
            df.groupBy(col)
            .count()
            .orderBy(F.col("count").desc())
            .filter(F.col(col).isNotNull())
            .limit(1)
            .collect()
        )
        if mode_row:
            mode_value = mode_row[0][0]
            df = df.fillna({col: mode_value})
    return df


def _transform_dates(df: DataFrame, datetime_col: str = "created_at") -> DataFrame:
    """
    Parse a datetime string column into separate date + time columns.

    Assumes format like 'YYYY-MM-DD HH:MM:SS'.
    """
    if datetime_col not in df.columns:
        return df

    ts_col = F.to_timestamp(F.col(datetime_col))
    df = df.withColumn("created_ts", ts_col)
    df = df.withColumn("created_date", F.to_date("created_ts"))
    df = df.withColumn("created_time", F.date_format("created_ts", "HH:mm:ss"))

    # Drop intermediate column if you don't want to keep it
    df = df.drop("created_ts")

    return df


def _build_aggregates(df: DataFrame) -> DataFrame:
    """
    Build consolidated insights:

    Group by:
        - status
        - product_type
        - branch

    Metrics:
        - loan_count
        - total_amount (sum of amount)
    """
    # Ensure amount is numeric
    if "amount" in df.columns:
        df = df.withColumn("amount_num", F.col("amount").cast("double"))
    else:
        df = df.withColumn("amount_num", F.lit(None).cast("double"))

    group_cols = []
    for c in ["status", "product_type", "branch"]:
        if c in df.columns:
            group_cols.append(c)

    if not group_cols:
        # Fallback: no grouping columns, return a single-row summary
        agg_df = df.agg(
            F.count(F.lit(1)).alias("loan_count"),
            F.sum("amount_num").alias("total_amount"),
        )
    else:
        agg_df = (
            df.groupBy(*group_cols)
            .agg(
                F.count(F.lit(1)).alias("loan_count"),
                F.sum("amount_num").alias("total_amount"),
            )
            .orderBy(*group_cols)
        )

    return agg_df


def run_loan_spark_etl(
    input_dir: str = "/shared_data/raw",
    output_dir: str = "/shared_data/output",
    cleaned_subdir: str = "cleaned",
    aggregates_subdir: str = "aggregates",
    coalesce_output: bool = True,
) -> Optional[str]:
    """
    Entry point for the Spark ETL.

    Reads all CSV files from input_dir, applies transformations, and writes
    cleaned + aggregated datasets to output_dir.

    Returns:
        Path to the cleaned dataset directory (for logging/debugging), or None if no input.
    """
    spark = _build_spark()

    input_path = os.path.join(input_dir, "*.csv")
    print(f"[Spark ETL] Reading input CSV(s) from: {input_path}")

    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

    if df.rdd.isEmpty():
        print("[Spark ETL] No input rows found, nothing to do.")
        spark.stop()
        return None

    # 1) Fill NULLs with per-column mode
    df = _fill_nulls_with_mode(df)

    # 2) Split datetime into date + time columns
    df = _transform_dates(df, datetime_col="created_at")

    # 3) Build aggregated metrics
    agg_df = _build_aggregates(df)

    # Prepare output paths
    cleaned_path = os.path.join(output_dir, cleaned_subdir)
    aggregates_path = os.path.join(output_dir, aggregates_subdir)

    os.makedirs(cleaned_path, exist_ok=True)
    os.makedirs(aggregates_path, exist_ok=True)

    print(f"[Spark ETL] Writing cleaned dataset to: {cleaned_path}")
    cleaned_out = df.coalesce(1) if coalesce_output else df
    cleaned_out.write.mode("overwrite").parquet(cleaned_path)

    print(f"[Spark ETL] Writing aggregates to: {aggregates_path}")
    agg_out = agg_df.coalesce(1) if coalesce_output else agg_df
    agg_out.write.mode("overwrite").parquet(aggregates_path)

    spark.stop()
    print("[Spark ETL] Completed successfully.")

    return cleaned_path


def get_latest_aggregates_summary(
    aggregates_dir: str = "/shared_data/output/aggregates", limit: int = 10
) -> List[Dict]:
    """
    Utility used by the Airflow DAG to pull a small, email-friendly summary
    of the latest aggregates.

    Returns a list of dicts with keys like:
        status, product_type, branch, loan_count, total_amount
    """
    if not os.path.isdir(aggregates_dir):
        print(f"[Spark ETL] Aggregates dir does not exist: {aggregates_dir}")
        return []

    spark = _build_spark(app_name="LoanSparkETL_ReadAggregates")

    try:
        df = spark.read.parquet(aggregates_dir)

        if "loan_count" in df.columns:
            df = df.orderBy(F.col("loan_count").desc())

        rows = df.limit(limit).collect()
        return [r.asDict() for r in rows]
    except Exception as e:
        print(f"[Spark ETL] Failed to read aggregates for summary: {e}")
        return []
    finally:
        spark.stop()
