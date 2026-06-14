"""
Read Silver Delta files from Azure Blob Storage and load into
Azure SQL as silver.prescriptions_clean, ready for dbt Gold layer.
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import pyodbc
import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import pyarrow.parquet as pq
import pyarrow as pa

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Azure Blob 
CONNECTION_STRING = os.getenv("BLOB_CONN_STRING")
CONTAINER_NAME    = os.getenv("CONTAINER_NAME", "nhs-pipeline")
SILVER_BLOB_PREFIX = "silver/pca/"

# Azure SQL 
SQL_SERVER   = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")


def get_sql_connection() -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str)


def create_silver_schema(conn: pyodbc.Connection) -> None:
    """Create silver schema in Azure SQL if it does not exist."""
    logger.info("Ensuring silver schema exists...")
    conn.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver') "
        "EXEC('CREATE SCHEMA silver')"
    )
    conn.commit()
    logger.info("Silver schema ready.")


def read_parquet_files_from_blob() -> pd.DataFrame:
    """
    Download all parquet files from silver/pca/ in Blob Storage
    and combine them into a single pandas DataFrame.
    """
    logger.info(f"Connecting to Blob Storage, reading from: {SILVER_BLOB_PREFIX}")

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client    = blob_service_client.get_container_client(CONTAINER_NAME)

    # List all blobs under silver/pca/ — filter to only .parquet files
    # (skip _delta_log JSON files)
    blobs = [
        b.name for b in container_client.list_blobs(name_starts_with=SILVER_BLOB_PREFIX)
        if b.name.endswith(".parquet")
    ]

    if not blobs:
        raise FileNotFoundError(
            f"No parquet files found under '{SILVER_BLOB_PREFIX}' in container '{CONTAINER_NAME}'.\n"
            "Run upload_silver_to_blob.py first."
        )

    logger.info(f"Found {len(blobs)} parquet files in Blob Storage")

    # Read each parquet file into a pandas DataFrame and combine
    frames = []
    for blob_name in blobs:
        logger.info(f"Reading: {blob_name}")
        blob_client = container_client.get_blob_client(blob_name)
        blob_bytes  = blob_client.download_blob().readall()
        table       = pq.read_table(BytesIO(blob_bytes))
        frames.append(table.to_pandas())

    df = pd.concat(frames, ignore_index=True)
    logger.info(f"Combined DataFrame: {len(df):,} rows, {len(df.columns)} columns")
    logger.info(f"Columns: {list(df.columns)}")
    return df


def write_to_azure_sql(df: pd.DataFrame, conn: pyodbc.Connection) -> None:
    """
    Write the DataFrame to Azure SQL as silver.prescriptions_clean.
    Drops and recreates the table on each run (clean load).
    """
    logger.info("Dropping existing silver.prescriptions_clean if it exists...")
    conn.execute(
        "IF OBJECT_ID('silver.prescriptions_clean', 'U') IS NOT NULL "
        "DROP TABLE silver.prescriptions_clean"
    )
    conn.commit()

    # Insert rows in batches of 5000 to avoid timeout
    batch_size  = 5000
    total_rows  = len(df)
    columns     = list(df.columns)
    col_names   = ", ".join(columns)
    placeholders = ", ".join(["?" for _ in columns])

    # Create table dynamically from DataFrame dtypes
    # Map pandas dtypes to SQL Server types
    dtype_map = {
        "int64":   "BIGINT",
        "int32":   "INT",
        "float64": "FLOAT",
        "float32": "FLOAT",
        "object":  "NVARCHAR(500)",
        "bool":    "BIT",
        "datetime64[ns]": "DATETIME2",
    }

    col_definitions = []
    for col in columns:
        dtype     = str(df[col].dtype)
        sql_type  = dtype_map.get(dtype, "NVARCHAR(500)")
        col_definitions.append(f"[{col}] {sql_type}")

    create_sql = (
        f"CREATE TABLE silver.prescriptions_clean "
        f"({', '.join(col_definitions)})"
    )

    logger.info("Creating silver.prescriptions_clean table...")
    conn.execute(create_sql)
    conn.commit()

    # Insert in batches
    logger.info(f"Inserting {total_rows:,} rows in batches of {batch_size:,}...")
    insert_sql = (
        f"INSERT INTO silver.prescriptions_clean ({col_names}) "
        f"VALUES ({placeholders})"
    )

    cursor = conn.cursor()
    cursor.fast_executemany = True  # speeds up bulk inserts significantly

    for i in range(0, total_rows, batch_size):
        batch  = df.iloc[i : i + batch_size]
        values = [tuple(row) for row in batch.itertuples(index=False, name=None)]
        cursor.executemany(insert_sql, values)
        conn.commit()
        logger.info(f"Inserted rows {i:,} – {min(i + batch_size, total_rows):,} of {total_rows:,}")

    logger.info("✅ silver.prescriptions_clean loaded successfully")


def main():
    logger.info("=" * 55)
    logger.info("Silver Blob → Azure SQL Loader")
    logger.info("=" * 55)

    # Step 1 - read parquet files from Blob
    df = read_parquet_files_from_blob()

    # Step 2 - connect to Azure SQL
    logger.info("Connecting to Azure SQL...")
    conn = get_sql_connection()

    try:
        # Step 3 - ensure silver schema exists
        create_silver_schema(conn)

        # Step 4 - write to Azure SQL
        write_to_azure_sql(df, conn)

        logger.info("=" * 55)
        logger.info("Loader complete.")
        logger.info("Next step: cd gold && dbt run")
        logger.info("=" * 55)

    except Exception as e:
        logger.error(f"Loader failed: {e}")
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()