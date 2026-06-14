"""
Upload local Silver Delta Lake files to Azure Blob Storage.
"""

import logging
from pathlib import Path
from azure.core.exceptions import AzureError

from ingestion.upload_bronze_to_blob import (
    get_blob_service_client,
    ensure_container_exists
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

SILVER_LOCAL_PATH = Path(__file__).parent.parent / "data" / "silver" / "pca"


def upload_silver_folder(overwrite: bool = False) -> dict:
    """
    Walk every file under data/silver/pca/ and upload to Blob,
    preserving the folder structure.

    Local:  data/silver/pca/year=2025/month=1/part-00000.parquet
    Blob:   silver/pca/year=2025/month=1/part-00000.parquet
    """

    if not SILVER_LOCAL_PATH.exists():
        logger.error(f"Silver path not found: {SILVER_LOCAL_PATH}")
        logger.error("Run run_silver.py first to generate Silver data")
        return {"successful": [], "failed": []}

    # rglob("*") walks all files recursively — parquet + delta log files
    all_files = [f for f in SILVER_LOCAL_PATH.rglob("*") if f.is_file()]

    if not all_files:
        logger.warning("No files found in Silver path")
        return {"successful": [], "failed": []}

    logger.info(f"Found {len(all_files)} files to upload")

    # Reuse existing connection functions — no duplication
    blob_service_client = get_blob_service_client()
    container_client    = ensure_container_exists(blob_service_client)

    successful = []
    failed     = []

    for local_file in all_files:
        relative_path = local_file.relative_to(
            SILVER_LOCAL_PATH.parent.parent  # strips up to /data/
        )
        blob_path = str(relative_path).replace("\\", "/")  # Windows fix

        try:
            blob_client = container_client.get_blob_client(blob_path)

            # Skip if already exists and overwrite is False
            if not overwrite:
                try:
                    blob_client.get_blob_properties()
                    logger.info(f"Already exists — skipping: {blob_path}")
                    successful.append(blob_path)
                    continue
                except Exception:
                    pass  # Does not exist yet — upload it

            with open(local_file, "rb") as data:
                blob_client.upload_blob(data, overwrite=overwrite)

            size_kb = round(blob_client.get_blob_properties().size / 1024, 1)
            logger.info(f"Uploaded: {blob_path} ({size_kb} KB)")
            successful.append(blob_path)

        except AzureError as e:
            logger.error(f"Azure error: {local_file.name}: {e}")
            failed.append(blob_path)
        except Exception as e:
            logger.error(f"Unexpected error: {local_file.name}: {e}")
            failed.append(blob_path)

    logger.info("=" * 50)
    logger.info(f"Silver upload: {len(successful)} succeeded, {len(failed)} failed")
    logger.info("=" * 50)

    return {"successful": successful, "failed": failed}


if __name__ == "__main__":
    result = upload_silver_folder(overwrite=False)
    print(f"\nUploaded:  {len(result['successful'])} files")
    print(f"Failed:    {len(result['failed'])} files")
    print("\nNext step: run load_silver_to_sql.py")