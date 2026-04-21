import logging
from ingestion.download_pca import run_download
from ingestion.upload_to_blob import run_upload

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def main():
    logger.info("NHS PCA Pipeline - Ingestion Phase Starting")
    logger.info("Step 1 of 2: Downloading NHS PCA files from NHSBSA portal")

    downloaded_files = run_download(num_months=3)

    if not downloaded_files:
        logger.error("No files downloaded — stopping. Check your internet connection.")
        return

    logger.info(f"Step 2 of 2: Uploading {len(downloaded_files)} files to Azure Blob Storage")
    result = run_upload(downloaded_files)

    # Summary of ingestion
    print("\n" + "=" * 50)
    print("INGESTION COMPLETE (Bronze Layer)")
    print("=" * 50)
    print(f"Downloaded:  {len(downloaded_files)} files")
    print(f"Uploaded:    {len(result['successful'])} files")
    print(f"Failed:      {len(result['failed'])} files")
    if result["failed"]:
        print(f"Failed files: {result['failed']}")
    print("\nNext step: run transformation (Silver layer)")
    print("=" * 50)


if __name__ == "__main__":
    main()