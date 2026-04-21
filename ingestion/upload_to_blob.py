import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import AzureError, ResourceExistsError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


CONNECTION_STRING = os.getenv("BLOB_CONN_STRING")
CONTAINER_NAME = os.getenv("CONTAINER_NAME", "nhs-pipeline")

def get_blob_service_client() -> BlobServiceClient:

    try:
        client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        print(client)
        logger.info("Azure BlobServiceClient created successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to create BlobServiceClient: {e}")
        raise


def ensure_container_exists(blob_service_client: BlobServiceClient) -> ContainerClient:
 
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    try:
        container_client.create_container()
        logger.info(f"Created container: {CONTAINER_NAME}")
    except ResourceExistsError:
        logger.info(f"Container already exists: {CONTAINER_NAME}")
    except AzureError as e:
        logger.error(f"Failed to create/access container '{CONTAINER_NAME}': {e}")
        raise

    return container_client

def build_blob_path(filename: str) -> str:
   
    stem = Path(filename).stem  # eg. 'PCA_202501'
    year_month = stem[-6:]      # eg. '202501'
    year = year_month[:4]       # eg. '2025'
    month = year_month[4:]      # eg. '01'

    blob_path = f"bronze/pca/year={year}/month={month}/{filename}"
    return blob_path

def upload_file_to_blob(local_path: Path, container_client: ContainerClient, overwrite: bool = False) -> bool:

    filename = local_path.name
    blob_path = build_blob_path(filename)

    logger.info(f"Uploading: {filename} → {blob_path}")

    try:

        blob_client = container_client.get_blob_client(blob_path)
        if not overwrite:
            try:
                blob_client.get_blob_properties()
                logger.info(f"Already in Blob Storage — skipping: {blob_path}")
                return True
            except Exception:
                pass

        with open(local_path, "rb") as data:
            blob_client.upload_blob(
                data,
                overwrite=overwrite,
            )

        properties = blob_client.get_blob_properties()
        size_mb = round(properties.size / (1024 * 1024), 2)
        logger.info(f"Uploaded successfully: {blob_path} ({size_mb} MB)")
        return True

    except AzureError as e:
        logger.error(f"Azure error uploading {filename}: {e}")
        return False

    except FileNotFoundError:
        logger.error(f"Local file not found: {local_path}")
        return False

    except Exception as e:
        logger.error(f"Unexpected error uploading {filename}: {e}")
        return False


def run_upload(local_files: list[Path]) -> dict:

    if not local_files:
        logger.warning("No files to upload — list is empty")
        return {"successful": [], "failed": []}

    logger.info("=" * 50)
    logger.info(f"NHS PCA Upload — starting ({len(local_files)} files)")
    logger.info("=" * 50)

    # Create Azure connection once
    blob_service_client = get_blob_service_client()
    container_client = ensure_container_exists(blob_service_client)

    successful = []
    failed = []

    for local_path in local_files:
        success = upload_file_to_blob(local_path, container_client)
        if success:
            successful.append(local_path.name)
        else:
            failed.append(local_path.name)

    logger.info("=" * 50)
    logger.info(f"Upload complete: {len(successful)} succeeded, {len(failed)} failed")
    if failed:
        logger.warning(f"Failed files: {failed}")
    logger.info("=" * 50)

    return {"successful": successful, "failed": failed}


# if __name__ == "__main__":
    
#     raw_dir = Path(__file__).parent.parent / "data" / "raw"

#     if not raw_dir.exists():
#         print(f"No data/raw folder found. Run download_pca.py first.")
#     else:
#         local_files = list(raw_dir.glob("PCA_*.csv"))
#         print(f"Found {len(local_files)} files to upload: {[f.name for f in local_files]}")
#         result = run_upload(local_files)
#         print(f"\nResult: {result}")