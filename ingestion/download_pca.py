import os
import requests
import logging
from pathlib import Path
from datetime import datetime, timedelta 
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# NHS_BASE_URL = (
#     "https://opendata.nhsbsa.net/dataset/"
#     "prescription-cost-analysis-pca-monthly-data/"
#     "resource/{resource_id}/download/PCA_{year_month}.csv"
# )

NHS_API_URL = (
    "https://opendata.nhsbsa.net/api/3/action/package_show"
    "?id=prescription-cost-analysis-pca-monthly-data"
)    

RAW_DATA_DIR = Path(__file__).parent.parent/"data"/"raw"
# RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

def get_available_months(num_months: int = 3) -> list[str]:
    
    months = []
    start_date = datetime.now().replace(day=1) - timedelta(days=60)

    for i in range(3):
        month = start_date.replace(day=1) # Calculate the month by going back i months from start_date
        target = (start_date - timedelta(days=i * 31)).replace(day=1) # Go back i months — subtract i*30 days then pin to 1st of month
        months.append(target.strftime("%Y%m"))  # format as YYYYMM

    logger.info(f"Target months to download: {months}")
    return months

def build_download_url(year_month: str) -> str:

    response = requests.get(NHS_API_URL)
    response.raise_for_status()
    
    resources = response.json()["result"]["resources"]
    
    search_term = f"pca_{year_month.lower()}.csv"  # e.g. "pca_202601.csv"
    
    for resource in resources:
        url = resource.get("url", "")
        name = resource.get("name", "")
        
        # check both the URL and the resource name
        if search_term in url.lower() or year_month in name:
            return url
    
    raise ValueError(f"No resource found for {year_month}")


def download_pca_file(year_month: str, output_dir: Path) -> Path | None: 

    output_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"PCA_{year_month}.csv"
    local_file_path = output_dir/file_name

    if local_file_path.exists():
        logger.info(f"File already downloaded: {local_file_path} - skipping download.")
        return local_file_path
    
    url = build_download_url(year_month)
    logger.info(f"Downloading PCA data for {year_month} from {url}")
    
    try:
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()  # Raise an error for bad status codes

        chunk_size = 8192  
        total_bytes = 0

        with open(local_file_path, "wb") as f:  
            for chunk in response.iter_content(chunk_size=chunk_size):
                
                if chunk:
                    f.write(chunk)
                    total_bytes += len(chunk)

        size_mb = round(total_bytes / (1024 * 1024), 2)
        logger.info(f"Downloaded: {file_name} ({size_mb} MB)")
        return local_file_path

    except requests.exceptions.HTTPError as e:
        # HTTP errors: 404 (not found), 403 (forbidden), 500 (server error)
        logger.error(f"HTTP error downloading {file_name}: {e}")
        return None

    except requests.exceptions.ConnectionError as e:
        # Network errors: no internet, DNS failure, connection refused
        logger.error(f"Connection error downloading {file_name}: {e}")
        return None

    except requests.exceptions.Timeout:
        # File took too long to download
        logger.error(f"Timeout downloading {file_name}")
        return None

    except Exception as e:
        # Catch-all for any other unexpected errors
        logger.error(f"Unexpected error downloading {file_name}: {e}")
        return None


def validate_downloaded_file(file_path: Path) -> bool:

    if not file_path.exists():
        logger.error(f"Validation failed — file does not exist: {file_path}")
        return False
    
    file_size = file_path.stat().st_size
    if file_size < 1000:  # less than 1KB is suspicious for a PCA file
        logger.error(f"Validation failed — file too small ({file_size} bytes): {file_path}")
        return False

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            first_line = f.readline().strip()

        # PCA files contain these columns — check at least one is present
        expected_columns = ["BNF_CODE", "ITEMS", "NIC", "ACTUAL_COST"]
        has_expected = any(col in first_line.upper() for col in expected_columns)

        if not has_expected:
            logger.warning(
                f"Validation warning — unexpected header in {file_path.name}: "
                f"'{first_line[:100]}'"
            )

        logger.info(f"Validation passed: {file_path.name} ({file_size:,} bytes)")
        return True

    except Exception as e:
        logger.error(f"Validation error reading {file_path}: {e}")
        return False
    
def run_download(num_months: int = 3) -> list[Path]:
    
    logger.info("=" * 50)
    logger.info("NHS PCA Download — starting")
    logger.info(f"Downloading {num_months} months of data")
    logger.info("=" * 50)

    months = get_available_months(num_months)

    successful_downloads = []

    for year_month in months:
        local_path = download_pca_file(year_month, RAW_DATA_DIR)

        if local_path is None:
            logger.warning(f"Skipping {year_month} — download failed")
            continue

        if validate_downloaded_file(local_path):
            successful_downloads.append(local_path)
        else:
            logger.warning(f"Skipping {year_month} — validation failed")
            local_path.unlink(missing_ok=True)


    logger.info("=" * 50)
    logger.info(f"Download complete: {len(successful_downloads)}/{len(months)} files successful")
    for path in successful_downloads:
        logger.info(f"  Ready for upload: {path.name}")
    logger.info("=" * 50)

    return successful_downloads

# if __name__ == "__main__":
#     downloaded_files = run_download(num_months=3)
#     print(f"\nResult: {len(downloaded_files)} files ready for upload")
#     for f in downloaded_files:
#         print(f"  {f}")