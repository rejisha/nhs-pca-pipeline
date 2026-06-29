"""
Bronze (Ingestion) -> Silver transformation.
This script reads the raw CSV files downloaded in the Bronze layer, performs cleaning and transformation, 
and writes out Parquet files in a partitioned folder structure for the Silver layer.
The transformation logic is defined in transformation/clean_pca.py, which contains the specific rules for cleaning
"""

import logging
from transformation.clean import run_silver_transform, list_bronze_files_from_blob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

import logging
from transformation.clean import run_silver_transform, list_bronze_files_from_blob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

if __name__ == "__main__":
    print("NHS Prescribing Pipeline - Silver Layer Transformation")
    print("=" * 55)

    bronze_files = list_bronze_files_from_blob()

    if not bronze_files:
        print("\nNo Bronze files found in Azure Blob Storage (bronze/pca/)")
        print("Run 'python run_ingestion.py' first to download and upload NHS PCA data")
    else:
        print(f"Found {len(bronze_files)} Bronze files to process:")
        for f in bronze_files:
            print(f"  {f}")
        print()

        success = run_silver_transform(bronze_files=bronze_files)

        print("\n" + "=" * 55)
        print(f"Result: {'SUCCESS' if success else 'FAILED'}")
        print(f"Silver data written to: silver/pca/ (Azure Blob)")
        print("\nNext step: set up dbt for the Gold layer")
        print("=" * 55)