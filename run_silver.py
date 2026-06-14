"""
Bronze → Silver transformation.
This script reads the raw CSV files downloaded in the Bronze layer, performs cleaning and transformation, 
and writes out Parquet files in a partitioned folder structure for the Silver layer.
The transformation logic is defined in transformation/clean_pca.py, which contains the specific rules for cleaning
"""

import logging
from transformation.clean import run_silver_transform
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

if __name__ == "__main__":
    print("NHS Prescribing Pipeline - Silver Layer Transformation")
    print("=" * 55)

    bronze_path = Path("data/raw")
    if not bronze_path.exists() or not list(bronze_path.glob("PCA_*.csv")):
        print("\nNo Bronze files found in data/raw/")
        print("Run 'python ingestion.py' first to download NHS PCA data")
    else:
        files = list(bronze_path.glob("PCA_*.csv"))
        print(f"Found {len(files)} Bronze files to process:")
        for f in files:
            print(f"  {f.name}")
        print()

        success = run_silver_transform()

        print("\n" + "=" * 55)
        print(f"Result: {'SUCCESS' if success else 'FAILED'}")
        print(f"Silver data written to: data/silver/pca/")
        print("\nFolder structure created:")
        print("  data/silver/pca/year=2025/month=1/  ← partition")
        print("  data/silver/pca/year=2025/month=2/  ← partition")
        print("\nNext step: set up dbt for the Gold layer")
        print("=" * 55)