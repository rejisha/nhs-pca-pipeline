"""
Read raw NHS PCA CSV files from Bronze layer, clean and validate
the data, and write it to the Silver layer as Delta Lake format.

This is core of silver layer:

The cleaning and validation steps include:
1. Read raw CSV files from Bronze layer.
2. Select only the relevent columns needed for analysis.
3. Rename columns to more descriptive names.
4. Convert data types to appropriate formats (e.g., dates, numeric).
5. Parse and standardize date formats.
6. Handle nulls (drop critical, fill_zero metrics, fill_str strings)
7. Remove duplicate rows
8. Add audit columns (e.g., ingestion timestamp, source file name).
9. Validate data quality (e.g., check for negative values, ensure date ranges are valid).
10. Write the cleaned and validated data to the Silver layer in Delta Lake format by year/month
"""


import logging
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, LongType

from transformation.spark_session import get_spark_session, stop_spark_session
from transformation.scheme import(
    PCA_RAW_SCHEMA,
    SILVER_COLUMNS,
    COLUMN_RENAME_MAP,
    NULL_RULES
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

BRONZE_PATH = Path(__file__).parent.parent / "data" / "raw" 
SILVER_PATH = Path(__file__).parent.parent / "data" / "silver" / "pca"

# Step 1: Read raw CSV files from Bronze layer
def read_raw_data(spark: SparkSession, file_path: Path) -> DataFrame:
    """Read raw NHS PCA CSV files from Bronze layer."""
    
    logger.info(f"Reading raw data from {file_path}")
    
    df = (
        spark.read
        .format("csv")
        .option("header", "true")

        .option("nullValue", "")
        .option("nullValue", "NULL")
        .option("nullValue", "N/A")
        .option("mode", "PERMISSIVE")
        .option("enforceSchema", "false")
        .schema(PCA_RAW_SCHEMA)
        .load(file_path)
    )

    raw_count = df.count()
    logger.info(f"Read {raw_count} rows from Bronze layer")

    return df

# Step 2: Select only the relevant columns needed for analysis
def select_relevant_columns(df: DataFrame) -> DataFrame:
    """Select only the relevant columns needed for analysis."""
    
    logger.info(f"Selecting {len(SILVER_COLUMNS)} required columns for Silver layer")
    # print(SILVER_COLUMNS)
    # Check if all required columns are present in the DataFrame
    missing_columns = [col for col in SILVER_COLUMNS if col not in df.columns]
    
    if missing_columns:
        logger.warning(f"Missing columns in DataFrame: {missing_columns}")
        available_columns = [col for col in SILVER_COLUMNS if col in df.columns]
        # print('###########')
        # print(available_columns)
        logger.info(f"Selecting available columns: {available_columns}")
        return df.select(available_columns)

    # print(df.select(SILVER_COLUMNS))
    return df.select(SILVER_COLUMNS)

# Step 3: Rename columns to more descriptive names (from UPPER_CASE to snake_case)
def rename_columns(df: DataFrame) -> DataFrame:
    """Rename columns to more descriptive names."""
    
    logger.info("Renaming columns to more descriptive names")
    # print(COLUMN_RENAME_MAP)
    # print('#################################')
    # print(df.columns)
    for old_name, new_name in COLUMN_RENAME_MAP.items():
        # print('befor if:', old_name, 'new_name:', new_name)
        if old_name in df.columns:
            # print('old_name:', old_name, 'new_name:', new_name)
            df = df.withColumnRenamed(old_name, new_name)
            # print('after checking if: ', df)
        else:
            logger.warning(f"Column '{old_name}' not found for renaming to '{new_name}'")
    # exit()
    return df

# Step 4: Convert data types to appropriate formats (e.g., dates, numeric)

def convert_data_types(df: DataFrame) -> DataFrame:
    """Convert data types to appropriate formats (e.g., dates, numeric)."""
    
    logger.info("Converting data types to appropriate formats")
    
    df = (
        df
        .withColumn("items", F.col("items").cast(IntegerType()))
        .withColumn("nic", F.col("nic").cast(DoubleType()))
        .withColumn("total_quantity", F.col("total_quantity").cast(DoubleType()))
        .withColumn("snomed_code", F.col("snomed_code").cast(LongType()))

        # Trim white spaces from string columns
        .withColumn("icb_name", F.trim(F.col("icb_name")))
        .withColumn("icb_code", F.trim(F.col("icb_code")))
        .withColumn("bnf_presentation_code", F.trim(F.col("bnf_presentation_code")))
        .withColumn("bnf_presentation_name", F.trim(F.col("bnf_presentation_name")))
        .withColumn("bnf_chemical_substance", F.trim(F.col("bnf_chemical_substance")))
        .withColumn("region_name", F.trim(F.col("region_name")))
        .withColumn("bnf_chapter", F.trim(F.col("bnf_chapter")))
    )

    return df

# Step 5: Parse and standardize date formats
def parse_and_standardize_dates(df: DataFrame) -> DataFrame:
    """Parse and standardize date formats."""
    
    logger.info("Parsing and standardizing date formats (YEAR_MONTH to DATE)")
     
    df = (
        df
        # Extract year: "202501" → 2025
        .withColumn(
            "year",
            F.substring(F.col("year_month"), 1, 4).cast(IntegerType())
        )

        # Extract month: "202501" → 1
        .withColumn(
            "month",
            F.substring(F.col("year_month"), 5, 2).cast(IntegerType())
        )

        # Create proper date: "202501" → 2025-01-01
        .withColumn(
            "prescription_month",
            F.to_date(F.col("year_month"), "yyyyMM")
        )

        # NHS financial year: April to March
        # If month >= 4: FY = "2025/26"
        # If month < 4:  FY = "2024/25"
        .withColumn(
            "financial_year",
            F.when(
                F.col("month") >= 4,
                F.concat(
                    F.col("year").cast("string"),
                    F.lit("/"),
                    (F.col("year") + 1).cast("string").substr(3, 2)
                )
            ).otherwise(
                F.concat(
                    (F.col("year") - 1).cast("string"),
                    F.lit("/"),
                    F.col("year").cast("string").substr(3, 2)
                )
            )
        )
    )

    return df

# Step 6: Handle nulls (drop critical, fill_zero metrics, fill_str strings)
def handle_nulls(df: DataFrame) -> DataFrame:
    """
    Applies null handling rules from schema.py NULL_RULES.

    Three strategies:
    - drop: drop the entire row (critical columns)
    - fill_zero: replace null with 0 (metric columns)
    - fill_str: replace null with "Unknown" (string columns)
    """

    logger.info("Handling nulls based on predefined rules")

    before_count = df.count()
    logger.info(f"Row count before null handling: {before_count}")
    
    # Collect columns that require row dropping
    print('##########################################################')
    print('NULL_RULES:', NULL_RULES)
    drop_columns = [col for col, rule in NULL_RULES.items() if rule == "drop"]
    print('drop_columns:', drop_columns)
    df = df.dropna(subset=drop_columns)
    

    # Collect columns that require filling with zero
    fill_zero_columns = [col for col, rule in NULL_RULES.items() if rule == "fill_zero"]
    if fill_zero_columns:
        df = df.fillna(0, subset=fill_zero_columns)

    # Collect columns that require filling with "Unknown"
    fill_str_columns = [col for col, rule in NULL_RULES.items() if rule == "fill_str"]
    if fill_str_columns:
        df = df.fillna("Unknown", subset=fill_str_columns)


    after_count = df.count()
    dropped_rows = before_count - after_count
    logger.info(f"Dropped {dropped_rows} rows due to nulls in critical columns")

    return df

# Step 7: Remove duplicate rows
def remove_duplicates(df: DataFrame) -> DataFrame:
    """Remove duplicate rows based on all columns."""
    
    before_count = df.count()
    logger.info(f"Removing duplicates. Rows before: {before_count:,}")
    print('########################################################################')
    print(df)
    df = df.dropDuplicates()
    print('passed')

    after_count = df.count()
    removed = before_count - after_count
    logger.info(f"Removed {removed:,} duplicate rows. Remaining: {after_count:,}")

    return df

# Step 8: Add audit columns (e.g., ingestion timestamp, source file name).
def add_audit_columns(df: DataFrame, source_file: str) -> DataFrame:
    """Add audit columns for data lineage and traceability."""
    
    logger.info(f"Adding audit columns. Source: {source_file}")

    df = (
        df
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
    )

    return df

# Step 9: Validate data quality (e.g., check for negative values, ensure date ranges are valid).
def validate_data_quality(df: DataFrame, source_file: str) -> bool:
    """Validate data quality by applying rules and filters."""
    
    logger.info(f"Validating Silver data for: {source_file}")
    passed = True

        # Check 1: Row count
    row_count = df.count()
    if row_count == 0:
        logger.error("VALIDATION FAILED: Zero rows after cleaning")
        return False
    if row_count < 1000:
        logger.warning(f"VALIDATION WARNING: Unusually low row count: {row_count:,}")
    logger.info(f"Check 1 PASSED: {row_count:,} rows")

    # Check 2: No nulls in critical columns
    for col in ["bnf_presentation_code", "icb_code", "year_month", "items"]: 
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            logger.error(f"VALIDATION FAILED: {null_count:,} nulls in '{col}'")
            passed = False
        else:
            logger.info(f"Check 2 PASSED: No nulls in '{col}'")

    # Check 3: No negative costs
    neg_cost = df.filter(F.col("nic") < 0).count()  
    if neg_cost > 0:
        logger.error(f"VALIDATION FAILED: {neg_cost:,} rows with negative nic")
        passed = False
    else:
        logger.info("Check 3 PASSED: All nic values >= 0")

    # Check 4: Valid year range
    current_year = datetime.now().year
    invalid_years = df.filter(
        (F.col("year") < 2021) | (F.col("year") > current_year + 1)
    ).count()
    if invalid_years > 0:
        logger.error(f"VALIDATION FAILED: {invalid_years:,} rows with invalid year")
        passed = False
    else:
        logger.info("Check 4 PASSED: All year values are valid")

    return passed


# Step 10: Write the cleaned and validated data to the Silver layer in Delta Lake format by year/month
def write_silver_delta(df: DataFrame, output_path: str) -> None:
    """
    Writes the cleaned DataFrame to Silver as Delta Lake format,
    partitioned by year and month.
    """
    logger.info(f"Writing Silver Delta Lake to: {output_path}")

    (
        df.write
        .format("delta")
        # Delta Lake format = Parquet + transaction log

        .mode("overwrite")
        # Replace existing data for this partition

        .option("partitionOverwriteMode", "dynamic")
        # Only overwrite partitions present in this DataFrame
        # Leave all other partitions untouched

        .partitionBy("year", "month")
        # Creates: data/silver/pca/year=2025/month=1/part-00000.parquet

        .save(output_path)
    )

    logger.info(f"Silver write complete: {output_path}")



# Main function to run the full Bronze → Silver transformation
def run_silver_transform(bronze_files: list = None) -> bool:
    """
    Main entry point. Runs the full Bronze → Silver transformation.

    """
    spark = get_spark_session("NHS_PCA_Silver_Transform")
    all_success = True

    try:
        # If no specific files given, find all PCA CSVs in Bronze folder
        if bronze_files is None:
            bronze_files = [str(p) for p in BRONZE_PATH.glob("PCA_*.csv")]

        if not bronze_files:
            logger.warning(f"No Bronze files found in {BRONZE_PATH}")
            logger.warning("Run run_ingestion.py first to download NHS data")
            return False

        logger.info(f"Processing {len(bronze_files)} Bronze files")

        for file_path in bronze_files:
            source_file = Path(file_path).name
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing: {source_file}")
            logger.info(f"{'='*50}")

            try:
                # Run all 10 steps in sequence
                df = read_raw_data(spark, file_path)      
                df = select_relevant_columns(df)             
                df = rename_columns(df)                      
                df = convert_data_types(df)                   
                df = parse_and_standardize_dates(df)                 
                df = handle_nulls(df)                        
                df = remove_duplicates(df)                   
                df = add_audit_columns(df, source_file)     

                # Step 9: Validate before writing
                is_valid = validate_data_quality(df, source_file)
                if not is_valid:
                    logger.error(f"Validation failed — skipping write for {source_file}")
                    all_success = False
                    continue

                # Step 10: Write to Silver
                write_silver_delta(df, str(SILVER_PATH))

                logger.info(f"Successfully processed: {source_file}")

            except Exception as e:
                logger.error(f"Failed to process {source_file}: {e}")
                all_success = False
                continue

    finally:
        # Always stop Spark — even if an error occurred
        stop_spark_session(spark)

    return all_success


# Testing
if __name__ == "__main__":
    success = run_silver_transform()
    print(f"\nSilver transformation: {'SUCCESS' if success else 'FAILED'}")
    print(f"Output: {SILVER_PATH}")
    print("\nNext step: set up dbt for the Gold layer")
