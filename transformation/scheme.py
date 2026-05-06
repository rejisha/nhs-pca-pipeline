"""
    Defines the expected schema (column names + data types) for
    the raw NHS PCA CSV files, and the rules for cleaning them.
"""

from pyspark.sql.types import (
    StructType,    # container for all column definitions
    StructField,   # defines one column: name + type + nullable
    StringType,    # text columns
    IntegerType,   # whole number columns
    DoubleType,    # decimal number columns
    LongType,      # very large whole numbers (SNOMED codes)
)

PCA_RAW_SCHEMA = StructType([
    StructField("YEAR_MONTH",                   StringType(),  True),
    StructField("REGION_NAME",                  StringType(),  True),
    StructField("REGION_CODE",                  StringType(),  True),
    StructField("ICB_NAME",                     StringType(),  True),
    StructField("ICB_CODE",                     StringType(),  True),
    StructField("DISPENSER_ACCOUNT_TYPE",        StringType(),  True),
    StructField("BNF_PRESENTATION_CODE",         StringType(),  True),
    StructField("BNF_PRESENTATION_NAME",         StringType(),  True),
    StructField("SNOMED_CODE",                   StringType(),  True),
    StructField("SUPPLIER_NAME",                 StringType(),  True),
    StructField("UNIT_OF_MEASURE",               StringType(),  True),
    StructField("GENERIC_BNF_EQUIVALENT_CODE",   StringType(),  True),
    StructField("GENERIC_BNF_EQUIVALENT_NAME",   StringType(),  True),
    StructField("BNF_CHEMICAL_SUBSTANCE_CODE",   StringType(),  True),
    StructField("BNF_CHEMICAL_SUBSTANCE",        StringType(),  True),
    StructField("BNF_PARAGRAPH_CODE",            StringType(),  True),
    StructField("BNF_PARAGRAPH",                 StringType(),  True),
    StructField("BNF_SECTION_CODE",              StringType(),  True),
    StructField("BNF_SECTION",                   StringType(),  True),
    StructField("BNF_CHAPTER_CODE",              StringType(),  True),
    StructField("BNF_CHAPTER",                   StringType(),  True),
    StructField("PREP_CLASS",                    StringType(),  True),
    StructField("PRESCRIBED_PREP_CLASS",         StringType(),  True),
    StructField("ITEMS",                         IntegerType(), True),
    StructField("TOTAL_QUANTITY",                DoubleType(),  True),
    StructField("NIC",                           DoubleType(),  True),
    StructField("PHARMACY_ADVANCED_SERVICE",     StringType(),  True),
])

# COLUMNS TO KEEP IN SILVER LAYER
SILVER_COLUMNS = [
    "YEAR_MONTH",
    "REGION_NAME",           
    "ICB_NAME",
    "ICB_CODE",
    "BNF_PRESENTATION_CODE", 
    "BNF_PRESENTATION_NAME", 
    "BNF_CHEMICAL_SUBSTANCE",
    "BNF_CHAPTER",           
    "SNOMED_CODE",
    "ITEMS",
    "TOTAL_QUANTITY",        
    "NIC",                   
]



# COLUMN RENAME MAP
# Maps original column names to cleaned column names.

COLUMN_RENAME_MAP = {
    "YEAR_MONTH":               "year_month",
    "REGION_NAME":              "region_name",           
    "ICB_NAME":                 "icb_name",
    "ICB_CODE":                 "icb_code",
    "BNF_CHAPTER":              "bnf_chapter",           
    "BNF_CHEMICAL_SUBSTANCE":   "bnf_chemical_substance",
    "BNF_PRESENTATION_CODE":    "bnf_presentation_code", 
    "BNF_PRESENTATION_NAME":    "bnf_presentation_name", 
    "SNOMED_CODE":              "snomed_code",
    "ITEMS":                    "items",
    "NIC":                      "nic",                   
    "TOTAL_QUANTITY":           "total_quantity",         
}
# NULL RULES
# Defines how to handle nulls per column.

# "drop"      = drop the entire row if this column is null
#               Use when the row is meaningless without this column
# "fill_zero" = fill null with 0
#               Use for metrics where null means zero (free prescriptions)
# "fill_str"  = fill null with "Unknown"
#               Use for string columns to prevent GROUP BY issues


NULL_RULES = {
    # Critical — row is useless without these
    "bnf_presentation_code":  "drop",
    "icb_code":               "drop",
    "year_month":             "drop",
    "items":                  "drop",

    # Metrics - null means zero
    "nic":                    "fill_zero",
    "total_quantity":         "fill_zero",
    "snomed_code":            "fill_zero",

    # Strings - fill with "Unknown" so GROUP BY works correctly
    "region_name":            "fill_str",
    "bnf_chemical_substance": "fill_str",
    "bnf_presentation_name":  "fill_str",
    "bnf_chapter":            "fill_str",
    "icb_name":               "fill_str",
}


