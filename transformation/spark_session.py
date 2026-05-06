
"""
Creates and returns a configured SparkSession.
Every transformation script imports from here.
"""

import os
import sys
import logging

from pyspark.sql import SparkSession

# Fix for Windows — must be set before SparkSession is created
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-21"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

# Add Java and Hadoop to PATH so Spark can find them
os.environ["PATH"] = (
    r"C:\Program Files\Java\jdk-21\bin" + os.pathsep +
    r"C:\hadoop\bin" + os.pathsep + 
    os.environ.get("PATH", "")
)

logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "NHS_Prescribing_Pipeline") -> SparkSession:

    logger.info(f"Initializing SparkSession: {app_name}")
    logger.info(f"Using Python: {sys.executable}")

    spark = (
        SparkSession.builder.appName(app_name) 
     
        # Enables Delta Lake SQL extension
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")

        # Registers Delta Lake as a data source
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        # Downloads Delta Lake JAR automatically from Maven
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") 

        # Forces Spark to use localhost instead of your network IP
        # Prevents Windows Firewall from blocking the connection
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
  
        # 4g = 4 gigabytes for NHS PCA monthly files
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")

        # Remove this line when running on Azure Databricks
        .master("local[*]")

        .getOrCreate()
    )

    # Only show warnings and errors, not every Spark operation
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"SparkSession ready — Spark version: {spark.version}")
    return spark

def stop_spark_session(spark: SparkSession) -> None:
        """
        Stops the SparkSession to free up resources.
        Should be called at the end of each transformation script.
        """
        if spark:
            spark.stop()
            logger.info("SparkSession stopped")