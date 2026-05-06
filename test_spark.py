import os
import sys


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

print(f"Using Python: {sys.executable}")

os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-21"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestSpark") \
    .master("local[1]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Spark version:", spark.version)

data = [("Amoxicillin", 1000, 250.50),
        ("Atorvastatin", 5000, 1200.00),
        ("Metformin", 8000, 800.75)]

df = spark.createDataFrame(data, ["drug_name", "items", "actual_cost"])
df.show()

print("PySpark is working correctly!")
spark.stop()