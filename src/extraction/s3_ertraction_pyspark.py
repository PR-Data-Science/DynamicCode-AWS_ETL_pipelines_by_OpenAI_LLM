from pyspark.sql import SparkSession
import os
from config.constants import S3_BASE_PATH

def get_spark_session():
    """Initialize and return a PySpark session with S3 support."""
    return SparkSession.builder \
        .appName("S3_PySpark_Operations") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .getOrCreate()

def read_s3_file_pyspark(folder_name, file_name):
    """Reads a CSV file from S3 into a PySpark DataFrame."""
    
    spark = get_spark_session()  # Initialize Spark session

    file_path = f"{S3_BASE_PATH}{folder_name}/{file_name}"  # Construct full S3 path

    print(f"📥 Reading file from S3: {file_path}")

    # Read CSV file from S3
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)

    df.show(5)  # Display first 5 rows
    
    return df

def write_s3_file_pyspark(df, folder_name, file_name):
    """Writes a PySpark DataFrame to S3 as a CSV file."""
    
    spark = get_spark_session()  # Ensure Spark session is available

    file_path = f"{S3_BASE_PATH}{folder_name}/{file_name}"  # Construct full S3 path

    print(f"💾 Saving file to S3: {file_path}")

    # Save DataFrame to S3
    df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(file_path)

    print(f"✅ File saved to S3: {file_path}")
