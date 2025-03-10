from pyspark.sql import SparkSession
import os

# AWS Configuration - Ensure your credentials are in environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3_BUCKET = "eltpipelinellmautomation"
RAW_FOLDER = f"s3a://{S3_BUCKET}/youtube-rawdata"

# Initialize Spark Session with S3 support
spark = SparkSession.builder \
    .appName("S3_CSV_Reader_Test") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.2.0,"
            "org.apache.hadoop:hadoop-common:3.2.0,"
            "org.apache.hadoop:hadoop-client:3.2.0") \
    .getOrCreate()

# File to read (Replace with actual filename in S3)
file_name = "INvideos.csv"  # Replace with your actual file
file_path = f"{RAW_FOLDER}/{file_name}"

print(f"📥 Attempting to read file from S3: {file_path}")

try:
    # Read CSV file from S3
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)

    # Show the first 5 rows
    df.show(5)
    
    print("✅ Successfully read the CSV file from S3.")

except Exception as e:
    print(f"❌ Error reading the CSV file: {e}")


