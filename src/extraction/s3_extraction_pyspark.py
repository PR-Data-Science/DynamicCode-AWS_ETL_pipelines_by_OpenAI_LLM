from pyspark.sql import SparkSession
import os

# AWS Configuration - Ensure your credentials are in environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3_BUCKET = "eltpipelinellmautomation"
RAW_FOLDER = f"s3a://{S3_BUCKET}/youtube-rawdata"
CLEAN_FOLDER = f"s3a://{S3_BUCKET}/youtube-cleandata"
S3_PATH = f"s3a://{S3_BUCKET}/"


def get_spark_session():
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
    return spark


def read_s3_file_pyspark(folder_name, file_name):
    """Reads a CSV file from S3 into a PySpark DataFrame."""
    
    spark = get_spark_session()  # Initialize Spark session

    # Use `s3a://` instead of `s3://`
    #file_name = "INvideos.csv"  # Replace with your actual file
    file_path = f"{S3_PATH}/{folder_name}/{file_name}"

    print(f"📥 Reading file from S3: {file_path}")

    try:
        # Read CSV file from S3
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(file_path)

        df.show(5)  # Display first 5 rows
        print("✅ Successfully read the CSV file from S3.")

        return df

    except Exception as e:
        print(f"❌ Error reading the CSV file: {e}")
        return None


def write_s3_file_pyspark(df, folder_name, file_name):
    """Writes a PySpark DataFrame to S3 as a CSV file."""
    
    spark = get_spark_session()  # Ensure Spark session is available

    file_path = f"{S3_PATH}/{folder_name}/{file_name}"

    print(f"💾 Saving file to S3: {file_path}")

    try:
        # Save DataFrame to S3
        df.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(file_path)

        print(f"✅ File saved to S3: {file_path}")

    except Exception as e:
        print(f"❌ Error saving file to S3: {e}")




