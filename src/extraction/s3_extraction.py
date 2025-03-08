# s3 read functions
import boto3
import pandas as pd
import io
import json
from config.config import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, S3_BUCKET


# Initialize S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

def read_csv_from_s3(key):
    """Read CSV file from S3 and return DataFrame."""
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(response['Body'].read()))

def read_json_from_s3(key):
    """Read JSON file from S3 and return dictionary."""
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(response['Body'].read().decode('utf-8'))