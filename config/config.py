# Central place for all config loading (like S3 bucket, folders, etc.)
import os
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")