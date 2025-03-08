from pyspark.sql import SparkSession
import os
from config.config import S3_BUCKET
from config.constants import CLEANED_FOLDER, CLEANED_FOLDER_NAME
from src.extraction.s3_ertraction_pyspark import read_s3_file_pyspark
from src.utils.openai_utils import call_openai_with_system_user_prompt
from src.utils.file_utils import save_cleaning_code_to_file

# Initialize Spark session
spark = SparkSession.builder.appName("CleaningVerification").getOrCreate()

def read_cleaned_file_pyspark(file_path):
    """Reads the cleaned file from S3 using PySpark and returns a DataFrame."""
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df.show(5)  # Display first 5 rows
    return df

def generate_pyspark_verification_prompt(video_data_filename, df):
    """Generate a PySpark verification prompt based on the cleaned dataset."""
    
    row_count = df.count()
    col_info = "\n".join([f"    - {col} ({dtype})" for col, dtype in df.dtypes])

    pyspark_verification_prompt = f"""
    You are verifying the cleaning process for the file: {video_data_filename}.
    This dataset was cleaned using an automated ETL pipeline.

    ### Cleaned Dataset Overview
    - File Name: {video_data_filename}
    - Total Rows: {row_count}
    - Columns and Data Types:
    {col_info}

    ### Data Cleaning Rules That Were Applied:
    - ✅ Timestamps (`publish_time`) must be in **ISO 8601 format (`YYYY-MM-DDTHH:MM:SSZ`)**.
    - ✅ Numeric columns (`views, likes, dislikes, comment_count`) must be **non-negative**.
    - ✅ Duplicate rows were removed.
    - ✅ `tags` column was **normalized (no special characters)**.
    - ✅ `category_id` must be **valid and mapped to `category_name`**.
    - ✅ Strings (`title`, `channel_title`, `description`) must have **no leading/trailing spaces**.
    - ✅ Missing `tags` should be replaced with `"No Tags"`.
    - ✅ Missing `description` should be replaced with an empty string (`""`).

    ### Expected Output
    - Generate a **PySpark script** to verify if all these transformations were successfully applied.
    - For each column, print statements such as:
        - `"✅ Timestamp format is correct"` or `"❌ Timestamps have incorrect formats"`
        - `"✅ No negative values found"` or `"❌ Some numeric values are negative"`
    - The script must use **PySpark DataFrame API** (not pandas).
    - **Return only the PySpark code**, no explanations.
    """

    return pyspark_verification_prompt

def generate_pyspark_verification_code(video_data_filename, df):
    """Calls OpenAI to generate PySpark code for data validation."""
    
    system_prompt = """
    You are a highly skilled Data Engineer with expertise in PySpark and large-scale data validation.
    You specialize in writing PySpark code to:
    - Verify data cleaning completeness.
    - Identify any missing transformations.
    - Check for data quality issues.

    You will be given:
    - A cleaned dataset schema and sample records.
    - The original data cleaning rules.

    Your task:
    - Generate PySpark code to validate whether all cleaning rules were correctly applied.
    - The PySpark code should output PRINT statements indicating whether each column meets the expected cleaning criteria.
    - Return only **fully executable PySpark code** (no explanations).
    """

    # Generate the user prompt dynamically
    verification_prompt = generate_pyspark_verification_prompt(video_data_filename, df)

    # Call OpenAI
    pyspark_code = call_openai_with_system_user_prompt(system_prompt, verification_prompt)

    return pyspark_code

def execute_pyspark_code(pyspark_code, spark, df):
    """Executes the generated PySpark validation code."""
    globals_dict = {"spark": spark, "df": df}
    exec(pyspark_code, globals_dict)

def run_cleaning_verification_pipeline(video_data_filename):
    """Runs the validation check on the cleaned file using PySpark.""" 

    # Read the cleaned file from S3 using our generic function
    df = read_s3_file_pyspark(CLEANED_FOLDER_NAME, video_data_filename.replace('.csv', '_cleaned.csv'))

    pyspark_code = generate_pyspark_verification_code(video_data_filename, df)

    # Save verification code for auditing
    save_cleaning_code_to_file(pyspark_code, video_data_filename.replace('.csv', '_verification.txt'))

    # Execute the generated PySpark code
    execute_pyspark_code(pyspark_code, spark, df)

    print(f"\n✅ PySpark validation completed for {video_data_filename}.")
