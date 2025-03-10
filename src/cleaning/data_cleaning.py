from src.utils.openai_utils import call_openai_with_system_user_prompt, clean_openai_code_response
from src.utils.file_utils import save_cleaning_code_to_file
from src.extraction.s3_extraction import read_csv_from_s3
from config.config import S3_BUCKET
from config.constants import RAW_FOLDER, CLEANED_FOLDER
import pandas as pd
import io

# def clean_openai_code_response(response_text):
#     """Remove markdown-like ```python and ``` from LLM code blocks."""

#     if response_text is None:
#         raise ValueError("Received no cleaning code from OpenAI.")
    
#     if response_text.startswith("```"):
#         response_text = response_text.split("\n", 1)[1]
#     if response_text.endswith("```"):
#         response_text = response_text.rsplit("\n", 1)[0]
#     return response_text

def execute_cleaning_code(df, cleaning_code):
    globals_dict = {"pd": pd, "df": df}
    try:
        exec(cleaning_code, globals_dict)
        if 'df' not in globals_dict:
            raise ValueError("Cleaning code did not return df")
        return globals_dict['df']
    except Exception as e:
        raise RuntimeError(f"Error executing cleaning code: {e}")

def save_cleaned_data_to_s3(s3, df, filename):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=f"{CLEANED_FOLDER}{filename}", Body=csv_buffer.getvalue())
    print(f"Saved cleaned data to {CLEANED_FOLDER}{filename}")

def generate_system_prompt():
    system_prompt = """
    You are a highly skilled Data Engineer and Data Quality Expert. 
    Your expertise includes:
    - Cleaning, enriching, and validating structured datasets.
    - Writing optimized, well-documented **Python (pandas) scripts** for ETL pipelines.
    - Ensuring data quality by following industry best practices.

    ### Instructions:
    - You will receive a dataset schema, sample data, and known data quality rules.
    - Your job is to **generate a Python function** named `clean_youtube_data(df)` that:
    1. Cleans and standardizes the dataset.
    2. Handles missing values appropriately.
    3. Ensures timestamps follow the **ISO 8601 format**.
    4. Enforces **non-negative values** for numerical fields.
    5. Normalizes text fields (removing special characters, extra spaces).
    6. Deduplicates records while preserving relevant information.
    7. Drops unnecessary columns that do not contribute to analysis.
    - Return only **fully executable Python code** (no explanations). 
    """
    return system_prompt

def generate_cleaning_prompt(video_data_filename):
    df = read_csv_from_s3(f"{RAW_FOLDER}{video_data_filename}")
    """Generate prompt dynamically based on dataset details."""

    row_count = len(df)  # Get total number of rows
    col_info = "\n".join([f"    - {col} ({dtype})" for col, dtype in df.dtypes.items()])  # Schema details

    cleaning_prompt = f"""
    You are tasked with cleaning data from the file: {video_data_filename}. 
    This data is part of a dynamic ETL pipeline and requires essential data cleaning and validation.

    ### Dataset Overview
    - File Name: {video_data_filename}
    - Total Rows: {row_count}
    - Columns and Data Types:
    {col_info}

    ### Sample Rows (First 10)
    {df.head(10).to_string(index=False)}

    ### Data Summary
    - Summary Statistics:
    {df.describe(include='all').to_string()}

    - Missing Values Per Column:
    {df.isnull().sum().to_string()}

    ### Data Quality Rules
    - ✅ Ensure timestamps (`publish_time`) are in **ISO 8601 format (`YYYY-MM-DDTHH:MM:SSZ`)**.
    - ✅ Numeric columns (`views, likes, dislikes, comment_count`) must be **non-negative**.
    - ✅ Remove **duplicate rows** to maintain uniqueness.
    - ✅ Standardize `tags` column by **removing special characters**.
    - ✅ Ensure `category_id` is **valid and can be mapped to `category_name`**.
    - ✅ Trim **leading/trailing spaces** from `title`, `channel_title`, and `description`.
    - ✅ Handle **missing values**:
        - Fill missing `tags` with `"No Tags"`.
        - Replace missing `description` with an empty string (`""`).

    ### Expected Output
    - Generate a **Python function named `clean_youtube_data(df)`**.
    - The function must use **pandas** for transformations.
    - The function must return the **cleaned DataFrame (`df`)**.
    - **Do not include explanations**, only return the **Python code**.
    """

    return cleaning_prompt  # Return the generated prompt



def run_cleaning_pipeline(video_data_filename, s3):
    df = read_csv_from_s3(f"{RAW_FOLDER}{video_data_filename}")

    system_prompt = generate_system_prompt()
    cleaning_prompt = generate_cleaning_prompt(video_data_filename)

    cleaning_code = call_openai_with_system_user_prompt(system_prompt, cleaning_prompt)
    cleaning_code = clean_openai_code_response(cleaning_code)

    save_cleaning_code_to_file(cleaning_code, video_data_filename)

    cleaned_df = execute_cleaning_code(df, cleaning_code)

    cleaned_filename = video_data_filename.replace('.csv', '_cleaned.csv')
    save_cleaned_data_to_s3(s3, cleaned_df, cleaned_filename)

    print(f"Cleaning completed for {video_data_filename}")
