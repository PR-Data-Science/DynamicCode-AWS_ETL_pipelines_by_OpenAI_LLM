import os
from src.utils.openai_utils import call_openai_with_system_user_prompt, clean_openai_code_response
from src.utils.file_utils import save_cleaning_code_to_file
from src.utils.logging_utils import setup_logging
from src.extraction.s3_extraction import read_csv_from_s3
from config.config import S3_BUCKET
from config.constants import RAW_FOLDER, CLEANED_FOLDER
import pandas as pd
import io
import logging
from datetime import datetime


def execute_cleaning_code(df, cleaning_code):
    """Executes cleaning function and logs validation messages."""
    
    try:
        globals_dict = {"pd": pd, "df": df.copy(), "logging": logging}
        exec(cleaning_code, globals_dict)

        if "clean_youtube_data" not in globals_dict:
            raise ValueError("Error: The executed code did not define clean_youtube_data")
        cleaned_df = globals_dict["clean_youtube_data"](globals_dict["df"])

        # Ensure df is updated in globals_dict
        globals_dict["df"] = cleaned_df

        return globals_dict["df"]
    except Exception as e:
        logging.error(f"❌ Error executing cleaning code: {e}")
        return df  # Return original df in case of failure

def save_cleaned_data_to_s3(s3, df, filename):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=f"{CLEANED_FOLDER}{filename}", Body=csv_buffer.getvalue())
    print(f"Saved cleaned data to {CLEANED_FOLDER}{filename}")
    logging.info(f"✅ Cleaned data saved to {CLEANED_FOLDER}{filename}")

def generate_system_prompt():
    """Generate system-level prompt for OpenAI."""
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

def generate_cleaning_prompt(video_data_filename, df):
    """Generate cleaning prompt dynamically and log issues before cleaning."""

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

     ### **Logging Requirements**
    - ✅ **DO NOT use `print()`**. Instead, use Python's `logging` module.
    - ✅ **Explicitly include `import logging` at the top** of your function.
    - ✅ **DO NOT use `logging.basicConfig()` inside the function**.
    - ✅ **Use `logger = logging.getLogger(__name__)` instead** to ensure logging is configured externally.
    - ✅ **Log all validation messages before and after cleaning**.


    ### **Cleaning Steps & Validation**
    1️⃣ **Before Cleaning:** Count issues before applying transformations:
       - Count timestamps that are **not in ISO 8601 format (`YYYY-MM-DDTHH:MM:SS.000Z`)**.
       - Count columns (`views, likes, dislikes, comment_count`) where a **negative-value** appears.
       - Count string columns (`title`, `channel_title`, `description`) that contain **leading/trailing spaces**.
       - Count rows where **`category_id` are missing or empty**
       - Count rows where **`tags` are missing or empty**.
       - Count rows where **`description` is missing**.
       - Count rows where **`tags` contain special characters**.
       - Count **duplicate rows**.
    
    2️⃣ **Perform Cleaning:**
       - ✅ Convert timestamps **ONLY if they do not match the correct ISO 8601 format (`YYYY-MM-DDTHH:MM:SS.000Z`)**.
       - ✅ Use regex `r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.000Z$'` to check if `publish_time` is correctly formatted.
       - ✅ Apply transformation **only where needed** using:
         ```python
         iso_format = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.000Z$'
         mask = ~df['publish_time'].astype(str).str.match(iso_format)
         df.loc[mask, 'publish_time'] = pd.to_datetime(df.loc[mask, 'publish_time'], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
         ```
       - ✅ Ensure Numeric columns (`views, likes, dislikes, comment_count`) must be **non-negative**.
       - ✅ **Trim leading/trailing spaces from all string columns.**
       - ✅ **Fill missing `category_id` with `1000`.**
       - ✅ **Remove special characters from `tags`, keeping only alphanumeric and spaces.**
       - ✅ **Fill missing `tags` with `"No Tags"`.**
       - ✅ **Fill missing `description` with an empty string `""`.**
       - ✅ **Remove duplicate rows to maintain uniqueness.**
    
    3️⃣ **After Cleaning:** Recount and log:
       - Log the number of **issues fixed** for each cleaning step.
       - If any issues remain, log a warning message.


    ### Expected Output
    - Generate a **Python function named `clean_youtube_data(df)`**.
    - The function must use **pandas** for transformations.
    - **Explicitly include `import logging`** at the top of the function.
    - **Use `log_file` for logging, not a hardcoded filename.**
    - The function must return the cleaned DataFrame (`df`):
    - **Do not include explanations**, only return the **Python code**.
    """

    return cleaning_prompt  # Return the generated prompt



def run_cleaning_pipeline(video_data_filename, s3):
    """Runs the cleaning pipeline with logging enabled."""
    
    # Initialize logging and get log file name
    log_file = setup_logging(log_type="cleaning")

    # Read raw data
    df = read_csv_from_s3(f"{RAW_FOLDER}{video_data_filename}")
    
    logging.info(f"📥 Loaded raw data from S3: {RAW_FOLDER}{video_data_filename}")

    # Define the path for the existing cleaning code
    cleaning_code_filename = video_data_filename.replace('.csv', '_cleaning_code.py')
    cleaning_code_path = os.path.join("src/llm_generated_codes", cleaning_code_filename)

    if os.path.exists(cleaning_code_path):
        # ✅ If the cleaning code already exists, use it
        logging.info(f"✅ Using existing cleaning code: {cleaning_code_path}")

        with open(cleaning_code_path, "r", encoding="utf-8") as file:
            cleaning_code = file.read()
    else:
        logging.info(f"⚠️ Cleaning code not found. Generating new code for {video_data_filename}.")
        # Generate system and cleaning prompts
        system_prompt = generate_system_prompt()
        cleaning_prompt = generate_cleaning_prompt(video_data_filename, df)

        # Get cleaning code from OpenAI
        cleaning_code = call_openai_with_system_user_prompt(system_prompt, cleaning_prompt)
        cleaning_code = clean_openai_code_response(cleaning_code)


        # Save cleaning code
        save_cleaning_code_to_file(cleaning_code, video_data_filename)
        logging.info(f"📥 saved cleaned code to a file : {video_data_filename}_cleaning_code.py")

    # Execute cleaning
    cleaned_df = execute_cleaning_code(df, cleaning_code)

    # Check if the cleaning actually changed the DataFrame
    if df.equals(cleaned_df):
        print("⚠️ No changes detected in the cleaned DataFrame!")
        logging.warning("⚠️ Cleaning function did not modify the DataFrame.")
    else:
        print("✅ Cleaned DataFrame has been updated.")
        logging.info("✅ Cleaning applied successfully, DataFrame has been updated.")

    # Save cleaned data to S3
    cleaned_filename = video_data_filename.replace('.csv', '_cleaned.csv')
    save_cleaned_data_to_s3(s3, cleaned_df, cleaned_filename)

    logging.info(f"✅ Cleaning completed for {video_data_filename}")
