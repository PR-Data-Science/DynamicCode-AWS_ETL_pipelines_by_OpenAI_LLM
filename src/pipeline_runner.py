# Main entry point to run the full ETL pipeline
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.cleaning.data_cleaning import run_cleaning_pipeline
from src.extraction.s3_extraction import s3
from src.verification.cleaning_verification import run_cleaning_verification_pipeline


def main():
    if len(sys.argv) < 2:
        print("Usage: python src/pipeline_runner.py <video_data_filename> [--verify]")
        sys.exit(1)

    video_data_filename = sys.argv[1]
    verify = "--verify" in sys.argv  # Check if verification is requested

    # Step 1: Run Cleaning Pipeline
    run_cleaning_pipeline(video_data_filename, s3)

    # Step 2: Run Verification (if requested)
    if verify:
        run_cleaning_verification_pipeline(video_data_filename)

if __name__ == "__main__":
    main()
