import sys
import os
# Add project root directory to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.config import S3_BUCKET
from src.verification.cleaning_verification import run_cleaning_verification_pipeline



def main():
    if len(sys.argv) < 2:
        print("Usage: python src/pipeline_verification_runner.py <video_data_filename>")
        sys.exit(1)

    print(sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))))
    print("S3 Bucket:", {S3_BUCKET})
    video_data_filename = sys.argv[1]

    # Run only the verification pipeline
    run_cleaning_verification_pipeline(video_data_filename)

if __name__ == "__main__":
    main()
