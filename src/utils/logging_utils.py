import logging
import os
from datetime import datetime

def setup_logging(log_type="cleaning"):
    """
    Setup logging configuration for execution logs.
    
    Args:
        log_type (str): Type of log (e.g., "cleaning", "verification").
    
    Returns:
        str: Path to the log file.
    """
    
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)

    # Generate log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"logs/{log_type}_execution_logs_{timestamp}.log"

    # Configure logging
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.info(f"📌 Logging initialized for {log_type} at {log_filename}")
    
    return log_filename
