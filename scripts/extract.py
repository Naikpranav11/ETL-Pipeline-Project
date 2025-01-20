import pandas as pd
import logging

# Set up logging
logging.basicConfig(
    filename='logs/etl_pipeline.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_data(file_path):
    """Extract data from a CSV file."""
    try:
        data = pd.read_csv(file_path)
        logging.info(f"Data extracted from {file_path}. Shape: {data.shape}")
        return data
    except Exception as e:
        logging.error(f"Error extracting data from {file_path}: {e}")
        return None
