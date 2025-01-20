import logging
from scripts.extract import extract_data
from scripts.transform import clean_data, process_price_data, process_borrower_data, process_fico_data
from scripts.load import load_to_database

logging.basicConfig(
    filename='logs/etl_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def etl_pipeline():
    logging.info("Starting ETL process...")
    
    # Extract Data
    price_data = extract_data("data/Nat_Gas.csv")
    borrower_data = extract_data("data/borrower_data.csv")
    fico_data = extract_data("data/corporateCreditRatingWithFinancialRatios.csv")

    # Initialize variables to avoid referencing before assignment
    cleaned_price_data = None
    cleaned_borrower_data = None
    cleaned_fico_data = None

    # Clean Data
    if price_data is not None:
        cleaned_price_data = clean_data(price_data)
        logging.info("Price data cleaned.")
    else:
        logging.error("Failed to extract price data.")
    
    if borrower_data is not None:
        cleaned_borrower_data = clean_data(borrower_data)
        logging.info("Borrower data cleaned.")
    else:
        logging.error("Failed to extract borrower data.")
    
    if fico_data is not None:
        cleaned_fico_data = clean_data(fico_data)
        logging.info("Credit score data cleaned.")
    else:
        logging.error("Failed to extract FICO data.")

    # Transform Data
    transformed_price_data = None
    transformed_borrower_data = None
    transformed_fico_data = None

    if cleaned_price_data is not None:
        transformed_price_data = process_price_data(cleaned_price_data)
    else:
        logging.error("Price data transformation failed.")
    
    if cleaned_borrower_data is not None:
        transformed_borrower_data = process_borrower_data(cleaned_borrower_data)
    else:
        logging.error("Borrower data transformation failed.")
    
    if cleaned_fico_data is not None:
        transformed_fico_data = process_fico_data(cleaned_fico_data)
    else:
        logging.error("FICO data transformation failed.")

    # Load Data
    if transformed_price_data is not None:
        load_to_database(transformed_price_data, "processed_price_data")
        logging.info("Price data loaded to database.")
    else:
        logging.error("Price data loading skipped due to transformation failure.")

    if transformed_borrower_data is not None:
        load_to_database(transformed_borrower_data, "processed_borrower_data")
        logging.info("Borrower data loaded to database.")
    else:
        logging.error("Borrower data loading skipped due to transformation failure.")
    
    if transformed_fico_data is not None:
        load_to_database(transformed_fico_data, "processed_fico_data")
        logging.info("FICO data loaded to database.")
    else:
        logging.error("FICO data loading skipped due to transformation failure.")
    
    logging.info("ETL process completed successfully.")

if __name__ == "__main__":
    try:
        etl_pipeline()
    except Exception as e:
        logging.error(f"Error in ETL pipeline: {e}")
