import pandas as pd
import logging

def clean_data(data):
    """Clean the extracted data."""
    try:
        logging.info(f"Raw Data: {data.head()}")
        
        # # Check if the 'date' column exists before proceeding
        # if 'date' in data.columns:
        #     data['date'] = pd.to_datetime(data['date'], errors='coerce')
        # else:
        #     logging.warning("'date' column is missing in the dataset.")

        # Other cleaning steps (you can add more specific checks here)
        # For example, checking for missing values in important columns
        data = data.dropna(axis=0, how='any')  # Remove rows with any missing values

        logging.info(f"Cleaned Data: {data.head()}")
        return data
    except Exception as e:
        logging.error(f"Error cleaning data: {e}")
        return None



# Transform price data for Natural Gas Pricing Model
# Transform price data for Natural Gas Pricing Model
def process_price_data(data):
    """Transform price data for Option Pricing Model."""
    try:
        logging.info(f"Raw Price Data: {data.head()}")
        # data['Date'] = pd.to_datetime(data['Date'], errors='coerce')  # Handling invalid date parsing
        # data = data.set_index('Date')
        logging.info(f"Processed Price Data: {data.head()}")
        logging.info("Natural gas price data processed successfully.")
        return data
    except Exception as e:
        logging.error(f"Error processing price data: {e}")
        return None

# Transform borrower data for Default Prediction Model
def process_borrower_data(data):
    """Transform borrower data for default prediction model."""
    try:
        logging.info(f"Raw Borrower Data: {data.head()}")
        # data['loan_date'] = pd.to_datetime(data['loan_date'], errors='coerce')  # Handling invalid date parsing
        # data['loan_amount'] = data['loan_amount'].fillna(data['loan_amount'].mean())  # Fill missing loan amounts
        # data['default'] = data['default'].map({0: 'No', 1: 'Yes'})
        logging.info(f"Processed Borrower Data: {data.head()}")
        logging.info("Borrower data processed successfully.")
        return data
    except Exception as e:
        logging.error(f"Error processing borrower data: {e}")
        return None

# Process FICO score data for Bucket Rating Model
def process_fico_data(data):
    """Transform Credit score data into buckets."""
    try:
        logging.info(f"Raw Credit Data: {data.head()}")
        # bins = [300, 600, 650, 700, 750, 800, 850]
        # labels = ['Poor', 'Fair', 'Good', 'Very Good', 'Excellent', 'Exceptional']
        # data['fico_bucket'] = pd.cut(data['fico_score'], bins=bins, labels=labels)
        logging.info(f"Processed FICO Data: {data.head()}")
        logging.info("Credit score data processed.")
        return data
    except Exception as e:
        logging.error(f"Error processing Credit score data: {e}")
        return None