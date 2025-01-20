import logging
from sqlalchemy import create_engine

def load_to_database(data, table_name, db_url="sqlite:///outputs/etl_output.db"):
    """Load data into a SQLite database."""
    try:
        engine = create_engine(db_url)
        data.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Data loaded into table {table_name} in the database.")
    except Exception as e:
        logging.error(f"Error loading data to database: {e}")
