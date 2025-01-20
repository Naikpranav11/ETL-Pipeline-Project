# ETL Data Engineering Project

This project demonstrates an ETL (Extract, Transform, Load) pipeline using Python. It extracts data from multiple sources, transforms the data to fit specific business models, and loads it into a database for further analysis.

## Table of Contents

- [Project Overview](#project-overview)
- [Technologies Used](#technologies-used)
- [File Structure](#file-structure)
- [Setup Instructions](#setup-instructions)
- [ETL Pipeline Workflow](#etl-pipeline-workflow)
- [Running the ETL Pipeline](#running-the-etl-pipeline)
- [Logging](#logging)
- [Error Handling](#error-handling)
- [Optional: Airflow Integration](#optional-airflow-integration)

## Project Overview

This ETL project handles three key data models:
1. **Natural Gas Pricing Model** - Transforms and processes historical natural gas prices for pricing analysis.
2. **Borrower Default Prediction Model** - Transforms borrower-related data to predict the likelihood of loan defaults.
3. **FICO Score Bucket Rating Model** - Transforms credit score data into predefined buckets for categorization.

The project is designed to extract raw data from CSV files, clean and transform the data, and then load it into a database for further use or analysis.

## Technologies Used

- Python 3.x
- Pandas (for data manipulation)
- Logging (for tracking and debugging the ETL pipeline)
- SQLite (or other relational databases)
- Apache Airflow (optional, for scheduling and managing the pipeline)

## File Structure
/your-project-folder
├── data/
│   ├── Nat_Gas.csv
│   ├── borrower_data.csv
│   └── fico_scores.csv
├── logs/
├── outputs/
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── dags/
│   └── etl_workflow.py  # New Airflow DAG file
├── main_etl.py
├── requirements.txt
└── etl_pipeline.log


## Setup Instructions

1. Clone the repository to your local machine:
   ```bash
   git clone <repository-url>
   cd ETL_Project

    pip install -r requirements.txt

## ETL Pipeline Workflow
The ETL pipeline follows these steps:

1. Extract: Data is extracted from the specified CSV files using the extract_data function.
2. Transform: The extracted data is cleaned and transformed to match the required format using the following functions:
    - clean_data: Cleans and preprocesses raw data (e.g., handling missing values).
    - process_price_data: Transforms the natural gas price data.
    - process_borrower_data: Transforms borrower-related data.
    - process_fico_data: Transforms FICO score data into credit score buckets.
3. Load: Transformed data is loaded into a database (SQLite by default, or any other supported DB) using the load_to_database function.

## Data Models
- Natural Gas Pricing Model: Transforms the natural gas pricing data to process it for pricing models.
- Borrower Default Prediction: Transforms borrower-related data by cleaning missing values and categorizing default status.
- FICO Score Bucket Rating Model: Categorizes FICO score data into predefined credit score buckets.

## Running the ETL Pipeline
python main_etl.py


### Key Sections in the README:
- **Project Overview**: Provides a brief about the project and the models being used.
- **Technologies Used**: Lists the libraries and tools used in the project.
- **File Structure**: Describes the folder structure and the purpose of each file.
- **Setup Instructions**: Guides on setting up the project environment.
- **ETL Pipeline Workflow**: Explains the flow of the ETL process.
- **Running the ETL Pipeline**: Instructions to run the pipeline.
- **Logging and Error Handling**: Details on how logging and errors are managed.
- **Optional Airflow Integration**: How to schedule the pipeline with Apache Airflow (optional).

Feel free to adapt the content based on your exact requirements!
