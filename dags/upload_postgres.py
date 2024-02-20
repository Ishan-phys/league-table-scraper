import logging
import pandas as pd
from sqlalchemy import create_engine


# Set the logging format using dictConfig
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def connect_to_db(db_name: str, db_user: str, db_password: str, db_host: str, db_port: str) -> create_engine:
    """Connect to the database

    args:
        db_name: str: Name of the database
        db_user: str: Username to connect to the database
        db_password: str: Password to connect to the database
        db_host: str: Host of the database
        db_port: str: Port of the database

    returns:
        create_engine: Database engine to connect to the database
    """
    try:
        # Create the database engine
        connection_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        db_engine = create_engine(connection_url)
        logging.info(f'Connected to {db_name} database')

    except Exception as e:
        logging.error(f'An error occurred: {e}')

    return db_engine


def extract_and_upload(data_filepath: str, table_name: str, db_engine):
    """Extract the CSV file from the gzipped file and upload it to the database.

    args:
        data_filepath: str: File path of the gzipped file
        table_name: str: Name of the table to upload the data to
        db_engine: create_engine: Database engine to upload the data

    returns:
        None
    """
    try:
        # Use pandas to read the gzipped CSV and save the 'data.csv' file to the output folder
        df = pd.read_csv(data_filepath)

        df.to_sql(table_name, db_engine, if_exists='append', index=False)
        
        logging.info(f'Uploaded {data_filepath} to {table_name} table')
    
    except Exception as e:
        logging.error(f'An error occurred: {e}')

    return None


def ingest_callable(db_user, db_pass, db_host, db_port, db_name, table_name, data_filepath):
    """Ingest the data into the database.

    Args:
        db_user: str: Username to connect to the database
        db_pass: str: Password to connect to the database
        db_host: str: Host of the database
        db_port: str: Port of the database
        db_name: str: Name of the database
        table_name: str: Name of the table to upload the data to
        data_filepath: str: File path

    Returns:
        None
    """
    # Connect to the database and get the database engine
    db_engine = connect_to_db(db_name, db_user, db_pass, db_host, db_port)

    # Extract the CSV file from the gzipped file
    extract_and_upload(data_filepath, table_name=table_name, db_engine=db_engine)

    return None