from bs4 import BeautifulSoup
import requests
import pandas as pd
import logging


# Set the logging format using dictConfig
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_html(url):
    """Get the HTML content of a webpage."""

    try:
        source = requests.get(url)
        logging.info(f"Request to {url} was successful")

    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred: {e}")

    return source.text


def format_col_name(col_name):
    """Format the column name.
    
    args:
        col_name: a BeautifulSoup object of the column name

    returns:
        col_name: a string of the column name
    """
    try:
        # Replace the newline character with a space and remove any leading or trailing whitespaces
        col_name = str(col_name.text).replace("\n", " ").strip() 

        # Split the column name by the space character and return the first element in lowercase
        col_name = col_name.split(' ')[0].lower()

    except Exception as e:
        logging.error(f"An error occurred: {e}")

    return col_name

def extract_columns(table):
    """Extract the column names of a table.
    
    args:
        table: a BeautifulSoup object of the table

    returns:
        table_cols: a list of column names
    """
    try:
        table_cols = [format_col_name(col) for col in table.thead.find_all('th')]

        logging.info(f"Column names {table_cols} were extracted successfully")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

    return table_cols


def remove_cols(col_names):
    """Format the column names."""
    try:
        tags = ['form', 'next', 'more']

        for tag in tags:
            if tag in col_names:
                col_names.remove(tag)

        # Add the 'previous_position' columnname
        col_names.insert(1, 'previous_position')

    except Exception as e:
        logging.error(f"An error occurred: {e}")

    return col_names


def extract_rows(table):
    """Extract the rows of a table which contains the league table info.
    
    args:
        table: a BeautifulSoup object of the table

    returns:
        table_rows: a list of BeautifulSoup objects of the rows
    """
    try: 
        # Extract the rows of the table
        table_rows = [row for row in table.tbody.select('tr:not(.league-table__expandable.expandable)')]

        logging.info(f"Rows were extracted successfully")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

    return table_rows   


def extract_row_data(row, col_names):
    """Extract the data of a row.
    
    args:
        row: a BeautifulSoup object of the row
        col_names: a list of column names

    returns:
        data: a dictionary of the row data
    """ 
    try:
        # Create a dictionary to store the row data with the keys as the column names
        row_data = {col: '' for col in col_names}

        # Extract the data of a row
        row_data_list = [list(filter(bool, val.text.strip().replace("\n", " ").split(" "))) for val in row.find_all('td')]
        
        # Add the data to the dictionary
        row_data[col_names[0]] = row_data_list[0][0]
        row_data[col_names[1]] = row_data_list[0][3]
        row_data[col_names[2]] = row_data_list[1][0]
        row_data[col_names[3]] = row_data_list[2][0]
        row_data[col_names[4]] = row_data_list[3][0]
        row_data[col_names[5]] = row_data_list[4][0]
        row_data[col_names[6]] = row_data_list[5][0]
        row_data[col_names[7]] = row_data_list[6][0]
        row_data[col_names[8]] = row_data_list[7][0]
        row_data[col_names[9]] = row_data_list[8][0]
        row_data[col_names[10]] = row_data_list[9][0]

        data = {key: int(value) if value.isdigit() or value[1:].isdigit() else value for key, value in row_data.items()}

        logging.info(f"Row data {data} was extracted successfully")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

    return data

def scrape_data_callable(url, save_path):
    """Scrape the data of a webpage.

    args:
        url: a string of the URL

    returns:
        data: a list of dictionaries of the data
    """
    try:
        data =  []

        html = get_html(url)
        soup = BeautifulSoup(html, 'html.parser')

        league_table = soup.find('table')

        # Extract the column names and rows of the table
        col_names  = remove_cols(extract_columns(table=league_table))
        table_rows = extract_rows(table=league_table)

        # Extract the data of the rows
        for row in table_rows:
            row_data = extract_row_data(row, col_names)
            data.append(row_data)

        # Create a DataFrame from the data
        df = pd.DataFrame(data)

        # Save the DataFrame to a CSV file
        df.to_csv(save_path, index=False)

        logging.info(f"Data was scraped successfully")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        
    return df



if __name__ == '__main__':

    url  = 'https://www.premierleague.com/tables'

    df = scrape_data_callable(url)