# Import necessary libraries/modules
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

# Assign known values, files and specs to global variables
CSV_PATH = 'largest_banks_data.csv'
LOG_FILE = 'code_log.txt'
DB_NAME = 'Banks.db'
TABLE_NAME = 'Largest_banks'
TABLE_ATTRIBUTES = [
    'Name',
    'MC_USD_Billion',
]

# SOURCE URL
URL = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'

def log_progress(log_file, message):
    """
    Records timestamp and stage of process in a designed log file.
    Returns nothing
    """
    time_format = '%Y-%h-%d-%H:%M:%s'
    now = datetime.now()
    timestamp = now.strftime(time_format)

    with open(log_file, 'a') as file:
        file.write(timestamp + " : " + message + '\n')

def extract(url, table_attributes):
    """
    Extracts data from the given URL. Converts the text data into a pandas df.
    Returns a pandas DF
    """
    # Gets response object from requests
    response = requests.get(url)

    # Parse response.text with HTML PARSER
    soup = BeautifulSoup(response.text, 'html.parser')

    # Create empty DF to receive data
    df = pd.DataFrame(columns=table_attributes)

    # Get the wanted table from soup object and its rows
    table = soup.find('table').find_all('tr')

    # Separate Header and Rows
    table_header = table[0]
    table_rows = table[1:]
    
    for row in table_rows:
        col = row.find_all('td')
        
        name = col[1].find_all('a')[1].contents[0]
        mc = float(col[2].get_text()[:-1])

        data_dict = {
            'Name' : name,
            'MC_USD_Billion' : mc,
        }

        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df, df1], ignore_index=True)
    
    return df

# ACTUAL PIPELINE
log_progress(LOG_FILE, 'PRELIMINARIES COMPLETED. INITIATING ETL PROCESS...')

extracted_data = extract(URL, TABLE_ATTRIBUTES)
print(f"DATAFRAME: \n{extracted_data}")
log_progress(LOG_FILE, 'DATA EXTRACTION COMPLETE, INITIATING TRANSFORMATION PROCESS...')
