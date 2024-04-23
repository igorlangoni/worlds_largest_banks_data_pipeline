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

def transform(extracted_data):
    """
    This function creates an exchange dictionary. 
    It also creates three new columns on the DF and
    rounds resulting data to 2 decimal places.
    """
    # Create Exchange Dictionary
    exchange_dictionary = {}

    # Creates PD from csv file and converts to dictionary usind pd.to_dict
    df = pd.read_csv('exchange_rate.csv').to_dict('split', index=False)

    # For each entry in df.data we take the key and value
    for currency in df['data']:
        exchange_dictionary[currency[0]] = currency[1]
    
    # Create GBP Column
    extracted_data['MC_GBP_Billion'] = [ np.round(x * exchange_dictionary['GBP'], 2) for x in extracted_data['MC_USD_Billion']]
    
    # Create EUR Column
    extracted_data['MC_EUR_Billion'] = [ np.round(x * exchange_dictionary['EUR'], 2) for x in extracted_data['MC_USD_Billion']]

    # Create INR Column
    extracted_data['MC_INR_Billion'] = [ np.round(x * exchange_dictionary['INR'], 2) for x in extracted_data['MC_USD_Billion']]


    return extracted_data

    # Add columns with extra currencies to our main df




# ACTUAL PIPELINE
log_progress(LOG_FILE, 'PRELIMINARIES COMPLETED. INITIATING ETL PROCESS...')

extracted_data = extract(URL, TABLE_ATTRIBUTES)
print(f"DATAFRAME: \n{extracted_data}")
log_progress(LOG_FILE, 'DATA EXTRACTION COMPLETE. INITIATING TRANSFORMATION PROCESS...')

transformed_data = transform(extracted_data)
print(transformed_data)
log_progress(LOG_FILE, 'DATA TRANSFORMATION COMPLETE. INITIATING LOADING PROCESS...')
