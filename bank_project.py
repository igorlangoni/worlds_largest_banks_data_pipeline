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
URL = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'

def log_progress(log_file, message):
    """
    Records timestamp and stage of process in a designed log file.
    Returns nothing
    """
    time_format = '%Y-%h-%d-%H:%m:%s'
    now = datetime.now()
    timestamp = now.strftime(time_format)

    with open(log_file, 'a') as file:
        file.write(timestamp + ', ' + message + '\n')


