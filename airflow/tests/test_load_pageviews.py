import os
from dotenv import load_dotenv
import sys
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task, dag

import datetime
import requests
from bs4 import BeautifulSoup
import pytz
import pandas as pd
from google.cloud import bigquery

# Load env variables from .env
load_dotenv()

# Grab AIRFLOW_HOME env variable
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

# Add airflow to sys.path
sys.path.append(AIRFLOW_HOME)

from airflow.helper.helper import get_latest_url, connect_to_bigquery, grab_table_id, today_date_from_latest_link, grab_current_rates, generate_rates_dataframe

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/projects/stock_pageviews/airflow/config/ServiceKey_GoogleCloud.json'

def test_latest_url_AddDate():
    '''
    base_url = https://dumps.wikimedia.org/other/pageviews/
    After adding date: https://dumps.wikimedia.org/other/pageviews/2023/2023-06/
    '''
    latest_url = get_latest_url()
    expected_url = 'https://dumps.wikimedia.org/other/pageviews/2023/2023-06/'
    assert latest_url == expected_url

def test_latest_url_ResponseCode200():
    '''
    test_latest_url_ResponseCode200: Test if the response code is 200
    '''
    latest_url = get_latest_url()
    response = requests.get(latest_url)
    assert response.status_code == 200

def test_timezoneSetToGMT_0():
    '''
    test_timezoneSetToGMT_0: Test if the timezone is set to GMT 0(currently it's GMT +3)
    '''
    tz = pytz.timezone('GMT')

    israel_hour = datetime.datetime.today().hour
    GMT_0_hour = datetime.datetime.now(tz).hour

    assert israel_hour -3 == GMT_0_hour

def test_connect_to_bigquery_IsConnected():
    '''
    test_connect_to_bigquery_IsConnected: Test if the connection to bigquery is established
    '''
    conn = connect_to_bigquery()
    assert conn is not None

def test_table_id():
    '''
    test_table_id: Test if the table id is correct
    '''
    conn = connect_to_bigquery()
    table_id = grab_table_id(conn, table_name='pageviews', service_key_path='/projects/stock_pageviews/airflow/config/ServiceKey_GoogleCloud.json')
    assert table_id == 'pageviews-390416.pageviews.pageviews'



def test_today_date_from_latest_link():
    date = today_date_from_latest_link()
    assert type(date) == pd.Timestamp

def test_grab_current_rates():

    quotes = ['META', 'AMZN', 'GOOGL', 'MSFT', 'AAPL']
    df = grab_current_rates()

    for quote in quotes:
        assert quote in df.company.values
    

def test_generate_rates_dataframe():
    df = generate_rates_dataframe()

    assert df['company'].dtype == 'object'
    assert df['rate'].dtype == 'float64'
    assert df['date'].dtype == 'datetime64[ns]'