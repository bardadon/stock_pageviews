from urllib import request
import datetime
import requests
from bs4 import BeautifulSoup
import pytz
from google.cloud import bigquery
import os
import sys
from dotenv import load_dotenv
import pandas as pd
import json
from yahoo_fin import stock_info as si
from airflow.exceptions import AirflowException


# Load env variables from .env
load_dotenv()

# Grab AIRFLOW_HOME env variable
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

# Add airflow to sys.path
sys.path.append(AIRFLOW_HOME)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/config/ServiceKey_GoogleCloud.json'


def get_latest_url(base_url = 'https://dumps.wikimedia.org/other/pageviews/'):
    '''
    Function: get_latest_url
    Summary: Get the latest url for the pageviews
    Args:
        base_url (str): Base url for the pageviews
    Returns:
        latest_url (str): Latest url for the pageviews
    Example of latest url: 
    https://dumps.wikimedia.org/other/pageviews/2021/2021-06/
    '''
    # Set timezone to GMT
    tz = pytz.timezone('GMT')

    # Grab current year and month and generate latest url
    year = str(datetime.datetime.now(tz).year)
    month = '0' + str(datetime.datetime.now(tz).month)
    latest_url = f'{base_url}{(year)}/{(year)}-{month}/'
    return latest_url

def get_latest_link():
    '''
    Function: get_latest_link
    Summary: Get the latest link for the pageviews
    Args:
        latest_url (str): Latest url for the pageviews
    Returns:
        latest_link (str): Latest link for the pageviews
    Example of latest link:
    pageviews-20210619-170000.gz
    '''
    # Grab latest url and parse it using BeautifulSoup
    latest_url = get_latest_url()
    response = requests.get(latest_url)
    soup = BeautifulSoup(response.text, "html.parser")

    return [link.text for link in soup.find_all('a') if 'pageviews' in str(link)][-1]


def generate_curl_command():
    '''
    Function: generate_curl_command
    Summary: Generate curl command to download the latest pageviews
    Args:
        None
    Returns:
        curl_command (str): Curl command to download the latest pageviews
    Notes:
        The curl command will be used in the BashOperator
        Example: curl -o /tmp/wikipageviews.gz https://dumps.wikimedia.org/other/pageviews/2021/2021-06/projectviews-20210619-170000.gz
    '''
    # Generate the latest_link
    latest_url = get_latest_url()
    latest_link = get_latest_link()

    # Return curl command
    curl_command = f'curl -o /tmp/wikipageviews.gz {latest_url}{latest_link}'
    return curl_command

def generate_unzip_command():
    return 'gunzip --force /tmp/wikipageviews.gz'


def _fetch_pageviews(pagenames):
    '''
    Function: _fetch_pageviews
    Summary: Fetch pageviews for a given list of pagenames and export as csv
    Args:
        pagenames (list): List of pagenames
    Returns:
        None
    Notes:
        Example of csv file:
        Google, 100
        Amazon, 200
        Apple, 300
    '''
    def export_as_csv(result):
        latest_link = get_latest_link()
        with open(f"/tmp/{latest_link}.csv", 'w') as write_file:
            write_file.write(str(result))
            write_file.close()

    # Load pagenames into a dict
    result = dict.fromkeys(pagenames, 0)

    # Iterate through each line and extract the pageviews for each pagenames
    with open(f"/tmp/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts
        f.close()
    
    # Print result and export as csv
    print(result)
    export_as_csv(result)

def connect_to_bigquery():
    '''
    Function: connect_to_bigquery
    Summary: Connect to BigQuery
    Args:
        None
    Returns:
        conn (obj): BigQuery connection object
    '''
    conn = bigquery.Client()
    return conn

def grab_table_id(conn, table_name, service_key_path='/opt/airflow/config/ServiceKey_GoogleCloud.json'):
    '''
    Function: grab_table_id
    Summary: Grab table id
    Args:
        conn (obj): BigQuery connection object
    Returns:
        table_id (str): Table id
    Template for table id:
    ProjectName.Datasetname.Tablename
    '''
    # Grab project name from service account json file
    project = conn.from_service_account_json(service_key_path).project
    
    # Generate and return table id
    dataset = conn.get_dataset('pageviews')
    table_id = f'{project}.{dataset.dataset_id}.{table_name}'
    return table_id


def today_date_from_latest_link():
    '''
    Function: today_date_from_latest_link
    Summary: Get the latest link available on https://dumps.wikimedia.org/other/pageviews/
    Args:
        None
    Returns:
        date (obj): Today's date
    '''
    latest_link = get_latest_link()
    latest_link = latest_link.replace("pageviews-", "")[:-3]
    date = pd.to_datetime(latest_link, format='%Y%m%d-%H%M%S')
    return date

def _load_data():
    '''
    Function: _load_data
    Summary: 
        - Convert csv file with pageviews to pandas dataframe
        - Generate a dataframe for the pageviews
        - Load the dataframe to BigQuery
    Args:
        None
    Returns:
        None
    '''

    # Grab latest link
    latest_link = get_latest_link()

    # Read csv file
    with open(f"/tmp/{latest_link}.csv", 'r') as read_file:
        # convert string - {'Apple': 0, 'Microsoft': 0, 'Facebook': 0, 'Google': 0, 'Amazon': 0} to dict
        new_string = ''
        for i in read_file.read():
            if i == "'":
                i = i.replace("'", '"')
            new_string += i # Generating a new string with double quotes
        df = json.loads(new_string) # Converting string to dict
        read_file.close()

    # Connect to BigQuery and grab table id
    conn = connect_to_bigquery()
    table_id = grab_table_id(conn, table_name='pageviews')

    date = today_date_from_latest_link()

    # Generate a Pandas dataframe from the dict
    df = pd.DataFrame(df.items(), columns=["company", "views"])
    df['views'] = df['views'].astype('int64')
    df['date'] = date

    # Generate job config. Specify schema and write disposition
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("company", bigquery.enums.SqlTypeNames.STRING, mode="REQUIRED"),
            bigquery.SchemaField("views", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"),
            bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATETIME, mode="REQUIRED"),
        ],
        write_disposition="WRITE_APPEND",
    )

    # Load data into BigQuery
    job = conn.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print("Loaded {} rows into {}.".format(job.output_rows, table_id))

def grab_latest_link_from_bigquery():
    '''
    Function: grab_latest_link_from_bigquery
    Summary: Grab latest link from BigQuery
    Args:
        None
    Returns:
        latest_link (str): Latest link
    '''
    conn = connect_to_bigquery()
    table_id = grab_table_id(conn, table_name='pageviews')
    query = f"""
    SELECT max(date) as date
    FROM `{table_id}`
    """
    query_job = conn.query(query)
    results = query_job.result().to_dataframe()
    results = results['date'].values[0]

    return results

def is_new_data_is_available():
    '''
    Function: check_if_new_data_is_available
    Summary: 
        - Check if new data is available by comparing the latest link from BigQuery and the latest link from Wikipedia
        - If the latest link from Wikipedia is newer than the latest link from BigQuery, return True
        - Else, return False
    Args:
        None
    Returns:
        True or False (bool): True if new data is available, False otherwise
    '''
    # Grab date from latest link from BigQuery
    latest_link_from_bigquery = grab_latest_link_from_bigquery()
    latest_link_from_bigquery = pd.to_datetime(latest_link_from_bigquery)
    

    # Grab date from latest link from Wikipedia
    latest_link_from_wikipedia = get_latest_link()
    latest_link_from_wikipedia = latest_link_from_wikipedia.replace("pageviews-", "")[:-3]
    latest_link_from_wikipedia = pd.to_datetime(latest_link_from_wikipedia, format='%Y%m%d-%H%M%S')

    print(f"Latest link from Wikipedia: {latest_link_from_wikipedia}")
    print(f"Latest link from BigQuery: {latest_link_from_bigquery}")

    # Compare dates
    if latest_link_from_wikipedia > latest_link_from_bigquery:
        return True
    else:
        print("Data is up to date.")
        raise AirflowException("Data is up to date.")

def grab_current_rates():
    '''
    Function: grab_current_rates
    Summary:
        - Grab current rates for several companies using yahoo_fin
        - Generate a Pandas dataframe
    Args:
        None
    Returns:
        df (obj): Pandas dataframe  
    '''
    quotes = ['META', 'AMZN', 'GOOGL', 'MSFT', 'AAPL']
    rates = {}
    
    # Grab rates
    for quote in quotes:
        rates[quote] = si.get_live_price(quote)

    df = pd.DataFrame(rates.items(), columns=['company', 'rate'])
    return df

def generate_rates_dataframe():
    '''
    Function: generate_rates_dataframe
    Summary: Generate rates dataframe
    Args:
        None
    Returns:
        df (obj): Pandas dataframe
    '''
    df = grab_current_rates()
    df['rate'] = df['rate'].astype("float64")

    current_date = today_date_from_latest_link()
    df['date'] = current_date
    return df

def load_current_rates():
    '''
    Function: load_current_rates
    Summary: 
        - Generate rates dataframe
        - Load rates dataframe into BigQuery
    Args:
        None
    Returns:
        None
    '''

    df = generate_rates_dataframe()
    conn = connect_to_bigquery()
    table_id = grab_table_id(conn, table_name='rates')

    # Generate job config. Specify schema and write disposition
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("company", bigquery.enums.SqlTypeNames.STRING, mode="REQUIRED"),
            bigquery.SchemaField("rate", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"),
            bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATETIME, mode="REQUIRED"),
        ],
        write_disposition="WRITE_APPEND",
    )

    # Load data into BigQuery
    job = conn.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print("Loaded {} rows into {}.".format(job.output_rows, table_id))


def extract_from_bigquery():
    conn = connect_to_bigquery()
    table_id = grab_table_id()
    # extract rates/pageview view from bigquery



def main():
    #latest_url = get_latest_url()
    #latest_link = get_latest_link(latest_url)
    #actual_command = generate_curl_command()
    #results = grab_latest_link_from_bigquery()
    #load_current_rates()

    print(is_new_data_is_available())



if __name__ == '__main__':
    main()


