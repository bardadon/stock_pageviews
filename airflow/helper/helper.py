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
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor

# Load env variables from .env
load_dotenv()

# Grab AIRFLOW_HOME env variable
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

# Add airflow to sys.path
sys.path.append(AIRFLOW_HOME)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/config/ServiceKey_GoogleCloud.json'

####                                        ####
#### Helper Functions For load_pageviews.py ####
####                                        ####

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
    Format of latest link: 2023-06-28T04:00:00.000000000
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
    # Grab date from latest link from BigQuery. If no data is available, return True
    try:
        latest_link_from_bigquery = grab_latest_link_from_bigquery()
        latest_link_from_bigquery = pd.to_datetime(latest_link_from_bigquery)
    except:
        return True

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


####                                        ####
#### Helper Functions For predict_rates.py ####
####                                        ####

def pre_processing() -> pd.DataFrame:
    '''
    Function: pre_processing
    Summary:
        - Connect to bigquery and grab ALL data
        - seperate date column
    Args:
        None
    Returns:
        pageviews_df(dataframe)
    '''
    # Generate dataframe with ALL data from bigquery
    conn = connect_to_bigquery()
    query = "SELECT * FROM pageviews-390416.pageviews.pageviews_ranks"
    pageviews_df = conn.query(query).result().to_dataframe()
    
    # Seperate and drop date column
    pageviews_df['year'] = pd.to_datetime(pageviews_df['date']).dt.year
    pageviews_df['month'] = pd.to_datetime(pageviews_df['date']).dt.month
    pageviews_df['day'] = pd.to_datetime(pageviews_df['date']).dt.day
    pageviews_df['hour'] = pd.to_datetime(pageviews_df['date']).dt.hour
    pageviews_df['minute'] = pd.to_datetime(pageviews_df['date']).dt.minute
    pageviews_df['second'] = pd.to_datetime(pageviews_df['date']).dt.second
    pageviews_df = pageviews_df.drop(['date'], axis=1)

    return pageviews_df

def train_test_samples(x, y, test_size = 0.2):
    number_of_test_rows = int(len(x) * test_size)

    x_train = x.iloc[number_of_test_rows:,:].values
    x_test = x.iloc[:number_of_test_rows, :].values

    y_train = y[number_of_test_rows:].values
    y_test = y[:number_of_test_rows].values
    
    return x_train, y_train, x_test, y_test

def generate_x_y(df, company):
    company_df = df[df.company == company]
    x = company_df.drop(['rate' ,'company'], axis = 1)
    y = company_df.loc[:, 'rate']
    return x, y

def train_model(df, company):
    '''
    Function: train_model
    Summary:
        - Create train and test data from ALL data
        - Train and test model
    Args:
        df - dataframe with ALL data
        company - name of assert
    Returns:
        model(RanomForestRegressor model)
    '''
    # Create train and test data for each company(80% train, 20% test)
    x, y = generate_x_y(df, company)
    x_train, y_train, x_test, y_test = train_test_samples(x, y)

    # Train a RandomForest model
    model = RandomForestRegressor(n_estimators=50, random_state = 365)
    model.fit(x_train, y_train)

    # Print score
    print(f"Model R^2 Score is: {model.score(x_test, y_test)}")

    return model    

def generate_new_data(company, date:pd.DatetimeIndex, pageviews:int) -> pd.DataFrame:
    '''
    Function: generate_new_data
    Summary:
        - Generate a dataframe with the views from the last hour
        - dataframe will have exactly the same format as the train dataframe
        - dataframe date will be one hour after the latest date in bigquery
    Args:
        company - name of assert
        date - latest date from bigquery
        pageviews - latest views from bigquery
    Returns:
        new_data_df(dataframe) 
    '''
    new_data_df = pd.DataFrame()
    date = pd.to_datetime(date)

    new_data_df['pageviews'] = [pageviews]
    new_data_df['year'] = [date.year]
    new_data_df['month'] = [date.month]
    new_data_df['day'] = [date.day]
    new_data_df['hour'] = [date.hour+1]
    new_data_df['minute'] = [date.minute]
    new_data_df['second'] = [date.second]
    
    return new_data_df

def _predict_rates() -> pd.DataFrame:
    '''
    Function predict_rates()
    Summary:
        - Train model and predict new data for each company
        - Generate a dataframe for all next hour predictions
    Args:
        None
    Returns:
        predictions_df(dataframe)
    '''

    # Generate dataframe with ALL data from bigquery
    df = pre_processing()

    # Create dataframe for predictions
    predictions_df = pd.DataFrame()

    # For each company in the list predict the next hour rate
    for company in ['AMZN', 'GOOGL', 'META', 'MSFT', 'AAPL']:
        company_df = df[df["company"] == company]
        
        # Get the latest date from bigquery
        date = grab_latest_link_from_bigquery()

        # generate new data for the next hour
        new_data = generate_new_data(company, date, company_df.views.values[-1])
        print(new_data)

        # Train model and predict new data
        model = train_model(company_df, company)
        prediction = model.predict(new_data)
        print(prediction)

        # Add prediction to dataframe
        predictions_df[company] = prediction

    # Export to csv
    predictions_df.to_csv('/tmp/predictions.csv', index=False)

def _generate_predictions_view():
    '''
    Function: generate_predictions_view
    Summary:
        - Generate a view with the predictions for the next hour
    Args:
        predictions_df(dataframe)
    Returns:
        None
    '''
    predictions_df = pd.read_csv('/tmp/predictions.csv')

    # Connect to bigquery
    conn = connect_to_bigquery()

    # Create dataframe for the view
    predictions_view_df = pd.DataFrame()
    predictions_view_df['current_AMZN'] = si.get_live_price("AMZN")
    predictions_view_df['predicted_AMZN'] = predictions_df['AMZN']
    predictions_view_df['current_GOOGL'] = si.get_live_price("GOOGL")
    predictions_view_df['predicted_GOOGL'] = predictions_df['GOOGL']
    predictions_view_df['current_META'] = si.get_live_price("META")
    predictions_view_df['predicted_META'] = predictions_df['META']
    predictions_view_df['current_MSFT'] = si.get_live_price("MSFT")
    predictions_view_df['predicted_MSFT'] = predictions_df['MSFT']
    predictions_view_df['current_AAPL'] = si.get_live_price("AAPL")
    predictions_view_df['predicted_AAPL'] = predictions_df['AAPL']
    predictions_view_df['date'] = pd.to_datetime(grab_latest_link_from_bigquery())

    # Create view
    table_id = grab_table_id(conn=conn, service_key_path="/opt/airflow/config/ServiceKey_GoogleCloud.json", table_name="predictions")
    table = conn.get_table(table_id)
    conn.delete_table(table_id)
    job = conn.load_table_from_dataframe(predictions_view_df, table_id)
    job.result()

    print("Loaded {} rows into {}.".format(job.output_rows, table_id))



def main():
    test_df = pre_processing()
    x, y = generate_x_y(test_df, 'GOOGL')
    x_train, y_train, x_test, y_test = train_test_samples(x, y)

    test_model = train_model(test_df, 'GOOGL')
    predictions_df = _predict_rates()
    #_generate_predictions_view(predictions_df)

if __name__ == '__main__':
    main()


