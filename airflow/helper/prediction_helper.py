import pandas as pd
import datetime
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
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

# Load env variables from .env
load_dotenv()

# Grab AIRFLOW_HOME env variable
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

# Add airflow to sys.path
sys.path.append(AIRFLOW_HOME)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/config/ServiceKey_GoogleCloud.json'

from airflow.helper.helper import *

def pre_processing():
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
    company_df = df[df.company == company]
    x = company_df.drop(['rate' ,'company'], axis = 1)
    y = company_df.loc[:, 'rate']
    test_size = 0.2
    number_of_test_rows = int(len(x) * test_size)

    x_train = x.iloc[number_of_test_rows:,:].values
    x_test = x.iloc[:number_of_test_rows, :].values

    y_train = y[number_of_test_rows:].values
    y_test = y[:number_of_test_rows].values

    assert len(x_train) + len(x_test) == len(x)
    assert len(y_train) + len(y_test) == len(y)

    # Train a RandomForest model
    model = RandomForestRegressor(n_estimators=50)
    model.fit(x_train, y_train)

    # Print score
    print(f"Model R^2 Score is: {model.score(x_test, y_test)}")

    return model    

def generate_new_data(company, date:pd.DatetimeIndex, pageviews:int):
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

def predict_rates():
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

    predictions_df = pd.DataFrame()
    df = pre_processing()
    for company in ['AMZN', 'GOOGL', 'META', 'MSFT', 'AAPL']:
        company_df = df[df["company"] == company]
        
        # Get the latest date from bigquery
        date = grab_latest_link_from_bigquery()

        # generate new data for the next hour
        new_data = generate_new_data(company, date, company_df.views.values[-1])
        print(new_data)

        # Train model and predict new data
        model = train_model(company_df, company)
        model.predict(new_data)
        predictions_df = predictions_df._append({'company': company, 'date': pd.to_datetime(date) + datetime.timedelta(hours=1), 'rate': model.predict(new_data)[0]}, ignore_index=True)

    return predictions_df


def main():
    df = predict_rates()
    print(df)


if __name__ == '__main__':
    main()