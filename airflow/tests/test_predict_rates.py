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
from airflow.exceptions import AirflowException
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor

# Load env variables from .env
load_dotenv()

# Grab AIRFLOW_HOME env variable
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

# Add airflow to sys.path
sys.path.append(AIRFLOW_HOME)


from airflow.helper.helper import predict_rates, generate_new_data, grab_latest_link_from_bigquery, train_model, pre_processing, train_test_samples, generate_x_y

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/projects/stock_pageviews/airflow/config/ServiceKey_GoogleCloud.json'


def test_pre_processing_GrabbedDataframe():
    test_df = pre_processing()
    assert isinstance(test_df, pd.DataFrame)

def test_pre_processing_CorrectColumns():
    test_df = pre_processing()
    columns = test_df.columns.to_list()
    assert columns == ['company', 'rate', 'views', 'year', 'month', 'day', 'hour', 'minute',
       'second']
    
def test_train_test_samples_TestingSizes():
    test_df = pre_processing()

    x, y = generate_x_y(test_df, 'GOOGL')

    x_train, y_train, x_test, y_test = train_test_samples(x, y)
    
    assert len(x) == len(test_df[test_df['company'] == 'GOOGL'])
    assert len(x_train) + len(x_test) == len(x)
    assert len(y_train) + len(y_test) == len(y)
    assert len(y.shape) == 1

    test_df = test_df.drop("company", axis = 1)
    assert x.shape[1] + len(y.shape) == test_df.shape[1]

def test_train_model_ModelIsRandomForest():
    test_df = pre_processing()
    x, y = generate_x_y(test_df, 'GOOGL')
    x_train, y_train, x_test, y_test = train_test_samples(x, y)

    test_model = train_model(test_df, 'GOOGL')

    assert isinstance(test_model, RandomForestRegressor)
    assert test_model.get_params().get("n_estimators") == 50
    assert test_model.get_params().get("random_state") == 365


    

def test_predict_rates_RateForEachCompany():

    test_df = pre_processing()
    x, y = generate_x_y(test_df, 'GOOGL')
    x_train, y_train, x_test, y_test = train_test_samples(x, y)
    test_model = train_model(test_df, 'GOOGL')

    def predict_rates_mock():
        # Create dataframe for predictions
        predictions_df = pd.DataFrame()

        # For each company in the list predict the next hour rate
        for company in ['GOOGL']:
            company_df = test_df[test_df["company"] == company]
            
            # Get the latest date from bigquery
            date = "2023-06-28T04:00:00.000000000"

            # generate new data for the next hour
            new_data = generate_new_data(company, date, company_df.views.values[-1])
            print(new_data)

            # Train model and predict new data
            model = train_model(company_df, company)
            prediction = model.predict(new_data)
            print(prediction)

            # Add prediction to dataframe
            predictions_df[company] = prediction
        

        return predictions_df

    predictions_df = predict_rates_mock()
    assert isinstance(predictions_df, pd.DataFrame)
    assert len(predictions_df.columns) == 1
    assert len(predictions_df) == 1
    assert predictions_df.columns.to_list() == ['GOOGL']
    assert predictions_df['GOOGL'].values[0] > 0
    