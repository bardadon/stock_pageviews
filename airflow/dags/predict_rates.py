from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.email import EmailOperator
from airflow.decorators import task, dag

import pendulum
import os
from dotenv import load_dotenv
import sys
import json
import pandas as pd

# Load env variables from .env
load_dotenv()

# Grab AIRFLOW_HOME env variable
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

# Add airflow to sys.path
sys.path.append(AIRFLOW_HOME)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/config/ServiceKey_GoogleCloud.json'

from helper.helper import _predict_rates, _generate_predictions_view

# Default Settings
default_args = {
    'start_date': pendulum.now('UTC').add(-3)
}


with DAG(dag_id='predict_rates', catchup=False, schedule_interval='0 * * * *', default_args=default_args) as dag:
    
    # Task #1 - predict rates
    @task
    def predict_rates():
        _predict_rates()        

    # Task #2 - Generate a predictions view in BigQuery
    @task
    def generate_predictions_view():
        _generate_predictions_view()

    # Dependencies
    predict_rates() >> generate_predictions_view()
    