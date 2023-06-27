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


# Load env variables from .env
load_dotenv()

# Grab AIRFLOW_HOME env variable
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

# Add airflow to sys.path
sys.path.append(AIRFLOW_HOME)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/config/ServiceKey_GoogleCloud.json'

from helper.helper import load_current_rates, is_new_data_is_available, generate_unzip_command, _load_data, _fetch_pageviews, generate_curl_command


# Default Settings
default_args = {
    'start_date': pendulum.now('UTC').add(-3)
}

# Worflow - Grab latest pageviews from  https://dumps.wikimedia.org/other/pageviews
with DAG(dag_id="load_pageviews", schedule_interval = '0 * * * *' , default_args=default_args, catchup=False ) as dag:

    # Task #1 - Check if new link is available
    check_if_new_link_is_available = PythonOperator(
        task_id = 'check_if_new_link_is_available',
        python_callable=is_new_data_is_available
    )

    # Task  #2 - Extract data
    extract_data = BashOperator(
        task_id = 'extract_data',
        bash_command= generate_curl_command()
    )

    # Task #3 - Check if file arrived
    wait_for_file = FileSensor(
        task_id = 'wait_for_file',
        fs_conn_id='gz_file',
        filepath="/tmp/wikipageviews.gz"
    )

    # Task #4 - Unzip file
    unzip_file = BashOperator(
        task_id = 'unzip_file',
        bash_command=generate_unzip_command()
    )

    # Task #5 - fetch view and export as CSV
    fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
    "pagenames": {
    "Google",
    "Amazon",
    "Apple",
    "Microsoft",
    "Facebook"
    }})

    # Task #6 - Load data to BigQuery
    load_pageview_data = PythonOperator(
        task_id = 'load_pageview_data',
        python_callable=_load_data
    )

    # Task #7 - Load current rates to BigQuery
    load_rates_data = PythonOperator(
        task_id = 'load_rates_data',
        python_callable=load_current_rates
    )

    # Task #8 - Create views using DBT scripts
    create_dbt_models = BashOperator(
        task_id = 'create_dbt_models',
        bash_command="cd /opt/airflow/helper/dbt_pageviews && dbt run"
    )

    # Task #9 - Send email when DAG is complete
    send_email_notification = EmailOperator(
        task_id = 'send_email_notification',
        to='bdadon50@gmail.com',
        subject='Stock Prediction Workflow',
        html_content="<h3>Workflow Suceeded. New Data Arrived</h3>"
    )

    # Dependencies
    check_if_new_link_is_available >> extract_data >> wait_for_file >> unzip_file \
    >> fetch_pageviews >> load_pageview_data >> load_rates_data >> create_dbt_models >> send_email_notification