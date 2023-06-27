#!/bin/bash

### Clean up Environment ###

if [ "$1" == "--clean-up" ]; then

    # Remove airflow folder
    rm -r $(pwd)/airflow

    # Stop and remove all containers
    docker stop $(docker ps -a -q)
    docker rm $(docker ps -a -q)

    # Remove all containers, volumes and images related to the environment
    docker-compose down --volumes --rmi all

    # Remove all process listening to port 8080
    lsof -i tcp:8080 | grep root | awk '{print $2}' | xargs kill -9
fi

### Install Airflow locally ###

# Set Airflow's home
mkdir $(pwd)/airflow
export AIRFLOW_HOME=$(pwd)/airflow

# Set airflow and python version 
AIRFLOW_VERSION=2.5.0
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

# pipe install airflow based on the constaints file
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

### Deploy Airflow using docker-compose ###

# Move to airflow folder
cd airflow

# fetch the file docker-compose.yaml from /projects/Cheatsheet/Airflow/Deplyoy_Airflow/docker-compose.yaml
cp /projects/Cheatsheet/Airflow/Deploy_Airflow_Image/Dockerfile .
cp /projects/Cheatsheet/Airflow/Deploy_Airflow_Image/docker-compose.yaml .

# copy the makefile from /projects/Cheatsheet/Airflow/Deplyoy_Airflow/Makefile
cp /projects/Cheatsheet/Airflow/Deploy_Airflow_Image/makefile .

curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.5.0/airflow.sh'
chmod +x airflow.sh

# Create an empty .env file
> .env

# Add an airflow user id to the file
echo AIRFLOW_UID=50000 >> .env
echo PYTHONPATH=$(pwd) >> .env
echo AIRFLOW_HOME_LOCAL=$(pwd) >> .env
echo AIRFLOW_HOME_CONTAINER=/opt/airflow >> .env
echo GOOGLE_CONFIG=config/ServiceKey_GoogleCloud.json >> .env

export PYTHONPATH=$(pwd)

# Add directories to the dags folder
cd airflow/dags

mkdir data
mkdir helper

# Grant user airflow permissions to all airflow folders
sudo chown -R 50000:50000 $(pwd)

# Add a pipeline.conf file to the config folder
touch ${AIRFLOW_HOME}/config/pipeline.conf
echo "[pipeline]" >> ${AIRFLOW_HOME}/config/pipeline.conf
echo "data_dir = /opt/airflow/dags/data" >> ${AIRFLOW_HOME}/config/pipeline.conf
echo "helper_dir = /opt/airflow/dags/helper" >> ${AIRFLOW_HOME}/config/pipeline.conf
echo "log_dir = /opt/airflow/logs" >> ${AIRFLOW_HOME}/config/pipeline.conf

# Start airflow
docker-compose up -d

# Generate a requirements.txt file
pip freeze > /opt/airflow/dags/requirements.txt