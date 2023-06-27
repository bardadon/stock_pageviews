start:
	bash start_airflow.sh

down:
	bash restart_airflow.sh

scheduler:
	docker exec -it airflow_airflow-scheduler_1 bash

dag_list:
	docker exec -it airflow_airflow-scheduler_1 airflow dags list

dag_list_errors:
	docker exec -it airflow_airflow-scheduler_1 airflow dags list-import-errors
	
webserver:
	docker exec -it airflow_webserver_1 bash

worker:
	docker exec -it airflow_worker_1 bash

flower:
	docker exec -it airflow_flower_1 bash

# Trigger dag. Grab the dag name from user input
trigger_dag = $(shell read -p "Enter dag name: " dag_name; echo $$dag_name)
trigger_date = $(shell read -p "Enter date (YYYY-MM-DD): " date; echo $$date)
trigger:
	docker exec -it airflow_airflow-scheduler_1 airflow dags trigger -e $(trigger_date) $(trigger_dag)

# Test task. Grab the dag name from user input
test_dag = $(shell read -p "Enter dag name: " dag_name; echo $$dag_name)
test_date = $(shell read -p "Enter date (YYYY-MM-DD): " date; echo $$date)
test_task = $(shell read -p "Enter task name: " task_name; echo $$task_name)
test_task:
	docker exec -it airflow_airflow-scheduler_1 airflow tasks test $(test_dag) $(test_task) $(test_date)

# Test dag. Grab the dag name from user input
test_dag = $(shell read -p "Enter dag name: " dag_name; echo $$dag_name)
test_date = $(shell read -p "Enter date (YYYY-MM-DD): " date; echo $$date)
test_dag:
	docker exec -it airflow_airflow-scheduler_1 airflow dags test $(test_dag) $(test_date)