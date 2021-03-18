from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators.python_operator import PythonOperator

from pipe_utils import file_to_s3
from pipe_utils import delete_file

default_args = {
	"owner": "airflow",
	"depends_on_past": True,
	"wait_for_downstream": True,
	"start_date": datetime(2010, 12, 1),
	"email": ["airflow@airflow.com"],
	"email_on_failure": False,
	"email_on_retry": False,
	"retries": 2,
	"retry_delay": timedelta(minutes = 1)
}

# Initialise Directed Acyclical Graph
dag = DAG("load_purchases", default_args = default_args, schedule_interval = None, max_active_runs = 1)

# Copy from the Postgres DB, records from today
unload_user_purchase = "scripts/sql/filter_unload_user_purchase.sql"
temp_filtered_user_purchase = "/temp/temp_filtered_user_purchase.csv"
pg_unload = PostgresOperator(
	dag = dag,
	task_id = "pg_unload",
	sql = unload_user_purchase,
	postgres_conn_id = "postgres_default",
	params = {
		"temp_filtered_user_purchase": temp_filtered_user_purchase
	}
)

# Upload to S3 bucket
bucket_name = "data-eng-dumps"
temp_filtered_user_purchase_key = "user_purchase/staging/{{ ds }}/temp_filtered_user_purchase.csv"

file_to_s3_task = PythonOperator(
	dag = dag,
	task_id = "file_to_s3",
	python_callable = file_to_s3,
	op_kwargs = {
		"filename": temp_filtered_user_purchase, 
		"key": temp_filtered_user_purchase_key,
		"bucket_name": bucket_name
	}
)

# Delete CSV
delete_csv_task = PythonOperator(
	dag = dag,
	task_id = "remove_local_csv_file",
	python_callable = delete_file,
	op_kwargs = {
		"location": temp_filtered_user_purchase
	}
)

# The pipeline
pg_unload >> file_to_s3_task >> delete_csv_task