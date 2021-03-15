from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator

unload_user_purchase = "scripts/sql/filter_unload_user_purchase.sql"
temp_filtered_user_purchase = "/temp/temp_filtered_user_purchase.csv"

default_args = {
	"owner": "airflow",
	"depends_on_past": True,
	"wait_for_downstream": True,
	"start_date": datetime(2010, 12, 1),
	"email": ["airflow@airflow.com"],
	"email_on_failure": False,
	"email_on_retry": False,
	"retries": 2,
	"retry_delay": timedelta(minutes = 5)
}

# Initialise Directed Acyclical Graph
dag = DAG("user_behaviour_dag", default_args = default_args, schedule_interval = "0 0 * * *", max_active_runs = 1)

# Copy from the Postgres DB records from today
pg_unload = PostgresOperator(
	dag = dag,
	task_id = "pg_unload",
	sql = unload_user_purchase,
	postgres_conn_id = "postgres_default",
	params = {
		"temp_filtered_user_purchase": temp_filtered_user_purchase
	},
	depends_on_past = True,
	wait_for_downstream = True
)

# Dummy task
end_of_pipeline = DummyOperator(
	dag = dag,
	task_id = "end_of_pipeline"
)

# The pipeline
pg_unload >> end_of_pipeline