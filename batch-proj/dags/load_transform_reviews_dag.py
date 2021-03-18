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

dag = DAG("load_transform_reviews", default_args = default_args, schedule_interval = None, max_active_runs = 1)

# Copy movie reviews to S3
bucket_name = "data-eng-dumps"
reviews_file = "/data/movie_review/movie_review.csv"
reviews_key = "movie_review/load/movie.csv"

reviews_to_s3 = PythonOperator(
	dag = dag,
	task_id = "file_to_s3",
	python_callable = file_to_s3,
	op_kwargs = {
		"filename": reviews_file, 
		"key": reviews_key,
		"bucket_name": bucket_name
	}
)

reviews_to_s3