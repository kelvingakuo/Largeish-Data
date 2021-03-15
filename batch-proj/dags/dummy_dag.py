from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

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

dag = DAG("dummy_dag", default_args = default_args, schedule_interval = "0 0 * * *", max_active_runs = 1)
end_of_pipeline = DummyOperator(task_id = "end_of_pipeline", dag = dag)

end_of_pipeline