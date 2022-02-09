import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta

dag = DAG(
    "dbt_basic_dag",
    start_date=datetime(2020, 12, 23),
    default_args={"owner": "astronomer", "email_on_failure": False},
    description="A sample Airflow DAG to invoke dbt runs using a BashOperator",
    schedule_interval=None,
    catchup=False,
)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR")

with dag:
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"source {AIRFLOW_HOME}/dbt-env/bin/activate && dbt debug --project-dir {DBT_PROJECT_DIR}"
    )

    # This task loads the CSV files from dbt/data into the local postgres database for the purpose of this demo.
    # In practice, we'd usually expect the data to have already been loaded to the database.
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"source {AIRFLOW_HOME}/dbt-env/bin/activate && dbt seed --project-dir {DBT_PROJECT_DIR}"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"source {AIRFLOW_HOME}/dbt-env/bin/activate && dbt run --project-dir {DBT_PROJECT_DIR}"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"source {AIRFLOW_HOME}/dbt-env/bin/activate && dbt test --project-dir {DBT_PROJECT_DIR}"
    )

    dbt_debug >> dbt_seed >> dbt_run >> dbt_test
