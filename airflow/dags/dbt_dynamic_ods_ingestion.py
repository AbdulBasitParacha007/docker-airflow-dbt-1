import os
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime

dag = DAG(
    "dbt_dynamic_ods_merge",
    start_date=datetime(2020, 12, 23),
    default_args={"email_on_failure": False},
    description="A sample Airflow DAG to invoke the dynamic ods merge dbt transformation",
    schedule_interval=None,
    catchup=False,
)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR")

with dag:

    templated_command = dedent(
        """
    source {{ params.AIRFLOW_HOME }}/dbt-env/bin/activate && dbt run --models ods_dynamic_ingestion_model --project-dir {{ params.DBT_PROJECT_DIR }} --vars "{{ params.dynamic_ods_ingestion_vars }}"
    """
    )

    dbt_dynamic_ods_ingestion = BashOperator(
        task_id="dbt_dynamic_ods_ingestion",
        bash_command=templated_command,
        params={
            'AIRFLOW_HOME': AIRFLOW_HOME,
            'DBT_PROJECT_DIR': DBT_PROJECT_DIR,
            'dynamic_ods_ingestion_vars': {
                'source_schema': 'use_dag_config',
                'source_table': 'use_dag_config',
                'target_schema': 'use_dag_config',
                'target_table': 'use_dag_config',
                'primary_keys': ['use_dag_config']
            }
        }
    )
