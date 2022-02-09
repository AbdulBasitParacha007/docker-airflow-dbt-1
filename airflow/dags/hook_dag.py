from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 12, 17),
    "email": ["airflow@airflow.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def get_activated_sources():
    request = "SELECT * FROM audit_tracker"
    pg_hook = PostgresHook(postgres_conn_id="postgre_sql", schema ="boltwire_backup")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    for source  in sources:
        print("source: {0} -activated: {1}" .format(source[0], source[1]))
    return sources

with DAG( 'hook_dag', default_args=default_args, schedule_interval='@once',catchup=False) as dag:
    start_task = DummyOperator(
        task_id = 'start_task'
    )

    hook_task = PythonOperator(
        task_id = 'hook_task',
        python_callable = get_activated_sources 
        )

    start_task >> hook_task
