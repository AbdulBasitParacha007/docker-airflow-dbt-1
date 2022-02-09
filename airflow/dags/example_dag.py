from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

from airflow.contrib.sensors.wasb_sensor import WasbBlobSensor, WasbPrefixSensor

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'tutorial',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='install_azure_blob_package',
        bash_command='pip install azure-storage-blob',
    )

    t2 = BashOperator(
        task_id='install_azure_blob_queue_package',
        bash_command='pip install azure-storage-queue',
        retries=3,
    )
    # t1.doc_md = dedent(
    #     """\
    # #### Task Documentation
    # You can document your task using the attributes `doc_md` (markdown),
    # `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    # rendered in the UI's Task Instance Details page.
    # ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    # """
    # )

    # dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
    # dag.doc_md = """
    # This is a documentation placed anywhere
    # """  # otherwise, type it like this

    # t3 = BashOperator(
    #     task_id='print_time2',
    #     bash_command='time',
    # )

    # t4 = WasbBlobSensor(
    #     task_id="check_if_blob_exists",
    #     poke_interval=30,
    #     container_name="testing",
    #     blob_name="Workbook1.zip",
    #     wasb_conn_id ="datalake_nbb_testing"
    # )

    # t4 >> [t1, t2, t3]