from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
with DAG(dag_id='bash_dag', schedule_interval=None, start_date=datetime(2020, 1, 1), catchup=False) as dag:
# Task 1
dummy_task = DummyOperator(task_id='dummy_task')



snapshot_query= """  
{% snapshot {table_name} %}
{%endsnapshot%}""".format(table_name)
BashOperator(
    echo '' > snapshot.sql && echo snapshot_query > snapshot.sql
)

# Task 2
bash_task = BashOperator(task_id='bash_task', bash_command="echo echo snapshot_query > snapshot.sql")





dummy_task >> bash_task