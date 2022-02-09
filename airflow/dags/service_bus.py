from datetime import datetime, timedelta
from textwrap import dedent
# from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage
# from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.python import PythonOperator
import json
import base64

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

# def _process():

#     CONNECTION_STR = "Endpoint=sb://azrgeuwasbpoc-a847721.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=gUzVIg9Pdd+AOakc2Ffngc9IzcBSJUZvQww9/MiENBU="
#     QUEUE_NAME = "gleif_Data"

#     servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True)

#     queue_messages = []
#     with servicebus_client:
#         # get the Queue Receiver object for the queue
#         receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME, max_wait_time=5)
#         with receiver:
#             for msg in receiver:
#                 print("Received: " + str(msg))
#                 queue_messages.append(str(msg))
#                 # complete the message so that the message is removed from the queue
#                 receiver.complete_message(msg)
#                 print("")
    
#     if len(queue_messages) == 0:
#         print("no messages")
#     else:
#         for x in range(len(queue_messages)):
#             print(json.loads(queue_messages[x])['data']['blobUrl'])

# events = ["laptop", "computer"]


def get_events():
    connect_str = "DefaultEndpointsProtocol=https;AccountName=azrgeuwasbpoca847721;AccountKey=+6cCorgmB2Zs6KTVQE1kMeHFpcPydx8PDpaP/dMrh6aP4MnO6HSiq09lIyrBT7qyaa46wM3UnafdKieAcUOqlQ==;EndpointSuffix=core.windows.net"
    queue_name = "testing-queue"

    queue_client = QueueClient.from_connection_string(connect_str, queue_name)

    # global events
    # temp_events = []
    # messages = queue_client.receive_messages()
    messages = queue_client.receive_messages(messages_per_page=5)

    # ll = ["pc","mag"]
    # for x in ll:
    #     temp_events.append(x)

    print_list = []
    for x in messages:
        # events.append(json.loads(base64.b64decode(x.content))['data']['blobUrl'])
        print_list.append(str(json.loads(base64.b64decode(x.content))['data']['blobUrl']))
        # queue_client.delete_message(x)

    return print_list


dag = DAG('airflow-databricks-queue',default_args=default_args,description='Python',schedule_interval=timedelta(minutes=2),start_date=datetime(2021, 1, 1),catchup=False)

# task_1 = PythonOperator(
#     task_id="python_task",
#     python_callable=get_events,
#     dag=dag
# )

events = get_events()

if len(events) != 0:
    starting_count = 1
    for x in events:
        # decoded = json.loads(base64.b64decode(x.content))['data']['blobUrl']

        notebook_task = {
        'notebook_path':'/Users/a847721@asb.dtcbtndsie.onmicrosoft.com/tetsing airflow',
        'base_parameters':{"queue messages": str(x), "time":  str(datetime.now())}
        }

        t2 = DatabricksSubmitRunOperator(
            task_id='databricks_run_'+str(starting_count),
            existing_cluster_id='1220-075017-ud0g4j7l',
            databricks_conn_id='databricks_nbb',
            # notebook_params={"name": "john doe", "age":  "35"},
            notebook_task=notebook_task,
            dag=dag
        )


        starting_count += 1
else:
    print("EMPTYYYYY")


# with DAG(
#     'airflow-databricks-queue',
#     default_args=default_args,
#     description='Python',
#     schedule_interval=timedelta(minutes=2),
#     start_date=datetime(2021, 1, 1),
#     catchup=False,
#     tags=['example'],
# ) as dag:
#     # run_this = BashOperator(
#     #     task_id='run_after_loop',
#     #     bash_command='pip install azure-storage-queue',
#     # )

#     task_1 = PythonOperator(
#         task_id="python_task",
#         python_callable=get_events
#     )
    
    # events = get_events()


    # if len(events) != 0:
    #     starting_count = 1
    #     for x in events:
    #         # decoded = json.loads(base64.b64decode(x.content))['data']['blobUrl']

    #         notebook_task = {
    #         'notebook_path':'/Users/a847721@asb.dtcbtndsie.onmicrosoft.com/tetsing airflow',
    #         'base_parameters':{"queue messages": str(x), "time":  str(datetime.now())}
    #         }

    #         t2 = DatabricksSubmitRunOperator(
    #             task_id='databricks_run_'+str(starting_count),
    #             existing_cluster_id='1220-075017-ud0g4j7l',
    #             databricks_conn_id='databricks_nbb',
    #             # notebook_params={"name": "john doe", "age":  "35"},
    #             notebook_task=notebook_task
    #         )


    #         starting_count += 1
    # else:
    #     print("EMPTYYYYY")


    # notebook_task = {
    # 'notebook_path':'/Users/a847721@asb.dtcbtndsie.onmicrosoft.com/tetsing airflow',
    # 'base_parameters':{"queue messages": "0", "time":  str(datetime.now())}
    # }

    # t2 = DatabricksSubmitRunOperator(
    #     task_id='databricks_run',
    #     existing_cluster_id='1220-075017-ud0g4j7l',
    #     databricks_conn_id='databricks_nbb',
    #     # notebook_params={"name": "john doe", "age":  "35"},
    #     notebook_task=notebook_task
    # )

#dapie786c77295affe62e5ff680994176e85-2