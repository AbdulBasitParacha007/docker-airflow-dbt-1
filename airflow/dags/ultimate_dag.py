from airflow import DAG
from datetime import datetime, timedelta, timezone
from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
# from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import json
import base64
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
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


def get_events():    
    connect_str = "DefaultEndpointsProtocol=https;AccountName=azrgeuwasbpoca847721;AccountKey=okmM8IoWMB+u5B8WFz1OglJ1E+YQnnhTcT3HJHNymRGeKkuBVCmmB8Z86wsiusDVC9exKqqhJLfHXwpBeXnkAA==;EndpointSuffix=core.windows.net"
    queue_name = "testing-queue-2"

    queue_client = QueueClient.from_connection_string(connect_str, queue_name)

    messages = queue_client.receive_messages()

    filter_out_list = ["/processing-files/","/extracted_files/"]
    temp_list = []
    for x in messages:
        temp_data = json.loads(base64.b64decode(x.content))['data']['blobUrl']
        # if "/processing-files/" not in temp_data:
        if not any(y in temp_data for y in filter_out_list):
            temp_list.append(json.loads(base64.b64decode(x.content)))
        queue_client.delete_message(x)
    return temp_list


def create_df_of_events(events_list):
    topic = []
    subject = []
    contentType = []
    blobUrl = []
    url = []
    eventTime = []
    original_dict = []

    for x in events_list:
        topic.append(x['topic'])
        subject.append(x['subject'])
        contentType.append(x['data']['contentType'])
        blobUrl.append(x['data']['blobUrl'])
        url.append(x['data']['url'])
        eventTime.append(x['eventTime'])
        original_dict.append(x)

    df_dict = {'Topic': topic, 'Subject': subject, 'ContentType': contentType, 'BlobURL':blobUrl, 'URL':url, 'EventTime':eventTime, 'OriginalJSON':original_dict}
    df = pd.DataFrame(df_dict)
    return df


def filter_events_to_upload(config_json,df):
    new_events_dict = {}
    for index, row in df.iterrows():
        try:
            new_events_dict[row['BlobURL'].rsplit("/",1)[0]+"/"].append(row['BlobURL'])
        except:
            new_events_dict[row['BlobURL'].rsplit("/",1)[0]+"/"] = []
            new_events_dict[row['BlobURL'].rsplit("/",1)[0]+"/"].append(row['BlobURL'])

    # folders_to_monitor = {}
    # for x,y in config_json['datasets'].items():
    #     folders_to_monitor[y['monitor_folder']] = x

    folders_to_monitor = {}
    for x,y in config_json['datasets'].items():
        temp_url_str = "https://"+y['ingestion_properties']['storage_account']+".blob.core.windows.net/"+y['ingestion_properties']['container']+"/"+y['ingestion_properties']['folder_path']+"/"
        folders_to_monitor[temp_url_str] = x

    new_events_final = {}
    for x in new_events_dict.keys():
        if x in folders_to_monitor.keys():
            new_events_final[folders_to_monitor[x]] = new_events_dict[x]
            
    return new_events_final


def upload_event_to_blob(config,dataset,data):
    connect_str = "DefaultEndpointsProtocol=https;AccountName=azrgeuwasbpoca847721;AccountKey=okmM8IoWMB+u5B8WFz1OglJ1E+YQnnhTcT3HJHNymRGeKkuBVCmmB8Z86wsiusDVC9exKqqhJLfHXwpBeXnkAA==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    container_name = config["events_processing_properties"]["container"]
    
    event_time = data['EventTime']
    event_time = event_time[:25]
    
    # Upload to All FIles directory
    event_time_object = datetime.strptime(event_time, "%Y-%m-%dT%H:%M:%S.%f")
    file_name = dataset + "/events/All_Events/" + "Year="+str(event_time_object.year) +"/"+ "Month="+str(event_time_object.month) +"/"+ "Day="+str(event_time_object.day) +"/"+ "events_" + event_time + ".json"
    
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    blob_client.upload_blob(json.dumps(data['OriginalJSON']),blob_type="BlockBlob")
    
    # Upload to Processing Files directory
    file_name = dataset + "/events/Events_to_Process/New/" + "events_" + event_time + ".json"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    blob_client.upload_blob(json.dumps(data['OriginalJSON']),blob_type="BlockBlob")


def upload_new_events_to_blob(config,events,df):
    for x in events.keys():
        urls_to_process = []
        for y in events[x]:
            urls_to_process.append(y)

        df_temp = df[df['BlobURL'].isin(urls_to_process)]
        df_temp = df_temp.sort_values("EventTime").reset_index(drop=True)

        for index, row in df_temp.iterrows():
            upload_event_to_blob(config["datasets"][x],x,row)


def orchestrator(ti,**kwargs):
    config_json = kwargs['config_json']

    #Fetching new events and storing them as JSONs in their respective folders on storage account
    new_events_list = get_events()
    new_events_df = create_df_of_events(new_events_list)
    new_events_to_upload = filter_events_to_upload(config_json, new_events_df)
    upload_new_events_to_blob(config_json,new_events_to_upload,new_events_df)

    return ["databricks_run_" + x for x in list(new_events_to_upload.keys())]
    # ti.xcom_push(key="ds_new_events",value=list(new_events_to_upload.keys()))


def create_databricks_task(dataset_name, config_json):
    notebook_task = {
    'notebook_path':'/Users/a847721@asb.dtcbtndsie.onmicrosoft.com/Master Notebook',
    'base_parameters':{"dataset_name":dataset_name, "dataset_config": json.dumps(config_json['datasets'][dataset_name]), "time":  str(datetime.now())}
    }

    return DatabricksSubmitRunOperator(
        task_id='databricks_run_'+dataset_name,
        existing_cluster_id='1220-075017-ud0g4j7l',
        databricks_conn_id='databricks_nbb',
        # notebook_params={"name": "john doe", "age":  "35"},
        notebook_task = notebook_task,
        # notebook_task={'notebook_path':'/Users/a847721@asb.dtcbtndsie.onmicrosoft.com/tetsing airflow','base_parameters':{"queue messages": str(x), "time":  str(datetime.now())}},
        dag=dag,
        task_concurrency=1
    )

dag = DAG('ultimate_dag',default_args=default_args,description='Python',schedule_interval=None,start_date=datetime(2021, 1, 1),catchup=False)

config_json = {
    "datasets":{
        "emir_ds":{
            "source_name":"emir",
            "dataset_name":"emir_ds",
            "ingestion_properties":{"storage_account":"azrgeuwasbpoca847721","container":"testing","folder_path":"emir_ds","file_name":""},
            "delivery_properties":{"mode":"overwrite"},
            "events_processing_properties":{"storage_account":"azrgeuwasbpoca847721","container":"processing-files","folder_path":""},
            "data_type":"xml",
            "compression":{"compression":True,"extension":"zip"},
            "xml_properties":{"flatten":True,"xsd":{"xsd":True,"storage_account":"","container":"","folder_path":"","file_name":""}, "rowTag":""},
            "on_failure":{"action":"stop","notification_emails":["abc@xyz.com"]}
        },
        "gleif_ds":{
            "source_name":"gleif",
            "dataset_name":"gleif_ds",
            "ingestion_properties":{
                "storage_account":"azrgeuwasbpoca847721",
                "container":"testing",
                "folder_path":"gleif_ds",
                "file_name":""
            },
            "delivery_properties":{
                "mode":"overwrite"
            },
            "events_processing_properties":{
                "storage_account":"azrgeuwasbpoca847721",
                "container":"processing-files",
                "folder_path":""
            },
            "data_type":"xml",
            "compression":{
                "compression":True,
                "extension":"zip"
            },
            "xml_properties":{
                "flatten":True,
                "xsd":{
                    "xsd":True,
                    "storage_account":"azrgeuwasbpoca847721",
                    "container":"testing",
                    "folder_path":"gleif_ds/xsd",
                    "file_name":"2017-03-21_lei-cdf-v2-1_gleif_xsd_2016.xsd",
                    "validation":{
                        "auto_schema_detection":False,
                        "namespace":"lei"
                    },
                },
                "rowTag":"lei:LEIRecord"
            },
            "on_failure":{
                "action":"stop",
                "notification_emails":["abc@xyz.com"]
            }
        },
        "fcb_ds":{
            "source_name":"fcb",
            "dataset_name":"fcb_ds",
            "ingestion_properties":{"storage_account":"azrgeuwasbpoca847721","container":"testing","folder_path":"fcb_ds","file_name":""},
            "delivery_properties":{"mode":"overwrite"},
            "events_processing_properties":{"storage_account":"azrgeuwasbpoca847721","container":"processing-files","folder_path":""},
            "data_type":"xml",
            "compression":{"compression":False,"extension":None},
            "xml_properties":{"flatten":True,"xsd":{"xsd":True,"storage_account":"","container":"","folder_path":"","file_name":""}, "rowTag":""},
            "on_failure":{"action":"stop","notification_emails":["abc@xyz.com"]}
        },
    }
}

# task_1 = PythonOperator(
#     task_id="Orchestrator",
#     python_callable=orchestrator,
#     op_kwargs={'config_json':config_json},
#     dag=dag
# )

# dependent_tasks = []
# for x in config_json['datasets']:
#     create_databricks_tasks(dataset_name=x,config_json=config_json)
#     dependent_tasks.append("databricks_run_"+x)

task_1 = BranchPythonOperator(
    task_id="Orchestrator",
    python_callable=orchestrator,
    op_kwargs={'config_json':config_json},
    dag=dag
)

for x in config_json['datasets']:
    task_1 >> create_databricks_task(dataset_name=x,config_json=config_json)
