
"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from pandas.io.sql import table_exists
import airflow
import pandas
import numpy as np
import json
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
# from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
import psycopg2
import configparser
from airflow.operators.dummy_operator import DummyOperator
import re


def getConnectionProperties(connection):

    dat = pandas\
        .read_sql_query("SELECT * from INGESTIONFRAMEWORK_SOURCE_DB_CONNECTION_PROPERTY;", connection)

    return dat
def get_ingestion_object(row, df_conn_props):
    df_src_db_props = df_conn_props[df_conn_props.source_db_id == row['sdb_id']]
    ing_conn_json_str = df_src_db_props[['key_', 'value_']].to_json(orient='records')
    table_details_json_str = row.to_json(orient='columns')
    ingestion_json_obj = json.loads("{}")
    ingestion_json_obj["connection"] = json.loads(ing_conn_json_str)
    ingestion_json_obj["table"] = json.loads(table_details_json_str)
    print(ingestion_json_obj)
    return ingestion_json_obj

def read_ini(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    metadb_config = {}
    for section in config.sections():
        for key in config[section]:
            metadb_config[key] = config[section][key]
    return metadb_config

# # try:
# # metadb_config = read_ini("config.ini")
# server = metadb_config['10.51.100.64']
# port = metadb_config['port']
# database = metadb_config['db']
# username = metadb_config['username']
# password = metadb_config['password']
# jdbc_url = metadb_config['jdbc_url']
# driver = metadb_config['driver']

server = "10.51.100.64"
port = "5432"
database = "boltwire_backup"
username = "boltwire_administrator"
password = "12345"
jdbc_url = "jdbc:postgresql://10.51.100.64:5432/boltwire_backup"
driver = "org.postgresql.Driver"
connection = psycopg2.connect(user=username,
                                password=password,
                                host=server,
                                port=port,
                                database=database)
cursor = connection.cursor()
df_conn_props = getConnectionProperties(connection)

ing_conn_json_str = df_conn_props[['key_','value_', 'source_db_id']]

postgreSQL_select_Query = """
    
SELECT
isct.name as src_connection_type,
ingestion.ID as INGESTION_ID,
ingestion_mode.NAME as INGESTION_MODE,ingestion.TABLE_ID as TBL_ID,
ist.NAME as SOURCE_TYPE,
sdb.ID as SDB_ID,
sdb.NAME as DB_NAME,
schemas.NAME as SCHEMA_NAME,
schematable.NAME as TABLE_NAME,
schematablehst.NAME as HISTORY_TABLE_NAME,
ifreq.FREQUENCY as FREQ,
ifp.file_type, ifp.file_reader, ifp.delimeter, 
ifp.multi_line, ifp.database_name, ifp.schema_name, 
ifp.table_name, ifp.schema_file_path, ifp.primary_keys, ifp.audit_column
FROM INGESTIONFRAMEWORK_INGESTION as ingestion
	INNER JOIN INGESTIONFRAMEWORK_FREQUENCY as ifreq
	ON ingestion .FREQUENCY_ID = ifreq .ID
	INNER JOIN INGESTIONFRAMEWORK_SCHEMA_TABLE as schematable
	ON ingestion .TABLE_ID = schematable .ID
	INNER JOIN INGESTIONFRAMEWORK_SCHEMA as schemas
	ON schematable .SCHEMA_ID = schemas .ID
	INNER JOIN INGESTIONFRAMEWORK_SOURCE_DB as sdb
	ON sdb.ID =schemas .SOURCE_DB_ID
	INNER JOIN INGESTIONFRAMEWORK_SOURCE_TYPE ist
	ON ist.ID = sdb.SOURCE_DB_TYPE_ID
	INNER JOIN INGESTIONFRAMEWORK_SOURCE_CONNECTION_TYPE isct
	ON isct.src_conn_type_id = ist.src_conn_type_id
	INNER JOIN INGESTIONFRAMEWORK_INGESTION_MODE ingestion_mode
	on ingestion_mode.id = ingestion.INGESTION_MODE_ID
	LEFT JOIN INGESTIONFRAMEWORK_TABLE_COLUMN tbl_col
	ON ingestion.column_id = tbl_col.id
	LEFT JOIN public.ingestionframework_file_properties ifp
	ON ingestion.TABLE_ID = ifp.TABLE_ID
	LEFT JOIN INGESTIONFRAMEWORK_SCHEMA_TABLE as schematablehst
	ON ingestion.HISTORY_TABLE_ID = schematablehst .ID
	WHERE ifreq.FREQUENCY = 'Daily' and isct.name='File System'
	group by ifp.file_type, ifp.file_reader, ifp.delimeter, ifp.multi_line, ifp.database_name, ifp.schema_name, 
			 ifp.table_name, ifp.schema_file_path, ifp.primary_keys, ifp.audit_column,
			 isct.name, ingestion_mode.NAME, ingestion.ID, ingestion.TABLE_ID, ist.NAME, 
			 sdb.ID, sdb.NAME, schemas.NAME, schematable.NAME,  schematablehst.NAME, ifreq.FREQUENCY
	;
    """

cursor.execute(postgreSQL_select_Query)

df_ingestion_tables = pandas.read_sql_query(postgreSQL_select_Query, connection)
ingestion_numpy = df_ingestion_tables.to_numpy()
conn_json = ing_conn_json_str.to_numpy()



args = {
"owner": "airflow",
"email": [],
"email_on_failure": True,
'email_on_retry': False,
'databricks_conn_id': 'adb_workspace',
'start_date': datetime(2021, 11, 7),
'retries': 3,
'retry_delay': timedelta(minutes=1)
}
dag = DAG("master_dag", catchup=False, default_args=args, schedule_interval='@daily')



notebook_task_params = {
    'existing_cluster_id': '', # add your cluster id
    'notebook_task': {
        'notebook_path': '', # absolute path of boltwire_ingestion notebook in databricks workspace
        "base_parameters": {
            'ingestion_list': '',
            'job_id': '{{ run_id }}',
            "audit_parameters":'',
        },
        
    },
    'run_name': ''
}

# print(conn_json)

ing_objects = []
for index, row in df_ingestion_tables.iterrows():
    ing_objects.append(get_ingestion_object(row, ing_conn_json_str))

load_per_job = 1
max_jobs = len(ing_objects)
# print(max_jobs)

audit_params = {
                'audit_jdbc_url' : jdbc_url,
                'audit_conn_prop' : {
                        'username': username,
                        'password': password,
                        'driver': driver
                    }
                }
audit_json =  json.dumps(audit_params)
notebook_task_params['notebook_task']['base_parameters']['audit_parameters'] = audit_json


for i in range(0, max_jobs, load_per_job):
    params = json.dumps(ing_objects[i:i + load_per_job])
    notebook_task_params['notebook_task']['base_parameters']['ingestion_list'] = params

    # print(params)
    # print(ing_objects[i:i + load_per_job])



    if ing_objects[i:i + load_per_job][0]["table"]["src_connection_type"] == "File System":
        # print (ing_objects[i:i + load_per_job][0]["table"]["src_connection_type"]) 

        source_to_raw = DummyOperator(

            task_id = "source_to_raw",
            dag = dag
        )

        raw_to_ods = DummyOperator(
            task_id = "raw_to_ods",
            dag = dag
            
        )

        source_to_raw >> raw_to_ods
    
    task_id = ""
    for j in range(0, load_per_job):
        task_id += str(ing_objects[i:i + load_per_job][j]['table']['db_name'])+'_'+ str(ing_objects[i:i + load_per_job][j]['table']['table_name']) 
        task_id = re.sub(r'[^0-9a-zA-Z:,]+', '_', task_id).lower()

    notebook_task_params['run_name'] = task_id.lower()

    # notebook_task = DatabricksSubmitRunOperator(
    #     task_id=task_id.lower(),
    #     dag=dag,
    #     json=notebook_task_params,
    #     task_concurrency=1,
    #     pool='ingestion_pool'
    # )



# except (Exception, psycopg2.Error) as error:
#     print("Error while fetching data from PostgreSQL", error)

# finally:
#     # closing database connection.
#     if connection:
#         cursor.close()
#         connection.close()
#         print("PostgreSQL connection is closed")