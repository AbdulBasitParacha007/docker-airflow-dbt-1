import pandas
import json
import configparser, os
import re
from airflow.hooks.base import BaseHook
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from ingestion_queries import *
import os
import copy


def __get_key_value(props):
    params = dict()
    for prop in props:
        params[prop['key_']] = prop['value_']

    return params

def __get_databricks_token(conn_name='databricks_default'):
    PAToken = None
    try:
        PAToken = BaseHook.get_connection(conn_name).password
    except Exception as error:
        print('databricks connection: token not found; revert to user defined')
    return PAToken

def __format_table_name(table_name_format,table_name,delivery_mode):
    return replace_clean_str(table_name_format.format(TABLE_NAME=table_name,DELIVERY_MODE=delivery_mode))

def __format_schema_name(schema_name_format,table,params):
    metadb_config = read_ini()

    current_env = metadb_config['environment'].lower()
    params['ZoneName'] = 'ods' if not 'ZoneName' in params else params['ZoneName']
    return schema_name_format.format(DATABASE_NAME=replace_clean_str(table['db_name']),SCHEMA_NAME=replace_clean_str(table['schema_name']),ZONE_NAME=replace_clean_str(params['ZoneName']),ENV=current_env)

def __format_mount_path(mount_name_format,zone_name):
    return mount_name_format.format(ZoneName=zone_name)

def __format_folder_name(folder_name_format,table,params,dir_type='data'):
    metadb_config = read_ini()
    current_env = metadb_config['environment'].lower()

    params['ZoneName'] = 'ods' if not 'ZoneName' in params else params['ZoneName']
    return folder_name_format.format(DATABASE_NAME=replace_clean_str(table['db_name']),SCHEMA_NAME=replace_clean_str(table['schema_name']),ZONE_NAME=replace_clean_str(params['ZoneName']),ENV=current_env,DIR_TYPE=dir_type)

def __format_source_folder_path(source):
    return f"{source['schema_name']}/{source['table_name']}"

def __getConnectionProperties(connection):

    dat = pandas\
        .read_sql_query("SELECT * from boltwire_db_source_db_connection_property;", connection)
    return dat

def __getTargetConnectionProperties(connection):
    
    dat = pandas\
        .read_sql_query("SELECT bdsdcp.*, bdsct.name as trg_connection_type from boltwire_db_source_db_connection_property bdsdcp inner join boltwire_db_source_db bdsd on bdsdcp.source_db_id = bdsd.id inner join boltwire_db_source_connection_type bdsct on bdsd.src_conn_type_id = bdsct.id;", connection)
    return dat

def __get_metadata_from_query(conn, query):
    
    dat = pandas\
        .read_sql_query(query, conn)
    return dat

def __get_ingestion_targets(row, conn):
    df_tgt_props = __get_metadata_from_query(conn, queries['connection_properties'])
    trgArr = row['tdb_id'].split(',') if 'tdb_id' in row and row['tdb_id'] != None else []
    ingestion_json_arr = dict()
    # collect metadata for target(s)
    df_tgt_ing = __get_metadata_from_query(conn, queries['target_ingestion'])
    for item in trgArr:     
        df_trg_db_props = df_tgt_props[df_tgt_props.source_db_id == int(item)]
        ing_conn_json_str = df_trg_db_props[['key_', 'value_']].to_json(orient='records')
        df_tgt_ing_str = df_tgt_ing[df_tgt_ing.target_id == int(item)]
        # extracting connection type of target
        target_type = df_tgt_ing_str['trg_connection_type'].unique()[0]
        
        ingestion_json_arr[target_type.lower()] = {'connection': json.loads(ing_conn_json_str),'table': json.loads(df_tgt_ing_str.to_json(orient='records'))[0]}
        

    return ingestion_json_arr

def __get_ingestion_object(row, conn):    
    ingestion_json_obj = dict()
    # step 1.1: collect metadata for table columns
    df_tbl_struct = __get_metadata_from_query(conn, queries['table_structure'])
    df_tbl_struct_str = df_tbl_struct[df_tbl_struct.table_id == row['tbl_id']].to_json(orient='records')
    # step 2: collect metadata connection properties for source  
    df_src_conn_prop = __get_metadata_from_query(conn, queries['connection_properties'])
    df_src_conn_prop = df_src_conn_prop[df_src_conn_prop.source_db_id == row['sdb_id']]
    ing_conn_json_str = df_src_conn_prop[['key_', 'value_']].to_json(orient='records')
    ing_conn_json_str = __get_key_value(json.loads(ing_conn_json_str))
    # step 2: collect source metadata 
    table_details_json_str = row.to_json(orient='columns')

    ingestion_json_obj["source"] = {'connection': ing_conn_json_str,'table': json.loads(table_details_json_str)}
    ingestion_json_obj["struct"] = json.loads(df_tbl_struct_str)
    ingestion_json_obj["targets"] = __get_ingestion_targets(row, conn)
    # print(ingestion_json_obj)
    return ingestion_json_obj

def __update_file_table_details(source):    
    source['db_name'] = source['custom_db_name'] if source['src_connection_type'].lower() == 'filesystem' and source['custom_db_name'] is not None else source['db_name']
    source['schema_name'] = source['custom_schema_name'] if source['src_connection_type'].lower() == 'filesystem' and source['custom_schema_name'] is not None else source['schema_name']
    source['table_name'] = source['custom_table_name'] if source['src_connection_type'].lower() == 'filesystem' and source['custom_table_name'] is not None else source['table_name']
    source['db_name'] = source['custom_db_name'] if source['src_connection_type'].lower() == 'filesystem' and source['custom_db_name'] is not None else source['db_name']
   
    return source

def __get_partition_cols(freq):
    freq = freq.lower()
    pList = []
    if freq == '15 mins':
        pList = ['year','month','day','hour','minute']
    elif freq == 'hourly':
        pList = ['year','month','day','hour']
    elif freq == 'every 6 hours':
        pList = ['year','month','day','hour']
    elif freq == 'every 12 hours':
        pList = ['year','month','day','hour']
    elif freq == 'daily':
        pList = ['year','month','day']
    elif freq == 'weekly':
        pList = ['year','month','day']
    elif freq == 'monthly':
        pList = ['year','month']
    elif freq == 'yearly':
        pList = ['year']

    return pList

def replace_clean_str(textItem,delimiter='_'):    
    return re.sub(r'[^0-9a-zA-Z:,]+',delimiter, textItem).strip()

def get_raw_zone_target_props(source,targetName,targets):
    rawTarget = dict()

    for targetName in targets:
        target = targets[targetName]
        if targetName.lower() == 'databricks':
            _params, rawTarget = get_target_props(source,targetName,target,'raw')
            break

    return rawTarget

def define_target_partition(target,freq):
    target['connection']['partitionCols'] = __get_partition_cols(freq)
    
    return target

def get_target_props(source_info,targetName,target,targetZone='ods'):
    source = source_info['table']
    sourceConn = source_info['connection']
    dbt_vars = dict()
    _params = __get_key_value(target['connection'])
    # if target zone is not equals to zone speified from metdata then override
    _params['ZoneName'] = targetZone if 'ZoneName' in _params and _params['ZoneName'] != targetZone else _params['ZoneName']

    if targetName.lower() == 'databricks':
        PAToken = __get_databricks_token(_params['PAToken'])
        if PAToken is None:
            PAToken = _params['PAToken']
        folder_name_format = _params['FolderPath']
        schema_name_format = _params['DatabaseName']
        table_name_format = _params['TableName']

        sourceModified = source
        if source['src_connection_type'].lower() == 'filesystem' and sourceConn['ZoneName'].lower() == 'lan':
            sourceConn['MountPath'] = __format_mount_path(sourceConn['MountPath'],sourceConn['ZoneName'])
            sourceConn['FolderPath'] = __format_source_folder_path(source)
            sourceModified = __update_file_table_details(copy.deepcopy(source))

        # forming the names from formats
        ods_tbl_name = __format_table_name(table_name_format,sourceModified['table_name'],target['table']['dm_short_code'])
        ods_folder_path = __format_folder_name(folder_name_format,sourceModified,_params)
        ods_exec_folder_path = __format_folder_name(folder_name_format,sourceModified,_params,'exec')
        ods_schema_name = __format_schema_name(schema_name_format,sourceModified,_params)
        # preparing the dbt vars for the run
        dbt_vars['BW_DBT_DATABRICKS_SCHMEA'] = ods_schema_name
        dbt_vars['BW_DBT_DATABRICKS_HOST'] = _params['HostName']
        dbt_vars['BW_DBT_DATABRICKS_HTTP_PATH'] = _params['HttpPath']
        dbt_vars['BW_DBT_DATABRICKS_PAT'] = PAToken
        
        # print(ods_folder_path,ods_exec_folder_path,ods_tbl_name,ods_schema_name)

        _params['FolderPath'] = ods_folder_path
        _params['FolderExecPath'] = ods_exec_folder_path
        _params['DatabaseName'] = ods_schema_name
        _params['TableName'] = ods_tbl_name
        # mount path will always be /mnt/{ZoneName}
        _params['MountPath'] = __format_mount_path(_params['MountPath'],_params['ZoneName'])
    elif targetName.lower() == 'synapse':
        schema_name_format = _params['SchemaName']
        table_name_format = _params['TableName']
        # forming the names from formats
        ods_schema_name = __format_schema_name(schema_name_format,source,_params)
        ods_tbl_name = __format_table_name(table_name_format,source['table_name'],target['table']['dm_short_code'])
        # preparing the dbt vars for the run
        dbt_vars['BW_DBT_SYNAPSE_SERVER'] = _params['Server']
        dbt_vars['BW_DBT_SYNAPSE_DB'] = _params['DatabasePool']
        dbt_vars['BW_DBT_SYNAPSE_SCHEMA'] = ods_schema_name
        dbt_vars['BW_DBT_SYNAPSE_USER'] = _params['Username']
        dbt_vars['BW_DBT_SYNAPSE_PASS'] = _params['Password']
        
        _params['SchemaName'] = ods_schema_name
        _params['TableName'] = ods_tbl_name
    
    # dbt vars not specific to one type of target
    dbt_vars['target_schema'] = ods_schema_name
    dbt_vars['target_table'] = ods_tbl_name
    dbt_vars['source_schema'] =  ods_schema_name
    dbt_vars['source_table'] =  f'{ods_tbl_name}_stg'
    dbt_vars['primary_keys'] = source['primary_key_columns'].split(',') if source['primary_key_columns'] is not None else []
    
    target['connection'] = _params
    
    return json.dumps(json.dumps(dbt_vars)),target

# def read_ini():
#     airflowDagFolder = os.environ["AIRFLOW__CORE__DAGS_FOLDER"]
#     configPath = f"{airflowDagFolder}/dags/config.ini"
    
#     print("mobeen",configPath)
    
#     config = configparser.ConfigParser()
#     config.read(configPath)
#     metadb_config = {}
#     for section in config.sections():
#         for key in config[section]:
#             # metadb_config[key] = config[section][key]
#             metadb_config[key] = config.get(section, key, vars=os.environ)
#     return metadb_config

def read_ini():

    metadb_config = {
        'host': os.environ["DMX_META_HOST"],
        'port': os.environ["DMX_META_PORT"],
        'db': os.environ["DMX_META_DATABASE"],
        'username': os.environ["DMX_META_USERNAME"],
        'password': os.environ["DMX_META_PASSWORD"],
        'jdbc_url': os.environ["DMX_META_JDBC_URL"],
        'driver': os.environ["DMX_META_DRIVER"],
        'spark_ods':"False",
        'environment':'dev',
        'cluster_id' :"0111-055656-bipu56a6",
        'ingestion_notebook': '/Shared/dmx_ingestion'

    }

    return metadb_config

def getIngestionList(conn,query):
    
    # step 1: collect metadata for source data extraction
    df_ingestion_tables = __get_metadata_from_query(conn,query)
    print("df_ingestion_tables", df_ingestion_tables)
    ing_objects = []
    for index, row in df_ingestion_tables.iterrows():
        ing_objects.append(__get_ingestion_object(row, conn))
    
    return ing_objects

def dbt_command_extra(delivery_mode,dbt_vars):
    dbt_refresh = ''
    delivery_mode = delivery_mode.lower()
    if delivery_mode == 'snapshot':
        dbt_refresh = '--full-refresh'
    elif delivery_mode == 'snapshotwithhistory':
        dbt_vars['primary_keys'] = ''
    
    return dbt_refresh