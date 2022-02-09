from textwrap import dedent
from datetime import datetime
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.bash_operator import BashOperator

def _get_session_folder_name(session_timestamp):
    """Returns a unique folder based on the timestamp of the execution, starts with a '/'."""
    
    # Double slash is not a problem
    return f"/{datetime.now().strftime('%Y%m')}/{session_timestamp}"

def dynamic_ods_ingestion_vars(dag_config):
    """Returns dbt variables for ods ingestion based on run config."""

    return {
        'source_schema': dag_config['synapse_schema_name'],
        'source_table': f"stg_{dag_config['synapse_table_name']}",
        'target_schema': dag_config['synapse_schema_name'],
        'target_table': dag_config['synapse_table_name'],
        'primary_keys': dag_config.get('source_file_key_columns', [])
    }

dag = DAG(
    "landing_raw_ods_dbt",
    start_date=datetime(2020, 12, 23),
    default_args={"email_on_failure": False},
    description="An airflow dag to ingest files from landing to ods",
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=False,
    user_defined_macros={
        'dynamic_ods_ingestion_vars': dynamic_ods_ingestion_vars
    }
)

with dag:
    # TODO: make macro
    
    SESSION_FOLDER_NAME = _get_session_folder_name("{{ ts_nodash }}")
    landing_raw_base_params = {
        'source_container_name': "{{ dag_run.conf['source_container_name']|default('') }}",
        'source_file_format': "{{ dag_run.conf['source_file_format']|default('') }}",
        'source_has_header': "{{ dag_run.conf['source_has_header']|default('') }}",
        'source_name': "{{ dag_run.conf['source_name']|default('') }}",
        'source_path': "{{ dag_run.conf['source_path']|default('') }}",
        'target_container_name': "{{ dag_run.conf['target_container_name']|default('') }}",
        'target_file_format': "{{ dag_run.conf['target_file_format']|default('') }}",
        'target_name': "{{ dag_run.conf['target_name']|default('') }}",
        'target_path': "{{ dag_run.conf['target_path']|default('') }}" + SESSION_FOLDER_NAME
    }
    file_ingestion_landing_raw = DatabricksSubmitRunOperator(
        task_id="file_ingestion_landing_raw",
        json={
            'notebook_task': {
                'notebook_path': "/Shared/dmx_ingestion",
                'base_parameters': landing_raw_base_params
            }
        },
        existing_cluster_id="1203-092012-z28m41xi" # TODO: make dynamic or use new cluster
    )

    # TODO: make macro
    raw_ods_base_params = {
        'source_container_name': "{{ dag_run.conf['target_container_name']|default('') }}",
        'source_path': "{{ dag_run.conf['target_path']|default('') }}" + SESSION_FOLDER_NAME,
        'source_file_format': "{{ dag_run.conf['target_file_format']|default('') }}",
        'source_has_header': "{{ dag_run.conf['source_has_header']|default('') }}",
        'source_file_type': "{{ dag_run.conf['source_file_type']|default('') }}",
        'source_file_delimiter': "{{ dag_run.conf['source_file_delimiter']|default('') }}",
        'source_file_columns': "{{ dag_run.conf['source_file_columns']|default('') }}",
        'source_file_dtypes': "{{ dag_run.conf['source_file_dtypes']|default('') }}",
        'synapse_container_name': "{{ dag_run.conf['synapse_container_name']|default('') }}",
        'synapse_storage_name': "{{ dag_run.conf['synapse_storage_name']|default('') }}",
        'synapse_workspace': "{{ dag_run.conf['synapse_workspace']|default('') }}",
        'synapse_database_pool': "{{ dag_run.conf['synapse_database_pool']|default('') }}",
        'synapse_dbuser_name': "{{ dag_run.conf['synapse_dbuser_name']|default('') }}",
        'synapse_dbuser_pwd': "{{ dag_run.conf['synapse_dbuser_pwd']|default('') }}",
        'synapse_schema_name': "{{ dag_run.conf['synapse_schema_name']|default('') }}",
        'synapse_table_name': "{{ dag_run.conf['synapse_table_name']|default('') }}",
        'synapse_db_port': "{{ dag_run.conf['synapse_db_port']|default('') }}"
    }
    file_ingestion_raw_ods = DatabricksSubmitRunOperator(
        task_id="file_ingestion_raw_ods",
        json={
            'notebook_task': {
                'notebook_path': "/Shared/_file_ingestion_raw_ods",
                'base_parameters': raw_ods_base_params
            }
        },
        existing_cluster_id="1203-092012-z28m41xi" # TODO: make dynamic or use new cluster
    )

    # TODO: We could make a custom dbt operator
    templated_command = dedent(
        """
    source $AIRFLOW_HOME/dbt-env/bin/activate && dbt run --models ods_dynamic_ingestion_model --project-dir $DBT_PROJECT_DIR --vars "{{ dynamic_ods_ingestion_vars(dag_run.conf) }}"
    """
    )

    dbt_stg_ods = BashOperator(
        task_id="dbt_dynamic_ods_ingestion",
        bash_command=templated_command,
    )

    file_ingestion_landing_raw >> file_ingestion_raw_ods >> dbt_stg_ods
