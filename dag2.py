
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators import bigquery_operator,bigquery_to_gcs,gcs_to_gcs
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.models import Variable
default_args = {
    'start_date': datetime.datetime(2021, 7, 5),
    'owner': 'santhosh_krishna',
    'depends_on_past': False,
    'email': ['santoshkrishna53@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup':False,
    }
dag=DAG('assignment_2',default_args=default_args,schedule_interval='@daily')
file_name = '{{ dag_run.conf["name"] }}'
with DAG('assignment_2',default_args=default_args,schedule_interval='@daily') as dag:
    t1 = BashOperator(
        task_id='print_gcs_info',
        bash_command='echo Triggered from GCF: {{ dag_run.conf }}',
    )
    #gcs to bigqueary
    gcs_to_big_queary = GoogleCloudStorageToBigQueryOperator(
    task_id = 'gcs_to_big_queary',
    bucket = 'dataflow-pub-sub-training2',
    source_objects = [file_name],
    destination_project_dataset_table = '{{ var.json.get("variables")[dag_run.conf["name"]]["TABLE_NAME"] }}',
    autodetect = True,
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    skip_leading_rows = 1
    )
    
    # run query from temp table
    get_table_from_bq = bigquery_operator.BigQueryOperator(
    task_id='bq_from_temp_table',
    sql='{{ var.json.get("variables")[dag_run.conf["name"]]["SQL"] }}',
    use_legacy_sql=False,
    destination_dataset_table='{{ var.json.get("variables")[dag_run.conf["name"]]["TABLE_TEMP"] }}')
    
    # export results to bucket
    export_temp_table_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='temp_table_to_gcs_object',
    source_project_dataset_table='{{ var.json.get("variables")[dag_run.conf["name"]]["TABLE_TEMP"] }}',
    destination_cloud_storage_uris='{{ var.json.get("variables")[dag_run.conf["name"]]["destination_cloud_storage_uris"] }}',
    export_format='CSV')
    
    # copy the file to archive 
    move_source_file_to_archive = GCSToGCSOperator(
    task_id='archive_file',
    source_bucket='dataflow-pub-sub-training2',
    source_objects=[file_name],
    destination_bucket='composer-archive53',
    destination_object=file_name
    )
    # delete temp_table
    delete__temp_table = BigQueryDeleteTableOperator(
    task_id="delete_table",
    deletion_dataset_table='{{ var.json.get("variables")[dag_run.conf["name"]]["deletion_dataset_table"] }}',
    )
    t1 >> gcs_to_big_queary >> get_table_from_bq >> export_temp_table_to_gcs >> [delete__temp_table,move_source_file_to_archive]


