
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators import bigquery_operator,bigquery_to_gcs,gcs_to_gcs
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.models import Variable
default_args = {
    'start_date': datetime.datetime(2021, 7, 1),
    'owner': 'santhosh_krishna',
    'depends_on_past': False,
    'email': ['santoshkrishna53@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup':False,
    }
dag=DAG('assignment_1',default_args=default_args,schedule_interval='@daily')

with DAG('assignment_1',default_args=default_args,schedule_interval='@daily') as dag:
    #gcs to bigqueary
    gcs_to_big_queary = GoogleCloudStorageToBigQueryOperator(
    task_id = 'gcs_to_big_queary',
    bucket = 'dataflow-pub-sub-training2',
    source_objects = ['data.csv'],
    destination_project_dataset_table = 'airflow.new_table',
    schema_fields=[
        {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'brand', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'made_in', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'price', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    skip_leading_rows = 1
    )
    
    # run query from temp table
    get_table_from_bq = bigquery_operator.BigQueryOperator(
    task_id='bq_from_temp_table',
    sql="SELECT * FROM springmltraining-316807.airflow.new_table",
    use_legacy_sql=False,
    destination_dataset_table='airflow.temp_table')
    
    # export results to bucket
    export_temp_table_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='temp_table_to_gcs_object',
    source_project_dataset_table='airflow.temp_table',
    destination_cloud_storage_uris='gs://dataflow-pub-sub-training2/composer_result/results.csv',
    export_format='CSV')
    
    # copy the file to archive 
    move_source_file_to_archive = GCSToGCSOperator(
    task_id='archive_file',
    source_bucket='dataflow-pub-sub-training2',
    source_objects=['data.csv'],
    destination_bucket='composer-archive53',
    destination_object='data.csv'
    )
    # delete temp_table
    delete__temp_table = BigQueryDeleteTableOperator(
    task_id="delete_table",
    deletion_dataset_table='springmltraining-316807.airflow.temp_table',
    )
    gcs_to_big_queary >> get_table_from_bq >> export_temp_table_to_gcs >> [delete__temp_table,move_source_file_to_archive]


