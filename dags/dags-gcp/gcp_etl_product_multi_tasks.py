from airflow.decorators import dag, task
from datetime import datetime

from helper_gcp.gcs_extract import Extract
from helper_gcp.gcs_transform import Transform
from helper_gcp.gcs_load import Load
from helper_gcp.gcs_etl_process import ETLProcess


api_url = 'https://api.tiki.vn/seller-store/v2/collections/116532/products'
params = {'limit': 100, 'cursor': 40}
headers = {'x-source': 'local', 'Host': 'api.tiki.vn'}
bucket_name = "sch-lake-theanh"
db_url = 'bigquery://'
transform_columns = ["tiki_pid", "name", "brand_name", "origin", 'ingestion_date', 'ingestion_dt_unix']
table_name = 'sch.dim_product'


# Initialize classes
extract = Extract(api_url, params, headers, bucket_name)
transform = Transform(bucket_name)
load = Load(db_url)

# Run ETL process
etl = ETLProcess(extract, transform, load)
    

@dag(dag_id='gcp_etl_product_multi_tasks',
     start_date=datetime(2021, 10, 26),
     catchup=False,
     schedule_interval='@daily')
def this_dag():
    @task()
    def extract():
        extracted_file = etl.run_extract()
        return extracted_file
        
    @task()
    def transform(extracted_file):
        parquet_path = etl.run_transform(transform_columns,extracted_file)
        return parquet_path

    @task()
    def load(parquet_path):
        parquet_path = etl.run_load(table_name,parquet_path)
        return parquet_path

    load(transform(extract()))
    

this_dag = this_dag()