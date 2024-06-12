from airflow.decorators import dag, task
from datetime import datetime
from sqlalchemy import create_engine
from helper_gcp.gcs_util import GoogleCloudStorage

@dag(dag_id='gcp_db_scd2',
     start_date=datetime(2021, 10, 26),
     catchup=False,
     schedule_interval='@daily')
def dag_etl():
    @task()
    def run_sql():
        gcs_handler = GoogleCloudStorage('sch-lake-theanh')
        db_url = 'bigquery://'
        engine = create_engine(db_url,credentials_path=gcs_handler.keyfile_path)
        with open('dags/sql/gcp_dim_product_scd2.sql', 'r') as file:
            sql_query = file.read()
            print('sqlquery', sql_query)
        
        with engine.begin() as connection:
            connection.execute(sql_query)

    run_sql()

greet_dag = dag_etl()
