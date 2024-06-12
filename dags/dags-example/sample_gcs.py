from airflow.decorators import dag, task
from datetime import datetime
from helper_gcp.gcs_util import GoogleCloudStorage

@dag(dag_id='test_gcs',
     start_date=datetime(2021, 10, 26),
     catchup=False,
     schedule_interval='@daily') # or https://crontab.guru
def this_dag():
    @task()
    def my_task():
        gcs = GoogleCloudStorage("sch-lake-theanh") #change to yourbucket name
        print(gcs.list_objects())
        

    my_task()

this_dag = this_dag()