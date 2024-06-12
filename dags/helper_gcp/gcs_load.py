import os, sys
import pandas as pd
from sqlalchemy import create_engine

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path + "/my_utils")

from helper_gcp.gcs_util import GoogleCloudStorage

gcs_handler = GoogleCloudStorage('sch-lake-theanh')

class Load:
    def __init__(self, db_url):
        self.engine = create_engine(db_url, credentials_path=gcs_handler.keyfile_path)

    def execute(self, parquet_path, table_name):
        df = pd.read_parquet(parquet_path, storage_options={"token": gcs_handler.keyfile_path})
        latest_row = df.loc[df['ingestion_dt_unix'].idxmax()]
        df_latest = df[df['ingestion_dt_unix'] == latest_row['ingestion_dt_unix']]
        df_latest.to_sql(table_name, self.engine, if_exists='replace', index=False)