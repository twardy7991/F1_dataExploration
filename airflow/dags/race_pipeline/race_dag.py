from race_pipeline.extract import get_session, get_session_telemetry
from race_pipeline.transform import build_session_dataset
from race_pipeline.load import save_session_df_to_pickle

from airflow.decorators import dag
from datetime import timedelta, datetime
from airflow.utils import timezone

default_args = default_args = {"owner": "airflow", "start_date": datetime(2021, 1, 1)}

year = 2023

@dag(default_args=default_args, schedule=None, catchup=False)
def etl_pipeline():
    
    data = get_session()
    transformed = build_session_dataset(data)
    save_session_df_to_pickle(transformed)
    
race_pipeline = etl_pipeline() 