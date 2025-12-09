from race_pipeline.extract import get_session, get_session_telemetry
from race_pipeline.transform import build_session_dataset
from race_pipeline.load import save_session_df_to_pickle

from airflow.decorators import dag
from datetime import timedelta, datetime
from airflow.utils import timezone

default_args = default_args = {"owner": "airflow", "start_date": datetime(2021, 1, 1)}

race_year = 2023
race_name = "Saudi Arabia"

@dag(default_args=default_args, schedule=None, catchup=False)
def etl_pipeline():
    
    transformed = build_session_dataset(race_name, race_year)
    save_session_df_to_pickle(transformed, gp_name=race_name, session_type="R", year=race_year)
    
race_pipeline = etl_pipeline() 