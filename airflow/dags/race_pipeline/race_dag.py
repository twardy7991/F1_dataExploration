from race_pipeline.extract import get_session, get_session_telemetry, extract
from race_pipeline.transform import build_session_dataset
from race_pipeline.load import save_session_df_to_pickle

from airflow.decorators import dag
from datetime import timedelta, datetime
from airflow.utils import timezone

from airflow.models.param import Param

default_args = {"owner": "airflow", "start_date": datetime(2021, 1, 1)}

race_year = 2023
race_name = "Saudi Arabia"

@dag(dag_id = "race_dag", default_args=default_args, schedule=None, catchup=False, params={"race_name" : race_name, "year" : race_year})
def etl_pipeline():
    
    extract_race = extract()
    transformed = build_session_dataset(race_name=race_name, race_year=race_year, fuel_start=100)
    save_session_df_to_pickle()

race_pipeline = etl_pipeline() 