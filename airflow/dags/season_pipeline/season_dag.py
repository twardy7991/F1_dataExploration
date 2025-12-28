import pandas as pd
from fastf1.core import Telemetry
from typing import List
from pathlib import Path
import fastf1
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param
from datetime import datetime

DIRECTORY_PATH = "./data"

@dag(
    dag_id = "season_dag",
    schedule = None,
    start_date=datetime(2021, 1, 1),
    catchup = False,
    params = {"year" : Param(2020, type="integer", minimum=2020)}
)
def race_dag():

    @task
    def extract_transform(**context):
        year = context["params"]["year"]

        for race_name in fastf1.get_event_schedule(year).Country.unique[:3]:
            TriggerDagRunOperator(trigger_dag_id="race_dag", 
                                start_date = datetime(2021, 1, 1),
                                conf = {"race_name" : race_name},
                                wait_for_completion=True #disables parallel processing (idk if possible, but my laptop would go bum anyway)
                                )
    @task    
    def load():
        dir = Path(f"{DIRECTORY_PATH}/races/{year}")

        season_df = pd.DataFrame()
        for parquet in dir:
            race_df = pd.read_parquet(parquet)
            pd.concat(season_df, race_df)

        season_df.to_parquet(
            path=f"{DIRECTORY_PATH}/seasons"
        )

    e = extract_transform()
    l = load()

    e >> l

year = 2023

example_dag = race_dag(year=year)

# def load_season_races_from_pickle(season_year): 

#     directory = Path(f"{DIRECTORY_PATH}/{season_year}")
    
#     if directory.is_dir():
#         for pickle in directory.rglob("*.pkl"):
#             yield load_race_from_pickle(pickle)
#     else:
#         raise FileNotFoundError("Season directory is not existing, please create directory and download races with race pipeline")
    
# def load_race_from_pickle(pickle_path, gp_name):
    
#     try: 
#         tel : pd.DataFrame = pd.read_pickle(pickle_path)
#     except FileNotFoundError:
#         raise FileNotFoundError(f"Telemetry data not found for {gp_name}. Expected file: {pickle_path}")

#     telemetry : Telemetry = Telemetry(tel)
#     telemetry.has_channel = lambda c: c in telemetry.columns  
#     telemetry['Time'] = pd.to_timedelta(telemetry['Time'])
#     return telemetry

