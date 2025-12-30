import pandas as pd
from fastf1.core import Telemetry
from typing import List
from pathlib import Path
import fastf1
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from airflow.sdk import dag, task, Param
from datetime import datetime

DIRECTORY_PATH = "./data"




@dag(
    dag_id = "season_dag",
    schedule = None,
    start_date=datetime(2021, 1, 1),
    catchup = False,
    params = {"year" : Param(2023, type="integer", minimum=2020)}
)
def race_dag():

    # @task
    # def extract_transform(**context):
    #     year = context["params"]["year"]

    #     for race_name in fastf1.get_event_schedule(year).Country.unique[:3]:
    #         TriggerDagRunOperator(
    #                             trigger_run_id = f"season_{race_name}",
    #                             trigger_dag_id="single_race_dag", 
    #                             start_date = datetime(2021, 1, 1),
    #                             conf = {"race_name" : race_name, "year" : year},
    #                             reset_dag_run=True,
    #                             wait_for_completion=True #disables parallel processing (idk if possible, but my laptop would go bum anyway)
    #                             )
    # @task    
    # def load(**context):
        # year = context["params"]["year"]
        # dir = Path(f"{DIRECTORY_PATH}/races/{year}")

        # season_df = pd.DataFrame()
        # for parquet in dir:
        #     race_df = pd.read_parquet(parquet)
        #     pd.concat(season_df, race_df)

        # season_df.to_parquet(
        #     path=f"{DIRECTORY_PATH}/seasons"
        # )
    
    @task
    def get_season_races(**context) -> List[dict]:
        year = context["params"]["year"]

        schedule = fastf1.get_event_schedule(year).Country.unique().tolist()
        races = []

        for race_name in schedule:
            races.append({
                "race_name": race_name,
                "year": year
            })

        return races

    @task # Z chatu (uwaga: kto wie co bedzie)
    def consolidate_season(**context):
        """
        Reads all the individual race parquet files and combines them.
        """
        year = context["params"]["year"]
        source_dir = Path(f"{DIRECTORY_PATH}/processed/{year}")
        
        # List to hold dataframes and track processed races
        dfs = []
        processed_races = []
        
        # Ensure directory exists
        if not source_dir.exists():
            print(f"Directory {source_dir} does not exist. No data to load.")
            return

        # Iterate through each GP directory
        for gp_dir in source_dir.iterdir():
            if gp_dir.is_dir():
                gp_name = gp_dir.name
                # Look for parquet files in this GP directory
                parquet_files = list(gp_dir.glob("*.parquet"))
                
                if parquet_files:
                    for parquet_file in parquet_files:
                        print(f"Loading {parquet_file}")
                        df = pd.read_parquet(parquet_file)
                        dfs.append(df)
                    processed_races.append(gp_name)
                else:
                    print(f"No parquet files found for {gp_name}")
        
        # Log summary
        print(f"\n{'='*60}")
        print(f"Season {year} consolidation summary:")
        print(f"Total races processed: {len(processed_races)}")
        print(f"Processed races: {', '.join(sorted(processed_races))}")
        print(f"{'='*60}\n")
        
        if dfs:
            # Efficient concat
            season_df = pd.concat(dfs, ignore_index=True)
            
            output_path = Path(f"{DIRECTORY_PATH}/seasons")
            output_path.mkdir(parents=True, exist_ok=True)
            
            season_df.to_parquet(output_path / f"season_{year}.parquet")
            print(f"Season data saved to {output_path / f'season_{year}.parquet'}")
            print(f"Total rows in season dataset: {len(season_df)}")
        else:
            print("No race data found to concatenate.")

    race_list = get_season_races()

    trigger_races = TriggerDagRunOperator.partial(
            task_id="trigger_race_etl",
            trigger_dag_id="single_race_dag",
            wait_for_completion=True, # Waits for the child DAG to finish
            poke_interval=30,
            reset_dag_run=True, # Allows re-running the same race if needed
            max_active_tis_per_dag=4,  
            failed_states=["failed"],  
            allowed_states=["success", "failed"]
        ).expand(conf=race_list)
    
    trigger_races >> consolidate_season()


example_dag = race_dag()












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

