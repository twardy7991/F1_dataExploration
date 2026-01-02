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

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)

@dag(
    dag_id = "season_dag",
    schedule = None,
    start_date=datetime(2021, 1, 1),
    catchup = False,
    params = {"year" : Param(2023, type="integer", minimum=2020)}
)
def race_dag():

    @task
    def get_season_races(**context) -> List[dict]:
        year = context["params"]["year"]

        schedule = fastf1.get_event_schedule(2023)
        
        ### RN FOR 3 RACES ONLY ###
        schedule = schedule[schedule['EventName'].str.contains('Grand Prix')]['EventName'].to_list()[:3]
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
            logger.error(f"Directory {source_dir} does not exist. No data to load.")
            return

        # Iterate through each GP directory
        for gp_dir in source_dir.iterdir():
            if gp_dir.is_dir():
                gp_name = gp_dir.name
                # Look for parquet files in this GP directory
                parquet_files = list(gp_dir.glob("*.parquet"))
                
                if parquet_files:
                    for parquet_file in parquet_files:
                        logger.info(f"Loading {parquet_file}")
                        df = pd.read_parquet(parquet_file, 
                                             engine="pyarrow",
                                             dtype_backend="pyarrow")
                        dfs.append(df)
                    processed_races.append(gp_name)
                else:
                    logger.error(f"No parquet files found for {gp_name}")
        
        logger.info(f"""\n{'='*60} \n
                Season {year} consolidation summary:\n
                Total races processed: {len(processed_races)}\n
                Processed races: {', '.join(sorted(processed_races))}\n
                {'='*60}\n""")
        
        if dfs:
            season_df = pd.concat(dfs, ignore_index=True)
            
            output_path = Path(f"{DIRECTORY_PATH}/seasons")
            output_path.mkdir(parents=True, exist_ok=True)
            
            season_df.to_parquet(output_path / f"season_{year}.parquet")
            logger.info(f"""Season data saved to {output_path / f'season_{year}.parquet'}\n
                        Total rows in season dataset: {len(season_df)}""")
        else:
            logger.error("No race data found to concatenate.")

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