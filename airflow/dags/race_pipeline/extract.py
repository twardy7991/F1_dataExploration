from fastf1.core import Session, Lap, Telemetry
from airflow.sdk import task
from pathlib import Path
import pandas as pd
import fastf1
from typing import Dict, Tuple, List
import os

#ugly workaround
gp_file_names = {
        'Saudi Arabia': 'Saudi',
        'Great Britain': 'Great Britain',
        'United States': 'USA',
        'Abu Dhabi': 'Abu Dhabi'
    }

@task(do_xcom_push=True, multiple_outputs=True)
def extract(year : int, gp_name : str, save_path : str, **context):

    save_path = Path(save_path)
    cache_dir_path = save_path / str(year) / gp_name
    cache_dir_path.mkdir(parents=True, exist_ok=True)
    
    race_data_file = cache_dir_path / f"{year}_{gp_name}_race_data.parquet"
    quali_data_file = cache_dir_path / f"{year}_{gp_name}_quali_data.parquet"
    race_telemetry_file = cache_dir_path / f"{year}_{gp_name}_race_telemetry.parquet"
    quali_telemetry_file = cache_dir_path / f"{year}_{gp_name}_quali_telemetry.parquet"
    
    # Jeśli wszystkie używamy naszego cache'a
    all_files_exist = all([
        race_data_file.exists(),
        quali_data_file.exists(),
        race_telemetry_file.exists(),
        quali_telemetry_file.exists()
    ])

    if all_files_exist:
        return {
            "quali_data_file": str(quali_data_file),
            "race_data_file": str(race_data_file),
            "race_telemetry_file": str(race_telemetry_file),
            "quali_telemetry_file": str(quali_telemetry_file)
        }
    
    race : Session
    quali : Session
    # TODO: obsługa błędów (pustych) i ładowanie tylko brakujących plików
    race, quali = get_session(year=year, gp_name=gp_name)
    
    race_data : pd.DataFrame = race.laps 
    quali_data : pd.DataFrame = quali.laps
    race_telemetry = get_session_telemetry(race)
    quali_telemetry = get_session_telemetry(quali)
    
    if not race_data_file.exists():
        race_data.to_parquet(race_data_file)
    
    if not quali_data_file.exists():
        quali_data.to_parquet(quali_data_file)
    
    if not race_telemetry_file.exists():
        race_telemetry.to_parquet(race_telemetry_file)
    
    if not quali_telemetry_file.exists():
        quali_telemetry.to_parquet(quali_telemetry_file)
    
    return {
        "quali_data_file": str(quali_data_file),
        "race_data_file": str(race_data_file),
        "race_telemetry_file": str(race_telemetry_file),
        "quali_telemetry_file": str(quali_telemetry_file)
    }

def get_session(year : int = 2023,
                    gp_name : str = "Saudi Arabia", 
                    session_type : str | int = None, # we want both Race and Quali, workaround for now 
                    ) -> Tuple[Session, Session]:
    
    #session : Session    
        
    #left constant for now, could be made flexible with future development
    telemetry : bool = True
    messages : bool = False
    weather : bool = False
    
    gp_name = gp_file_names.get(gp_name, gp_name)
    
    if gp_name.isdigit():
        quali = fastf1.get_session(year=year, gp=int(gp_name), identifier='Q')
        race = fastf1.get_session(year=year, gp=int(gp_name), identifier='R')

    else:
        quali = fastf1.get_session(year=year, gp=gp_name, identifier="Q")
        race = fastf1.get_session(year=year, gp=gp_name, identifier="R")
    
    quali.load(telemetry=telemetry, messages=messages, weather=weather)
    race.load(telemetry=telemetry, messages=messages, weather=weather)

    return race, quali

## is type return proper?
def get_session_telemetry(session : Session) -> pd.DataFrame:
        
        telemetry_data : List = []
        
        lap : Lap
        
        for _, lap in session.laps.iterrows():
            try:
                tel: Telemetry = lap.get_telemetry()
                tel['DriverNumber'] = lap['DriverNumber']
                tel['LapNumber'] = lap['LapNumber']
                telemetry_data.append(tel)
            except fastf1.core.DataNotLoadedError:
                print(f"WARNING: Telemetry not loaded for driver {lap['DriverNumber']}, lap {lap['LapNumber']}")
                continue
