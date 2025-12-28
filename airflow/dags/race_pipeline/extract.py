from airflow.sdk import task
from fastf1.core import Session, Lap, Telemetry
import fastf1
from typing import List, Tuple
import pandas as pd
import os


#ugly workaround
gp_file_names = {
        'Saudi Arabia': 'Saudi',
        'Great Britain': 'Great Britain',
        'United States': 'USA',
        'Abu Dhabi': 'Abu Dhabi'
    }

@task(do_xcom_push=True, multiple_outputs=True)
def extract(**context):

    params = context["params"]
    gp_name = params["race_name"]
    year = params["year"]

    if not os.path.exists(f'../../cache/{year}'):
        os.makedirs(f'../../cache/{year}')
    # fastf1.Cache.enable_cache('cache')

    race : Session
    quali : Session

    race, quali = get_session(year=year, gp_name=gp_name)

    race_data = race.laps
    quali_data = quali.laps
    race_telemetry = get_session_telemetry(race)
    quali_telemetry = get_session_telemetry(quali)

    cache_dir_path = f"../../cache/{year}"
    dir_files = os.listdir(cache_dir_path)

    race_data_file = f"../../cache/{year}/{year}_{gp_name}_race_data.parquet"
    if race_data_file not in dir_files:
        race_data.to_parquet(race_data_file) 
    
    quali_data_file = f"../../cache/{year}/{year}_{gp_name}_quali_data.parquet"
    if quali_data_file not in dir_files:
        quali_data.to_parquet(quali_data_file)

    race_telemetry_file = f"../../cache/{year}/{year}_{gp_name}_race_data.parquet"
    if race_telemetry_file not in dir_files:
        race_telemetry.to_parquet(race_telemetry_file)

    quali_telemetry_file = f"../../cache/{year}/{year}_{gp_name}_race_data.parquet"
    if quali_telemetry_file not in dir_files:
        quali_telemetry.to_parquet(quali_telemetry_file)

    r = open("_READY")
    r.close()

    return {
            "quali_data_file" : quali_data_file,
            "race_data_file" : race_data_file,
            "race_telemetry_file" : race_telemetry_file,
            "quali_telemetry_file" : quali_telemetry_file
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
            
            tel : Telemetry = lap.get_telemetry()
            tel['DriverNumber'] = lap['DriverNumber']  
            tel['LapNumber'] = lap['LapNumber']
            telemetry_data.append(tel)

        return pd.concat(telemetry_data, ignore_index=True)
