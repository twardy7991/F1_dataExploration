from pathlib import Path
import logging
from typing import Tuple, List
from typing import Tuple, List
import queue

import threading
import pandas as pd
import numpy as np

import pandas as pd
import fastf1
from fastf1.core import Session, Lap, Telemetry
from airflow.sdk import task

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)

#ugly workaround
gp_file_names = {
        'Saudi Arabia': 'Saudi',
        'Great Britain': 'Great Britain',
        'United States': 'USA',
        'Abu Dhabi': 'Abu Dhabi'
    }

@task(do_xcom_push=True, multiple_outputs=True)
def extract(year : int, gp_name : str, save_path : str, **context):
    
    fastf1.Cache.set_disabled()

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
    
    logger.info("getting race telemetry data")
    race_telemetry = get_session_telemetry(race)
    
    logger.info("getting quali telemetry data")
    quali_telemetry = get_session_telemetry(quali)
    
    race_data = race_data.astype({
        "LapNumber": "Int16",
        "Stint": "Int8",
        "TyreLife": "Int8",
        "Position": "Int8",

        "IsPersonalBest": "boolean",
        "Deleted": "boolean",

        "Driver": "string",
        "DriverNumber": "string",
        "Compound": "string",
        "Team": "string",
        "TrackStatus": "string",
        "DeletedReason": "string",
    })
    
    # data_schema = pa.schema([
    #     pa.field("Time", pa.duration("ns")),

    #     pa.field("Driver", pa.string()),
    #     pa.field("DriverNumber", pa.string()),

    #     pa.field("LapTime", pa.duration("ns")),
    #     pa.field("LapNumber", pa.int16()),
    #     pa.field("Stint", pa.int8()),

    #     pa.field("PitOutTime", pa.duration("ns")),
    #     pa.field("PitInTime", pa.duration("ns")),

    #     pa.field("Sector1Time", pa.duration("ns")),
    #     pa.field("Sector2Time", pa.duration("ns")),
    #     pa.field("Sector3Time", pa.duration("ns")),

    #     pa.field("Sector1SessionTime", pa.duration("ns")),
    #     pa.field("Sector2SessionTime", pa.duration("ns")),
    #     pa.field("Sector3SessionTime", pa.duration("ns")),

    #     pa.field("SpeedI1", pa.float32()),
    #     pa.field("SpeedI2", pa.float32()),
    #     pa.field("SpeedFL", pa.float32()),
    #     pa.field("SpeedST", pa.float32()),

    #     pa.field("IsPersonalBest", pa.bool_()),

    #     pa.field("Compound", pa.string()),
    #     pa.field("TyreLife", pa.int8()),
    #     pa.field("FreshTyre", pa.bool_()),
    #     pa.field("Team", pa.string()),

    #     pa.field("LapStartTime", pa.duration("ns")),
    #     pa.field("LapStartDate", pa.timestamp("ns")),

    #     pa.field("TrackStatus", pa.string()),

    #     pa.field("Position", pa.int8()),

    #     pa.field("Deleted", pa.bool_()),
    #     pa.field("DeletedReason", pa.string()),

    #     pa.field("FastF1Generated", pa.bool_()),
    #     pa.field("IsAccurate", pa.bool_())
    # ])
    
    # race_data_table = pa.Table.from_pandas(
    #     race_data,
    #     schema=data_schema,
    #     preserve_index=False
    # )
    
    # if not race_data_file.exists():
    #     pa.parquet.write_table(race_data_table, race_data_file)
    
    if not race_data_file.exists():
        race_data.to_parquet(race_data_file,         
                            engine="pyarrow")
    
    if not quali_data_file.exists():
        quali_data.to_parquet(quali_data_file,
                              engine="pyarrow")
    
    if not race_telemetry_file.exists():
        race_telemetry.to_parquet(race_telemetry_file,
                                  engine="pyarrow")
    
    if not quali_telemetry_file.exists():
        quali_telemetry.to_parquet(quali_telemetry_file,
                                   engine="pyarrow")
    
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

    logger.debug("returning downloaded data")
    
    return race, quali

## is type return proper?
def get_session_telemetry(session : Session) -> pd.DataFrame:
        
        telemetry_data : List = []
        
        rows = session.laps.shape[0]
        
        lap : Lap
        
        for _, lap in session.laps.iterrows(): 
            try:
                tel: Telemetry = lap.get_telemetry()
                tel['DriverNumber'] = lap['DriverNumber']
                tel['LapNumber'] = lap['LapNumber']
                telemetry_data.append(tel)
                
                if _ % 100 == 0:
                    logger.info(f"{round(_ / rows * 100, 2) } % completed")
                
            except fastf1.core.DataNotLoadedError:
                logger.error(f"WARNING: Telemetry not loaded for driver {lap['DriverNumber']}, lap {lap['LapNumber']}")
                continue
        
        logger.info(f"{100} % completed")
        return pd.concat(telemetry_data, ignore_index=True)
