from airflow.decorators import task
from fastf1.core import Session, Lap, Telemetry
import fastf1
from typing import List
import pandas as pd

#ugly workaround
gp_file_names = {
        'Saudi Arabia': 'Saudi',
        'Great Britain': 'Great Britain',
        'United States': 'USA',
        'Abu Dhabi': 'Abu Dhabi'
    }

@task 
def get_session(year : int = 2023,
                    gp_name : str = "Saudi Arabia", 
                    session_type : str | int = "q", 
                    ) -> Session:
    
    session : Session    
        
    #left constant for now, could be made flexible with future development
    telemetry : bool = True
    messages : bool = False
    weather : bool = False
    
    gp_name = gp_file_names.get(gp_name, gp_name)
    
    if gp_name.isdigit():
        session = fastf1.get_session(year=year, gp=int(gp_name), identifier=session_type)
    else:
        session = fastf1.get_session(year=year, gp=gp_name, identifier=session_type)
    
    session.load(telemetry=telemetry, messages=messages, weather=weather)
    
    return session

## is type return proper?
@task
def get_session_telemetry(session : Session) -> pd.DataFrame:
        
        telemetry_data : List = []
        
        lap : Lap
        
        for _, lap in session.laps.iterrows():
            
            tel : Telemetry = lap.get_telemetry()
            tel['DriverNumber'] = lap['DriverNumber']  
            tel['LapNumber'] = lap['LapNumber']
            telemetry_data.append(tel)

        return pd.concat(telemetry_data, ignore_index=True)
