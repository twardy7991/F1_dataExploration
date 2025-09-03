import pandas as pd
import fastf1
from fastf1.core import Session, Lap, Telemetry
from typing import List

def get_session_telemetry():
    
    year = 2023
    gp_name = 'Saudi'
    session_type = 'Q'
    telemetry =  True
    messages = False
    weather = False
    
    session = fastf1.get_session(year=year, gp=gp_name, identifier=session_type)
        
    session.load(telemetry=telemetry, messages=messages, weather=weather)
    
    telemetry_data : List = []
    
    lap : Lap
    
    for _, lap in session.laps.iterrows():
        
        tel : Telemetry = lap.get_telemetry()
        tel['DriverNumber'] = lap['DriverNumber']  
        tel['LapNumber'] = lap['LapNumber']
        telemetry_data.append(tel)

    telemetry_df =  pd.concat(telemetry_data, ignore_index=True)
    
    pd.to_pickle(telemetry_df, "./test_telemetry_session_data_2023_Saudi_Q.pkl")
    
if __name__ == "__main__":
    get_session_telemetry()