import pandas as pd
import fastf1
from fastf1.core import Session, Lap, Telemetry
from typing import List

"""
Available GP names:

Bahrain, Saudi, Australia, Azerbaijan, Miami, 
Monaco, Spain, Canada, Austria, Great Britain, 
Hungary, Belgium, Netherlands, Italy, Singapore, Japan, 
Qatar, USA, Mexico, Brazil, Las Vegas, Abu Dhabi
"""

# for differences between fastapi and us
gp_file_names = {
        'Saudi Arabia': 'Saudi',
        'Great Britain': 'Great Britain',
        'United States': 'USA',
        'Abu Dhabi': 'Abu Dhabi'
    }

class RaceExtractor:

    def __init__(self):
        pass

    def get_session(self, year : int,
                    gp_name : str, 
                    session_type : str | int, 
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
    def get_session_telemetry(self, session : Session) -> pd.DataFrame:
        
        telemetry_data : List = []
        
        lap : Lap
        
        for _, lap in session.laps.iterrows():
            
            tel : Telemetry = lap.get_telemetry()
            tel['DriverNumber'] = lap['DriverNumber']  
            tel['LapNumber'] = lap['LapNumber']
            telemetry_data.append(tel)

        return pd.concat(telemetry_data, ignore_index=True)
    
