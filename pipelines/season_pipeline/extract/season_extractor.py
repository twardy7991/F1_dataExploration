import pandas as pd
from fastf1.core import Telemetry
from typing import List
from pathlib import Path

DIRECTORY_PATH = "./cache"

class SeasonExtractor:

    def __init__(self):
        self.directory_path = DIRECTORY_PATH

    def load_season_races_from_pickle(self, season_year): 

        directory = Path(f"{self.directory_path}/{season_year}")
        
        if directory.is_dir():
            for pickle in directory.rglob("*.pkl"):
                yield self.load_race_from_pickle(pickle)
        else:
            raise FileNotFoundError("Season directory is not existing, please create directory and download races with race pipeline")
        
    def load_race_from_pickle(self, pickle_path, gp_name):
        
        try: 
            tel : pd.DataFrame = pd.read_pickle(pickle_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"Telemetry data not found for {gp_name}. Expected file: {pickle_path}")

        telemetry : Telemetry = Telemetry(tel)
        telemetry.has_channel = lambda c: c in telemetry.columns  
        telemetry['Time'] = pd.to_timedelta(telemetry['Time'])
        return telemetry
    
