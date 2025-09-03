from pipelines.race_pipeline.extract.extractor import RaceExtractor

import fastf1
import pandas as pd

from fastf1.core import Session

def test_get_session(race_extractor : RaceExtractor):
    
    year = 2023
    gp_name = 'Saudi'
    session_type = 'Q'
    
    target_response = fastf1.get_session(year=year, gp=gp_name, identifier=session_type)
    target_response.load(telemetry=True, messages=False, weather=False)
    
    response = race_extractor.get_session(year=year, gp_name=gp_name, session_type=session_type)
    
    assert isinstance(response, Session) 
    assert target_response.laps.equals(response.laps)

def test_get_session_telemetry(race_extractor : RaceExtractor):
    
    year = 2023
    gp_name = 'Saudi'
    session_type = 'Q'
    
    target_response : pd.DataFrame = pd.read_pickle("test_unit/test_race/test_data/test_telemetry_session_data_2023_Saudi_Q.pkl")
    
    session = race_extractor.get_session(year=year, gp_name=gp_name, session_type=session_type)
    response = race_extractor.get_session_telemetry(session=session)
    
    assert isinstance(response, pd.DataFrame)
    assert response.equals(target_response)