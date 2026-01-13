from airflow_project.dags.race_pipeline.tasks.load import load
from airflow_project.plugins.hooks.my_postgres_hook import MyPostgresHook
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from pytest_mock.plugin import MockerFixture
from airflow.sdk import Connection

import os

def create_connection() -> None:
    conn_uri = "postgresql+psycopg2://postgres:postgres@localhost:5452/test_f1_db"
    return Connection(uri=conn_uri)
    
def test_load(mocker : MockerFixture, monkeypatch):
    
    ti = mocker.MagicMock()
    
    def mock_get_conn(*args, **kwargs):
        return create_connection(MyPostgresHook, "get_conn", mock_get_conn)
    
    monkeypatch.setattr()
    
    def xcom_pull_side_effect(*, task_ids=None, key=None, **_):
        xcom_map = {
            ("transform", "processed_file") : "airflow_project/test/data/processed/Bahrain Grand Prix/session_dataset.parquet",
        }
        return xcom_map[(task_ids, key)]
    
    # patching the xcom_pull method with money function
    ti.xcom_pull.side_effect = xcom_pull_side_effect
    
    context = {"ti" : ti,
        "year" : 2023,
        "gp_name" :"Bahrain Grand Prix",
    }

    result = load.function(
        **context
    )
    
    print(result)
    

        
        
# def test_get_session(race_extractor : RaceExtractor):
    
#     year = 2023
#     gp_name = 'Saudi'
#     session_type = 'Q'
    
#     target_response = fastf1.get_session(year=year, gp=gp_name, identifier=session_type)
#     target_response.load(telemetry=True, messages=False, weather=False)
    
#     response = race_extractor.get_session(year=year, gp_name=gp_name, session_type=session_type)
    
#     assert isinstance(response, Session) 
#     assert target_response.laps.equals(response.laps)

# def test_get_session_telemetry(race_extractor : RaceExtractor):
    
#     year = 2023
#     gp_name = 'Bahrain'
#     session_type = 'Q'
    
#     import os
#     print(os.getcwd())
    
#     target_response : pd.DataFrame = pd.read_pickle("test/data/Bahrain Grand Prix/2023_Bahrain Grand Prix_quali_data.parquet")
    
#     session = race_extractor.get_session(year=year, gp_name=gp_name, session_type=session_type)
#     response = race_extractor.get_session_telemetry(session=session)
    
#     assert isinstance(response, pd.DataFrame)
#     assert response.equals(target_response)