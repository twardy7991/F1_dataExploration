from airflow_project.dags.race_pipeline.tasks.load import load

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from pytest_mock.plugin import MockerFixture
from airflow.models import Connection
from airflow.utils import db

import sqlalchemy 
from sqlalchemy.engine import Engine
from contextlib import contextmanager

import os

conn_uri = "postgresql+psycopg2://postgres:postgres@localhost:5452/test_f1_db"

def create_connection() -> Connection:
    return Connection(
            conn_id="test_database", 
            conn_type="postgres",
            host="postgresql+psycopg2",
            schema="test_f1_db",
            login="airflow",
            password="airflow",
            port=5432
            )

@contextmanager
def get_conn():
    engine = sqlalchemy.create_engine(conn_uri)
    conn = engine.connect()

    try:
        yield conn
    finally:
        conn.close()
    
def test_load(mocker : MockerFixture, monkeypatch):
    
    ti = mocker.MagicMock()
    
    # Mock the get_connection to avoid database lookup
    test_conn = Connection(
        conn_id="POSTGRES_TEST_CONN",
        conn_type="postgresql",
        host="localhost",
        schema="test_f1_db",
        login="postgres",
        password="postgres",
        port=5452
    )
    
    print(test_conn.get_uri())
    
    mocker.patch('airflow.sdk.bases.hook.BaseHook.get_connection', return_value=test_conn)
    
    def xcom_pull_side_effect(*, task_ids=None, key=None, **_):
        xcom_map = {
            ("transform", "processed_file") : "airflow_project/test/data/processed/Bahrain Grand Prix/session_dataset.parquet",
        }
        return xcom_map[(task_ids, key)]
    
    # patching the xcom_pull method with monkey function
    ti.xcom_pull.side_effect = xcom_pull_side_effect
    
    context = {"ti" : ti,
        "year" : 2023,
        "gp_name" :"Bahrain Grand Prix",
        "conn_id" : "POSTGRES_TEST_CONN",
        "load_type" : "replace"
    }

    result = load.function(
        **context
    )
    
    with get_conn() as conn:
        data_created = pd.read_sql(
            "select * from session_data",
            dtype_backend="pyarrow",
            con=conn
        )
    
    data_expected = pd.read_parquet("airflow_project/test/data/processed/Bahrain Grand Prix/session_dataset.parquet",
                                engine="pyarrow", 
                                dtype_backend="pyarrow"
                            )
    
    data_expected["Year"] = 2023
    data_expected["Race"] = "Bahrain Grand Prix"
    data_expected.columns = [col.lower() for col in data_expected.columns]
    
    data_expected = data_expected.reindex(sorted(data_expected.columns), axis=1)
    
    data_expected.reset_index(drop=True, inplace=True)
    data_created.reset_index(drop=True, inplace=True)
    
    print("data_expected",
        data_expected[
            (data_expected["driver"] == "LEC") &
            (data_expected["lapnumber"] == 40)
        ]["laptime"]
    )
    
    print("data_created",
        data_created[
            (data_created["driver"] == "LEC") &
            (data_created["lapnumber"] == 40)
        ]["laptime"]
    )
    
    print("\nexpected" , data_expected[data_expected["laptime"].isna()])
    print("\ncreated", data_created[data_created["laptime"].isna()])
    
    assert_frame_equal(data_created, data_expected, check_dtype=False)
    
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