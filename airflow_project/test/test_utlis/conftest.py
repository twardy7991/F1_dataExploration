import pytest
from airflow_.utils.fuel_processing import FuelProcessing
from airflow_.utils.telemetry_processing import TelemetryProcessing
from airflow_.utils.acceleration_computations import AccelerationComputations
import pandas as pd


@pytest.fixture
def acceleration_computations():
    return AccelerationComputations()

@pytest.fixture
def fuel_processing():
    return FuelProcessing(lap_df=pd.DataFrame())

@pytest.fixture
def telemetry_processing(acceleration_computations):
    return TelemetryProcessing(data=pd.DataFrame() ,acceleration_computations=acceleration_computations)


    