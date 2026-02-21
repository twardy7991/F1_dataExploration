import pandas as pd
import pytest
from dags.utils import AccelerationComputations, TelemetryProcessing, FuelProcessing

@pytest.fixture
def acceleration_computations():
    return AccelerationComputations()

@pytest.fixture
def fuel_processing():
    return FuelProcessing(lap_df=pd.DataFrame())

@pytest.fixture
def telemetry_processing(acceleration_computations):
    return TelemetryProcessing(data=pd.DataFrame() ,acceleration_computations=acceleration_computations)


    