import pytest
from pipelines.race_pipeline.transform.utils.fuel_processing import FuelProcessing
from pipelines.race_pipeline.transform.utils.telemetry_processing import TelemetryProcessing
from pipelines.race_pipeline.transform.utils.acceleration_computations import AccelerationComputations
import pandas as pd


@pytest.fixture
def acceleration_computations():
    return AccelerationComputations()

@pytest.fixture
def fuel_processing():
    return FuelProcessing()

@pytest.fixture
def telemetry_processing(acceleration_computations):
    return TelemetryProcessing(data=pd.DataFrame() ,acceleration_computations=acceleration_computations)


    