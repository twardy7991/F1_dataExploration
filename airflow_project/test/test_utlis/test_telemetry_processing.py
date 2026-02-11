import pandas as pd
from dags.utils import TelemetryProcessing
from pytest_mock import MockerFixture

from pyspark.testing.utils import assertDataFrameEqual
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *

@pytest.fixture
def spark_test():
    s = SparkSession.builder.appName("Test_pyspark").getOrCreate()
    yield s

def test_divide_column_by_sign(telemetry_processing : TelemetryProcessing, spark_test : SparkSession):
    
    df = spark_test.createDataFrame([[1],
                       [0],
                       [-1],
                       [1],
                       [-1]], schema = ["to_divide"])
    
    result = telemetry_processing._divide_column_by_sign(df, "to_divide")
    
    expected_result = spark_test.createDataFrame([[1,1,0],
                                    [0,0,0],
                                    [-1,0,-1],
                                    [1,1,0],
                                    [-1,0,-1]], 
                                    schema = ["to_divide", "positive_to_divide", "negative_to_divide"])

    assertDataFrameEqual(result,expected_result)
    
def test_normalize_drs(telemetry_processing : TelemetryProcessing, spark_test : SparkSession):
    
    df = spark_test.createDataFrame([[12],
                    [10],
                    [14],
                    [1],
                    [20],
                    [10000],
                    [-10000]], schema = ["DRS"])
    
    expected_result = spark_test.createDataFrame([[1],
                                    [1],
                                    [1],
                                    [0],
                                    [0],
                                    [0],
                                    [0]], StructType([StructField('DRS', IntegerType(), False)]))
                        
    telemetry_processing.data = df
    
    telemetry_processing.normalize_drs()
    
    assertDataFrameEqual(telemetry_processing.data,expected_result)

def test_calculate_mean_lap_speed(telemetry_processing : TelemetryProcessing, spark_test : SparkSession):
    
    df = spark_test.createDataFrame([[1, 1,  50],
                       [2, 1,   0],
                       [1, 1, 100],
                       [2, 1, 100],
                       [1, 2, 200],
                       [2, 2, 150],
                       [1, 2, 200],
                       [2, 2, 300]], 
                       schema = ["DriverNumber","LapNumber","Speed"])
    
    expected_result = spark_test.createDataFrame([[1, 1,  50, 75.0],
                                    [2, 1,   0, 50.0],
                                    [1, 1, 100, 75.0],
                                    [2, 1, 100, 50.0],
                                    [1, 2, 200, 200.0],
                                    [2, 2, 150, 225.0],
                                    [1, 2, 200, 200.0],
                                    [2, 2, 300, 225.0]], 
                                    schema = StructType(
                                                [StructField('DriverNumber', LongType(), True),
                                                StructField('LapNumber', LongType(), True), 
                                                StructField('Speed', LongType(), True), 
                                                StructField('MeanLapSpeed', DoubleType(), True)]
                                                )
                                    )
    
    telemetry_processing.data = df
    telemetry_processing.calculate_mean_lap_speed()
    
    assertDataFrameEqual(telemetry_processing.data, expected_result)
    
## too much overhead rn
def test_calculate_accelerations(telemetry_processing : TelemetryProcessing, mocker : MockerFixture):
    pass    
    # acceleration_computations = AccelerationComputations()
    
    # mocker.patch(acceleration_computations.compute_accelerations, return_value="")
    
    # telemetry_processing.acceleration_computations = 

def test_calculate_lap_progress(telemetry_processing : TelemetryProcessing, spark_test : SparkSession):
    
    from datetime import timedelta
    
    df = spark_test.createDataFrame([
        [1, 1, timedelta(seconds=0)],
        [2, 1, timedelta(seconds=0)],
        [1, 1, timedelta(minutes=1)],
        [2, 1, timedelta(minutes=1)], 
        [1, 2, timedelta(minutes=2)],
        [2, 2, timedelta(minutes=2)],
        [1, 2, timedelta(minutes=3)],
        [2, 2, timedelta(minutes=3)],
        [1, 2, timedelta(minutes=4)],
        [2, 2, timedelta(minutes=4)]
    ], schema=StructType([
        StructField('DriverNumber', LongType(), True),
        StructField('LapNumber', LongType(), True), 
        StructField('Time', DayTimeIntervalType(), True)
    ])) 

    expected_result =  spark_test.createDataFrame([
        [1, 1, timedelta(seconds=0), 0.5],
        [2, 1, timedelta(seconds=0), 0.5],

        [1, 1, timedelta(minutes=1), 1.0],
        [2, 1, timedelta(minutes=1), 1.0],

        [1, 2, timedelta(minutes=2), 1/3],
        [2, 2, timedelta(minutes=2), 1/3],

        [1, 2, timedelta(minutes=3), 2/3],
        [2, 2, timedelta(minutes=3), 2/3],

        [1, 2, timedelta(minutes=4), 1.0],
        [2, 2, timedelta(minutes=4), 1.0],
    ], 
        schema = ["DriverNumber","LapNumber", "Time", "LapProgress"])
    
    telemetry_processing.data = df
    telemetry_processing.calculate_lap_progress()
    
    assertDataFrameEqual(telemetry_processing.data,expected_result)
        
def test_get_single_lap_data(telemetry_processing : TelemetryProcessing, spark_test : SparkSession):
    
    df = spark_test.createDataFrame([
            [1, 1, 123],
            [2, 1, 345],
            [1, 1, 654],
            [2, 1, 444], 
            [1, 2, 789],
            [2, 2, 123],
            [1, 2, 321],
            [2, 2, 356],
            [1, 2, 555],
            [2, 2, 999]
            ], 
            schema = ["DriverNumber","LapNumber", "some_data"])

    expected_result = spark_test.createDataFrame([
            [1, 1, 123],
            [1, 2, 789], 
            [2, 1, 345],
            [2, 2, 123]
            ], 
            schema = ["DriverNumber","LapNumber", "some_data"])
    
    
    telemetry_processing.data = df
    final_df = telemetry_processing.get_single_lap_data()
    
    assertDataFrameEqual(final_df, expected_result)