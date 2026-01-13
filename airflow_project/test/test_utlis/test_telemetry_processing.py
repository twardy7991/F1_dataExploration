import pandas as pd
from pipelines.race_pipeline.transform.utils.telemetry_processing import TelemetryProcessing
from pipelines.race_pipeline.transform.utils.acceleration_computations import AccelerationComputations
from pytest_mock import mocker
from pytest_mock import MockerFixture
from pandas.testing import assert_frame_equal

def test_divide_column_by_sign(telemetry_processing : TelemetryProcessing):
    
    df = pd.DataFrame([[1],
                       [0],
                       [-1],
                       [1],
                       [-1]], columns = ["to_divide"])
    
    result : pd.DataFrame = telemetry_processing._divide_column_by_sign(df, "to_divide")
    
    expected_result = pd.DataFrame([[1,1,0],
                                    [0,0,0],
                                    [-1,0,-1],
                                    [1,1,0],
                                    [-1,0,-1]], 
                                    columns = ["to_divide", "positive_to_divide", "negative_to_divide"])
    
    assert isinstance(result, pd.DataFrame)
    assert_frame_equal(result,expected_result)
    
def test_normalize_drs(telemetry_processing : TelemetryProcessing):
    
    df = pd.DataFrame([[12],
                    [10],
                    [14],
                    [1],
                    [20],
                    [10000],
                    [-10000]], columns = ["DRS"])
    
    expected_result = pd.DataFrame([[1],
                                    [1],
                                    [1],
                                    [0],
                                    [0],
                                    [0],
                                    [0]], columns = ["DRS"])
                        
    telemetry_processing.data = df
    
    telemetry_processing.normalize_drs()
    
    assert isinstance(telemetry_processing.data, pd.DataFrame)
    assert_frame_equal(telemetry_processing.data,expected_result)

def test_calculate_mean_lap_speed(telemetry_processing : TelemetryProcessing):
    
    df = pd.DataFrame([[1, 1,  50],
                       [2, 1,   0],
                       [1, 1, 100],
                       [2, 1, 100],
                       [1, 2, 200],
                       [2, 2, 150],
                       [1, 2, 200],
                       [2, 2, 300]], 
                       columns = ["DriverNumber","LapNumber","Speed"])
    
    expected_result = pd.DataFrame([[1, 1,  50, 75],
                                    [2, 1,   0, 50],
                                    [1, 1, 100, 75],
                                    [2, 1, 100, 50],
                                    [1, 2, 200, 200],
                                    [2, 2, 150, 225],
                                    [1, 2, 200, 200],
                                    [2, 2, 300, 225]], 
                                    columns = ["DriverNumber","LapNumber","Speed","MeanLapSpeed"])
    expected_result["MeanLapSpeed"] = expected_result["MeanLapSpeed"].astype('float64')
    
    telemetry_processing.data = df
    telemetry_processing.calculate_mean_lap_speed()
    
    assert isinstance(telemetry_processing.data, pd.DataFrame)
    assert_frame_equal(telemetry_processing.data, expected_result)
    
## too much overhead rn
def test_calculate_accelerations(telemetry_processing : TelemetryProcessing, mocker : MockerFixture):
    pass    
    # acceleration_computations = AccelerationComputations()
    
    # mocker.patch(acceleration_computations.compute_accelerations, return_value="")
    
    # telemetry_processing.acceleration_computations = 

def test_calculate_lap_progress(telemetry_processing : TelemetryProcessing):

    df = pd.DataFrame([
    [1, 1],
    [2, 1],
    [1, 1],
    [2, 1], 
    [1, 2],
    [2, 2],
    [1, 2],
    [2, 2],
    [1, 2],
    [2, 2]
    ], 
    columns = ["DriverNumber","LapNumber"])

    expected_result =  pd.DataFrame([
        [1, 1, 0.5],
        [2, 1, 0.5],
        [1, 1,   1],
        [2, 1,   1],
        [1, 2, 1/3],
        [2, 2, 1/3],
        [1, 2, 2/3],
        [2, 2, 2/3],
        [1, 2,   1],
        [2, 2,   1]
        ], 
        columns = ["DriverNumber","LapNumber","LapProgress"])
    
    telemetry_processing.data = df
    telemetry_processing.calculate_lap_progress()
    
    assert isinstance(telemetry_processing.data, pd.DataFrame)
    assert_frame_equal(telemetry_processing.data,expected_result)
        
def test_get_single_lap_data(telemetry_processing : TelemetryProcessing):
    
    df = pd.DataFrame([
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
            columns = ["DriverNumber","LapNumber", "some_data"])

    expected_result = pd.DataFrame([
            [1, 1, 654],
            [1, 2, 555], 
            [2, 1, 444],
            [2, 2, 999]
            ], 
            columns = ["DriverNumber","LapNumber", "some_data"])
    
    ## not good, probably need to change the function
    expected_result["LapNumber"] = expected_result["LapNumber"].astype('object')
    expected_result["DriverNumber"] = expected_result["DriverNumber"].astype('object')
    expected_result["some_data"] = expected_result["some_data"].astype('object')
    
    telemetry_processing.data = df
    final_df : pd.DataFrame = telemetry_processing.get_single_lap_data()

    final_df.reset_index(drop=True, inplace=True)
    
    assert isinstance(final_df, pd.DataFrame)
    assert_frame_equal(final_df, expected_result)