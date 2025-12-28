import time
from race_pipeline.extract import get_session
from race_pipeline.utils import TelemetryProcessing, FuelProcessing, DTWComputations, AccelerationComputations
import pandas as pd
from fastf1.core import Session, Telemetry, Laps
from airflow.sdk import task

TRACK_INFO = {
    'Bahrain': (5.412, 308.238),
    'Saudi Arabia': (6.174, 308.450),
    'Australia': (5.278, 306.124),
    'Azerbaijan': (6.003, 306.049),
    'Miami': (5.412, 308.326),
    'Monaco': (3.337, 260.286),
    'Spain': (4.675, 308.424),
    'Canada': (4.361, 305.270),
    'Austria': (4.318, 306.452),
    'Great Britain': (5.891, 306.198),
    'Hungary': (4.381, 306.670),
    'Belgium': (7.004, 308.052),
    'Netherlands': (4.259, 306.587),
    'Italy': (5.793, 306.720),
    'Singapore': (4.940, 308.706),
    'Japan': (5.807, 307.471),
    'Qatar': (5.419, 308.611),
    'United States': (5.513, 308.405),
    'Mexico': (4.304, 305.354),
    'Brazil': (4.309, 305.879),
    'Las Vegas': (6.201, 305.880),
    'Abu Dhabi': (5.281, 305.355),
}

def get_data(context):

    race_telemetry = context["ti"].xcom_pull(task_id="extract", key = "race_telemetry")
    quali_telemetry = context["ti"].xcom_pull(task_id="extract", key = "quali_telemetry")
    race_data = context["ti"].xcom_pull(task_id="extract", key = "race_data")
    quali_data = context["ti"].xcom_pull(task_id = "extract", key="quali_data")

    return (pd.read_parquet(race_telemetry),
            pd.read_parquet(quali_telemetry),
            pd.read_parquet(race_data),
            pd.read_parquet(quali_data))

@task   
## feels too monolith, probably needs decoupling
def build_session_dataset(**context) -> pd.DataFrame:
    
    race_telemetry = get_data()

    params = context["params"]
    gp_name : str = params["gp_name"]
    year : int = params["year"] 
    fuel_start : int = 100 

    #start = time.time()
    #race_session, quali_session = get_session(year=year, gp_name=gp_name) # Reverse order so it matches usage prevoiusly
    print(f"Session loaded in {time.time() - start:.2f} seconds.")
    
    # Extract telemetry from the loaded session
    # Doesnt work
    # race_telemetry = race_session.car_data

    ### creating telemetry dataframe  #########

    race_telemetry, quali_telemetry, race_laps_df. quali_data = get_data(context) 
    
    if 'Time' in race_telemetry.columns:
        race_telemetry['Time'] = pd.to_timedelta(race_telemetry['Time'])

    ######## ------------------------------------------- ########
    print(f"Telemetry extracted in {time.time() - start:.2f} seconds.")

    lap_length, track_length = TRACK_INFO[gp_name]
    #race_laps_df : Laps = race_session.laps
    main_cols = ['Driver', 'Team', 'Compound', 'TyreLife', 'LapTime', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'LapNumber', 'DriverNumber']
    
    # drop NA # causing bugs rn
    race_laps_df : pd.DataFrame = race_laps_df[main_cols]
    #lap_df = lap_df[main_cols].dropna()
    
    start = time.time()
    # -----------FUEL------------ #
    fuel_processing : FuelProcessing = FuelProcessing(race_laps_df)
    race_laps_with_fuel = fuel_processing.calculateFCL(lap_length=lap_length, track_length=track_length, fuel_start=fuel_start)
    
    print(f"Fuel calculations done in {time.time() - start:.2f} seconds.")
    
    start = time.time()
    # -----------TELEMETRY------------ #
    telemetry_df = pd.DataFrame(race_telemetry)

    tel_processing = TelemetryProcessing(telemetry_df, acceleration_computations=AccelerationComputations())
    tel_processing.calculate_mean_lap_speed()
    tel_processing.calculate_accelerations()
    tel_processing.normalize_drs()
    
    lap_telemetry = tel_processing.get_single_lap_data()
    
    lap_df = pd.merge(
        race_laps_with_fuel,
        lap_telemetry,
        on=['DriverNumber', 'LapNumber'],
        how='left'
    )

    print(f"Telemetry calculations done in {time.time() - start:.2f} seconds.")


    start = time.time()
    # -----------DTW------------ #
    dtw_computations = DTWComputations()
    dist_df = dtw_computations.calculate_dtw(quali_session=quali_session, telemetry_df=telemetry_df, race_session=race_session)
    
    lap_df = pd.merge(lap_df,dist_df, on=['DriverNumber', 'LapNumber'], how='left')

    final_cols = ['LapNumber','Driver', 'Compound', 'TyreLife', 'StartFuel', 'FCL', 'LapTime', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'SumLonAcc', 'SumLatAcc', 'MeanLapSpeed', 'LonDistanceDTW', 'LatDistanceDTW']
    processed_df = lap_df[final_cols].reset_index(drop=True)
    
    print(processed_df.head())
    print('Shape:', processed_df.shape)
    
    print(f"DTW calculations done in {time.time() - start:.2f} seconds.")

    return pd.DataFrame(processed_df)

