import time
from pathlib import Path
from race_pipeline.utils import TelemetryProcessing, FuelProcessing, DTWComputations, AccelerationComputations
import pandas as pd
from fastf1.core import Session, Telemetry, Laps
from airflow.sdk import task
import logging

TRACK_INFO = {
    'Bahrain Grand Prix': (5.412, 308.238),
    'Saudi Arabian Grand Prix': (6.174, 308.450),
    'Australian Grand Prix': (5.278, 306.124),
    'Azerbaijan Grand Prix': (6.003, 306.049),
    'Miami Grand Prix': (5.412, 308.326),
    'Monaco Grand Prix': (3.337, 260.286),
    'Spanish Grand Prix': (4.675, 308.424),
    'Canadian Grand Prix': (4.361, 305.270),
    'Austrian Grand Prix': (4.318, 306.452),
    'British Grand Prix': (5.891, 306.198),
    'Hungarian Grand Prix': (4.381, 306.670),
    'Belgian Grand Prix': (7.004, 308.052),
    'Dutch Grand Prix': (4.259, 306.587),
    'Italian Grand Prix': (5.793, 306.720),
    'Singapore Grand Prix': (4.940, 308.706),
    'Japanese Grand Prix': (5.807, 307.471),
    'Qatar Grand Prix': (5.419, 308.611),
    'United States Grand Prix': (5.513, 308.405),
    'Mexico City Grand Prix': (4.304, 305.354),
    'São Paulo Grand Prix': (4.309, 305.879),
    'Las Vegas Grand Prix': (6.201, 305.880),
    'Abu Dhabi Grand Prix': (5.281, 305.355),
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task(do_xcom_push=True, multiple_outputs=True)   
## feels too monolith, probably needs decoupling
def transform(year : int, gp_name : str, save_path : str, **context) -> pd.DataFrame:
    
    save_path : Path = Path(save_path)
    fuel_start : int = 100 

    def get_data(context):

        race_telemetry = context["ti"].xcom_pull(task_ids="extract", key = "race_telemetry_file")
        quali_telemetry = context["ti"].xcom_pull(task_ids="extract", key = "quali_telemetry_file")
        race_data = context["ti"].xcom_pull(task_ids="extract", key = "race_data_file")
        quali_data = context["ti"].xcom_pull(task_ids="extract", key="quali_data_file")

        return (pd.read_parquet(race_telemetry),
                pd.read_parquet(quali_telemetry),
                pd.read_parquet(race_data),
                pd.read_parquet(quali_data))

    # start = time.time()
    # race_session, quali_session = get_session(year=year, gp_name=gp_name) # Reverse order so it matches usage prevoiusly
    # print(f"Session loaded in {time.time() - start:.2f} seconds.")
    
    # Extract telemetry from the loaded session
    # Doesnt work
    # race_telemetry = race_session.car_data

    ### creating telemetry dataframe  #########

    race_telemetry, quali_telemetry, race_laps_df, quali_data = get_data(context) 
    
    if 'Time' in race_telemetry.columns:
        race_telemetry['Time'] = pd.to_timedelta(race_telemetry['Time'])

    ######## ------------------------------------------- ########
    # print(f"Telemetry extracted in {time.time() - start:.2f} seconds.")

    lap_length, track_length = TRACK_INFO[gp_name]
    #race_laps_df : Laps = race_session.laps
    main_cols = ['Driver', 'Team', 'Compound', 'TyreLife', 'LapTime', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'LapNumber', 'DriverNumber']
    
    # drop NA # causing bugs rn
    race_laps_df : pd.DataFrame = race_laps_df[main_cols]
    #lap_df = lap_df[main_cols].dropna()
    
    # start = time.time()
    # -----------FUEL------------ #
    logger.debug(f"race_laps_df driver list : \n{race_laps_df['DriverNumber'].unique()} \n")
    
    fuel_processing : FuelProcessing = FuelProcessing(race_laps_df)
    race_laps_with_fuel = fuel_processing.calculateFCL(lap_length=lap_length, track_length=track_length, fuel_start=fuel_start)
    
    # print(f"Fuel calculations done in {time.time() - start:.2f} seconds.")
    
    # start = time.time()
    # -----------TELEMETRY------------ #
    telemetry_df = pd.DataFrame(race_telemetry)
    
    logger.debug(f"telemetry_df driver list : \n{telemetry_df['DriverNumber'].unique()} \n")

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

    # start = time.time()
    # -----------DTW------------ #
    # dtw_computations = DTWComputations()
    # dist_df = dtw_computations.calculate_dtw(quali_session=quali_session, telemetry_df=telemetry_df, race_session=race_session)
    
    # lap_df = pd.merge(lap_df,dist_df, on=['DriverNumber', 'LapNumber'], how='left')

    #final_cols = ['LapNumber','Driver', 'Compound', 'TyreLife', 'StartFuel', 'FCL', 'LapTime', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'SumLonAcc', 'SumLatAcc', 'MeanLapSpeed', 'LonDistanceDTW', 'LatDistanceDTW']
    
    # -----------FINAL------------ #
    final_cols = ['LapNumber','Driver', 'Compound', 'TyreLife', 'StartFuel', 'FCL', 'LapTime', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'SumLonAcc', 'SumLatAcc', 'MeanLapSpeed']
    processed_df = lap_df[final_cols].reset_index(drop=True)
    
    logger.debug(processed_df.head())
    
    #logging.info(f"DTW calculations done in {time.time() - start:.2f} seconds.")

    out_dir = save_path / str(year) / gp_name
    out_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Output directory: {out_dir}")
    
    out_file = out_dir / "session_dataset.parquet"
    
    logger.info(f"Saving processed data to {out_file}")
    
    processed_df.to_parquet(out_file)

    return {
        "processed_file" : str(out_file)
    }

