from race_pipeline.utils import TelemetryProcessing, FuelProcessing, DTWComputations, AccelerationComputations
import pandas as pd
from fastf1.core import Session, Telemetry, Laps
from airflow.decorators import task

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


@task   
## feels too monolith, probably needs decoupling
def build_session_dataset(gp_name : str, race_session : Session, quali_session : Session, race_telemetry : Telemetry, fuel_start : int = 100) -> pd.DataFrame:
    
    lap_length, track_length = TRACK_INFO[gp_name]
    race_laps_df : Laps = race_session.laps
    main_cols = ['Driver', 'Team', 'Compound', 'TyreLife', 'LapTime', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'LapNumber', 'DriverNumber', 'LapNumber']
    
    # drop NA # causing bugs rn
    race_laps_df : pd.DataFrame = race_laps_df[main_cols]
    #lap_df = lap_df[main_cols].dropna()
    
    # -----------FUEL------------ #
    fuel_processing : FuelProcessing = FuelProcessing(race_laps_df)
    fuel_processing.calculateFCL(lap_length=lap_length, track_length=track_length, fuel_start=fuel_start)

    # -----------TELEMETRY------------ #
    telemetry_df : Telemetry = race_telemetry

    tel_processing = TelemetryProcessing(telemetry_df, acceleration_computations=AccelerationComputations())
    tel_processing.calculate_mean_lap_speed()
    tel_processing.calculate_accelerations()
    tel_processing.normalize_drs()
    
    lap_telemetry = tel_processing.get_single_lap_data()
    
    lap_df = pd.merge(
        race_laps_df,
        lap_telemetry,
        on=['DriverNumber', 'LapNumber'],
        how='left'
    )

    # -----------DTW------------ #
    dtw_computations = DTWComputations()
    dist_df = dtw_computations.calculate_dtw(quali_session=quali_session, telemetry_df=telemetry_df)
    
    lap_df = pd.merge(lap_df,dist_df, on=['DriverNumber', 'LapNumber'], how='left')

    final_cols = ['LapNumber','Driver', 'Compound', 'TyreLife', 'StartFuel', 'FCL', 'LapTime', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'SumLonAcc', 'SumLatAcc', 'MeanLapSpeed', 'LonDistanceDTW', 'LatDistanceDTW']
    processed_df = lap_df[final_cols].reset_index(drop=True)
    
    print(processed_df.head())
    print('Shape:', processed_df.shape)
    
    return processed_df

