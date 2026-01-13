import pandas as pd
import numpy as np
from dtaidistance import dtw
from fastf1.core import Session, Lap, Laps

from .telemetry_processing import TelemetryProcessing
from .acceleration_computations import AccelerationComputations

class DTWComputations:
    
    def __init__(self):
        pass
    
    def calculate_dtw(self, quali_session : Session, telemetry_df : pd.DataFrame, race_session : Session) -> pd.DataFrame:
        
        ref_singnal_df = self._calculate_reference_df(telemetry_df=telemetry_df, quali_session=quali_session, race_session=race_session)

        distance_records = []
        
        for driver, group in telemetry_df.groupby('DriverNumber'):

            ref_signal = ref_singnal_df.loc[driver]

            # Drop NaN values before converting to numpy arrays
            ref_lon = np.array(ref_signal['LonAcc'].dropna().values, dtype=np.double)
            ref_lat = np.array(ref_signal['LatAcc'].dropna().values, dtype=np.double)

            for lap in group['LapNumber'].unique():
                lap_df2 = group[group['LapNumber'] == lap]

                # Drop NaN values for lap data too
                lap_lon = np.array(lap_df2['LonAcc'].dropna().values, dtype=np.double)
                lap_lat = np.array(lap_df2['LatAcc'].dropna().values, dtype=np.double)
                
                # Compute DTW distances
                distances_lon = dtw.distance_fast(ref_lon, lap_lon)
                distances_lat = dtw.distance_fast(ref_lat, lap_lat)
                distance_records.append({
                        'DriverNumber': driver,
                        'LapNumber': lap,
                        'LonDistanceDTW': distances_lon,
                        'LatDistanceDTW': distances_lat
                    })

        #distance_records = [] # ?
    
        dist_df = pd.DataFrame(distance_records)
        
        return dist_df
        
    def _calculate_reference_df(self, telemetry_df : pd.DataFrame, quali_session : Session, race_session : Session, quali_data) -> pd.DataFrame:
        ref_singnal_df = pd.DataFrame()

        for driver in telemetry_df['DriverNumber'].unique():
            
            driver_laps = quali_data[quali_data["Driver"] == "VER"]
            #driver_laps : Laps = quali_session.laps.pick_drivers(driver)
            fastest_lap : Lap | None = driver_laps.pick_fastest()
            
            if fastest_lap is None and not driver_laps.empty:
                fastest_lap : Laps = race_session.laps.pick_drivers(driver)
            
            if fastest_lap is not None:
                q_lap = fastest_lap.get_telemetry()
                q_lap['DriverNumber'] = driver
                q_lap['LapNumber'] = 1.0
                
                telemetry_processing = TelemetryProcessing(q_lap, acceleration_computations=AccelerationComputations())
                telemetry_processing.calculate_accelerations()
                
                telemetry_processing.data.drop(columns='LapNumber', inplace=True)
                
                ref_singnal_df = pd.concat([ref_singnal_df, q_lap], axis=0)
        
        
        ref_singnal_df = ref_singnal_df.set_index('DriverNumber')

        return ref_singnal_df