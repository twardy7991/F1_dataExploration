import logging

import pandas as pd

from utils.acceleration_computations import AccelerationComputations

### UTIL CLASS FOR TELEMETRY ###
class TelemetryProcessing: 

    def __init__(self, data : pd.DataFrame, acceleration_computations : AccelerationComputations):
        self.data = data
        self.acceleration_computations = acceleration_computations 

    def _divide_column_by_sign(self, df : pd.DataFrame, column : str) -> pd.DataFrame:
    
        df[f"positive_{column}"] = df[column].apply(lambda x: 0 if x < 0 else x)
        df[f"negative_{column}"] = df[column].apply(lambda x: 0 if x > 0 else x)
        
        logging.debug("df after _divide_column_by_sign %s" , self.data.head().to_string(max_cols=None))
        
        return df    
    
    def normalize_drs(self):

        self.data.DRS = self.data.DRS.apply(lambda x: 1 if x in [10, 12, 14] else 0)
        
        logging.debug("df after normalize_drs %s", self.data.head().to_string(max_cols=None))
        
        return self

    def calculate_mean_lap_speed(self):

        self.data["MeanLapSpeed"] = self.data.groupby(["DriverNumber", "LapNumber"])["Speed"].transform("mean")

        logging.debug(
           "df after mean_lap_speed:\n%s",
           self.data.head().to_string(max_cols=None)
        )

        return self

    ## not great, (up for improvement)
    def calculate_accelerations(self):

        computations = self.acceleration_computations # ?

        all_lon, all_lat = [], []

        for (driver, lap), group in self.data.groupby(['DriverNumber', 'LapNumber']):
            lon_, lat_ = computations.compute_accelerations(telemetry=group)
            all_lon.append(lon_)
            all_lat.append(lat_)

        all_lon_series = [pd.Series(arr) for arr in all_lon]
        all_lat_series = [pd.Series(arr) for arr in all_lat]

        self.data['LonAcc'] = pd.concat(all_lon_series, ignore_index=True)
        self.data['LatAcc'] = pd.concat(all_lat_series, ignore_index=True)
    
        self.data['AbsLatAcc'] = self.data['LatAcc'].abs()
        self.data['AbsLonAcc'] = self.data['LonAcc'].abs()

        self.data['SumLatAcc'] = self.data.groupby(['DriverNumber', 'LapNumber'])['AbsLatAcc'].transform('sum')
        self.data['SumLonAcc'] = self.data.groupby(['DriverNumber', 'LapNumber'])['AbsLonAcc'].transform('sum')

        logging.debug("df after calculate_accelerations %s", self.data.head().to_string(max_cols=None))
        
        return self

    def calculate_lap_progress(self):
        
        self.data['TimeNumberLapTime'] = self.data.groupby(['DriverNumber', 'LapNumber']).cumcount() + 1
        self.data['TimeNumberLapCounts'] = self.data.groupby(['DriverNumber', 'LapNumber'])['LapNumber'].transform('count')

        self.data['LapProgress'] = self.data['TimeNumberLapTime'] / self.data['TimeNumberLapCounts']
        
        ## added during refactoring
        self.data.drop(columns=['TimeNumberLapTime', 'TimeNumberLapCounts'], inplace=True)
        
        logging.debug("df after calculate_lap_progress %s", self.data.head().to_string(max_cols=None))
        
        return self

    ## not great, but i need a way to get single lap telemetry data (up for improvement)
    def get_single_lap_data(self):
        
        final_df = pd.DataFrame(columns=self.data.columns)

        for driver in self.data['DriverNumber'].unique():
            driver_df = self.data[self.data['DriverNumber'] == driver]
            
            logging.debug("driver_num %s \n driver_df \n%s \n", driver, driver_df.head().to_string(max_cols=None))
            
            laps : pd.DataFrame = int(driver_df['LapNumber'].max())

            logging.debug("laps \n %s \n", laps)
                   
            for lap in range(1, laps + 1):
                lap_df = driver_df[driver_df['LapNumber'] == lap]
                
                logging.debug("laps \n %s \n", lap_df.head().to_string(max_cols=None))
                
                if not lap_df.empty:
                    final_row = lap_df.iloc[[-1], :]
                    
                    logging.debug("final_row \n %s \n", final_row)
                    
                    final_df = pd.concat([final_df, final_row], axis=0)
                    
        logging.debug("df after get_single_lap_data \n%s \n", final_df.head().to_string(max_cols=None))
        logging.debug("df shape get_single_lap_data \n%s \n", final_df.shape)
        return final_df
