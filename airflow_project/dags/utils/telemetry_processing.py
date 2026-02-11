import logging

import pandas as pd
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, when, lit, aggregate
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window

### UTIL CLASS FOR TELEMETRY ###
class TelemetryProcessing: 

    def __init__(self, data : DataFrame, acceleration_computations):
        self.data = data
        self.acceleration_computations = acceleration_computations 

    def _divide_column_by_sign(self, df : DataFrame, column : str) -> pd.DataFrame:
        
        df = df.withColumn(f"positive_{column}", 
                           when(col(column) < 0, lit(0))
                           .otherwise(col(column))
                        )
        
        df = df.withColumn(f"negative_{column}", 
                           when(col(column) > 0, lit(0))
                           .otherwise(col(column))
                        )
        
        #logging.debug("df after _divide_column_by_sign %s" , df.head())
        
        return df    
    
    def normalize_drs(self):
        
        print(self.data.columns)
        
        self.data = self.data.withColumn("DRS", 
                                        when(col("DRS").isin(10, 12, 14), 1)
                                        .otherwise(0)
                                    )

        #logging.debug("df after normalize_drs %s", self.data.head())
        
        return self

    def calculate_mean_lap_speed(self):
        
        df_1 = (
            self.data.select("Speed","DriverNumber", "LapNumber")
            .groupBy("DriverNumber", "LapNumber")
            .agg({"Speed" : "mean"})
            .withColumnRenamed("avg(Speed)", "MeanLapSpeed")
        )

        self.data = self.data.join(
            df_1,
            on=["DriverNumber", "LapNumber"],
            how="left"
        )

        print("mean lap speed cols", self.data.columns)
        #logging.debug(
        #   "df after mean_lap_speed:\n%s",
        #   self.data.head()
        #)

        return self

    ## not great, (up for improvement)
    def calculate_accelerations(self):

        schema = StructType([
            StructField("Date", LongType() ,False),
            StructField("SesssionTime", DayTimeIntervalType(), False),
            StructField("DriverAhead", StringType(), True),
            StructField("DistanceToDriverAhead", DoubleType(), True),
            StructField("Time", DayTimeIntervalType(), False),
            StructField("RPM", DoubleType(), False),
            StructField("Speed", DoubleType(), False),
            StructField("nGear", LongType(), False),
            StructField("Throttle", DoubleType(), False),
            StructField("Brake", BooleanType(), False),
            StructField("DRS", LongType(), False),
            StructField("Source", StringType(), True),
            StructField("Distance", DoubleType(), False),
            StructField("RelativeDistance", DoubleType(), False),
            StructField("Status", StringType(), False),
            StructField("X", DoubleType(), False),
            StructField("Y", DoubleType(), False),
            StructField("Z", DoubleType(), False),
            StructField("DriverNumber", StringType(), False),
            StructField("LapNumber", DoubleType(), False),
            StructField("MeanLapSpeed", DoubleType(), False),
            StructField("LonAcc", DoubleType(), True),
            StructField("LatAcc", DoubleType(), True)
        ])
        
        computations = self.acceleration_computations 
        
        print("SDFWQEGFSEFASEGAWSSGARSG",   self.data.show())
        
        self.data = (
            self.data.groupby('DriverNumber', 'LapNumber')
            .applyInPandas(
                lambda pdf: computations.compute_accelerations(pdf), 
                schema=schema
                )
        )
        
        print(self.data.show())
        
        self.data = self.data.withColumn(
            'AbsLatAcc',
            F.abs('LatAcc')
        )

        self.data = self.data.withColumn(
            'AbsLonAcc',
            F.abs('LonAcc')
        )
        
        w = Window.partitionBy('DriverNumber', 'LapNumber')

        self.data = self.data.withColumn('SumLonAcc', F.sum('AbsLonAcc').over(w))
        self.data = self.data.withColumn('SumLatAcc', F.sum('AbsLatAcc').over(w))
        #logging.debug("df after calculate_accelerations %s", self.data.head())
        
        return self

    def calculate_lap_progress(self):
        w = Window.partitionBy("DriverNumber", "LapNumber")
        
        self.data = self.data.withColumn("TimeNumberLapTime", F.row_number().over(Window.partitionBy("DriverNumber", "LapNumber").orderBy("Time")))
        
        self.data = self.data.withColumn("TimeNumberLapCounts", F.max("TimeNumberLapTime").over(w))

        self.data = self.data.withColumn("LapProgress", col("TimeNumberLapTime") / col("TimeNumberLapCounts"))

        self.data = self.data.drop("TimeNumberLapTime", "TimeNumberLapCounts")
                
        return self

    ## not great, but i need a way to get single lap telemetry data (up for improvement)
    def get_single_lap_data(self):
        
        self.data = self.data.dropDuplicates(['DriverNumber', 'LapNumber'])
                    
        #logging.debug("df after get_single_lap_data \n%s \n", self.data.head())
        #logging.debug("df shape get_single_lap_data \n%s \n", self.data.s)
        return self.data



### UTIL CLASS FOR TELEMETRY ###
class OldTelemetryProcessing: 

    def __init__(self, data : pd.DataFrame, acceleration_computations):
        self.data = data
        self.acceleration_computations = acceleration_computations 

    def _divide_column_by_sign(self, df : pd.DataFrame, column : str) -> pd.DataFrame:
    
        df[f"positive_{column}"] = df[column].apply(lambda x: 0 if x < 0 else x)
        df[f"negative_{column}"] = df[column].apply(lambda x: 0 if x > 0 else x)
        
        #logging.debug("df after _divide_column_by_sign %s" , self.data.head().to_string(max_cols=None))
        
        return df    
    
    def normalize_drs(self):

        self.data.DRS = self.data.DRS.apply(lambda x: 1 if x in [10, 12, 14] else 0)
        
        #logging.debug("df after normalize_drs %s", self.data.head().to_string(max_cols=None))
        
        return self

    def calculate_mean_lap_speed(self):

        self.data["MeanLapSpeed"] = self.data.groupby(["DriverNumber", "LapNumber"])["Speed"].transform("mean")

        #logging.debug(
        #   "df after mean_lap_speed:\n%s",
        #   self.data.head().to_string(max_cols=None)
        #)

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

        #logging.debug("df after calculate_accelerations %s", self.data.head().to_string(max_cols=None))
        
        return self

    def calculate_lap_progress(self):
        
        self.data['TimeNumberLapTime'] = self.data.groupby(['DriverNumber', 'LapNumber']).cumcount() + 1
        self.data['TimeNumberLapCounts'] = self.data.groupby(['DriverNumber', 'LapNumber'])['LapNumber'].transform('count')

        self.data['LapProgress'] = self.data['TimeNumberLapTime'] / self.data['TimeNumberLapCounts']
        
        ## added during refactoring
        self.data.drop(columns=['TimeNumberLapTime', 'TimeNumberLapCounts'], inplace=True)
        
        #logging.debug("df after calculate_lap_progress %s", self.data.head())
        
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
                    
        #logging.debug("df after get_single_lap_data \n%s \n", final_df.head().to_string(max_cols=None))
        #logging.debug("df shape get_single_lap_data \n%s \n", final_df.shape)
        return final_df
