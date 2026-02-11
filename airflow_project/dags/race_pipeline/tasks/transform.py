from pathlib import Path
import logging
import sys

import pandas as pd
from fastf1.core import Session, Telemetry, Laps
from airflow.sdk import task

# Add dags directory to path for imports to work in all contexts (tests, Airflow, PySpark)
# current_file = Path(__file__)
# dags_dir = current_file.parent.parent.parent  # Up: tasks -> race_pipeline -> dags
# if str(dags_dir) not in sys.path:
#     sys.path.insert(0, str(dags_dir))

from utils import TelemetryProcessing, AccelerationComputations, FuelProcessing
from race_pipeline.tasks.constants import SCHEMA, TRACK_INFO

from pyspark.sql import SparkSession    
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#pyspark(conn_id = "spark_conn", do_xcom_push=True, multiple_outputs=True)   
## feels too monolith, probably needs decoupling
def transform(spark : SparkSession, **context) -> dict:
    
    fuel_start : int = 100 
    # Extract parameters from XCom (from get_params task)
    ti = context["ti"]
    params_dict = ti.xcom_pull(task_ids="get_params")
    year = params_dict["year"]
    gp_name = params_dict["gp_name"]
    save_path = Path(params_dict["processed_base"])

    def read_df(path, key):
        return (spark
                .read
                .schema(schema=SCHEMA[key])
                .parquet(path)
                )
    
    def get_data(context):

        ti = context["ti"]
        race_telemetry = ti.xcom_pull(task_ids="extract", key = "race_telemetry_file")
        quali_telemetry = ti.xcom_pull(task_ids="extract", key = "quali_telemetry_file")
        race_data = ti.xcom_pull(task_ids="extract", key = "race_data_file")
        quali_data = ti.xcom_pull(task_ids="extract", key="quali_data_file")

        return (read_df(race_telemetry, "race_telemetry_file"),
                read_df(quali_telemetry, "quali_telemetry_file"),
                read_df(race_data, "race_data_file"),
                read_df(quali_data, "quali_data_file"))

    ### creating telemetry dataframe  #########

    race_telemetry, quali_telemetry, race_laps_df, quali_data = get_data(context) 
    
    race_laps_df = race_laps_df.withColumn(
    "LapTime", F.col("LapTime") / F.lit(1000000000)
    )
    # if 'Time' in race_telemetry.columns:
    #     race_telemetry['Time'] = pd.to_timedelta(race_telemetry['Time'])

    lap_length, track_length = TRACK_INFO[gp_name]
    #race_laps_df : Laps = race_session.laps
    main_cols = ['Driver', 'Team', 'Compound', 'TyreLife', 'LapTime', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'LapNumber', 'DriverNumber']
    
    # drop NA # causing bugs rn
    race_laps_df = race_laps_df.select(main_cols)
    #lap_df = lap_df[main_cols].dropna()
    
    # start = time.time()
    # -----------FUEL------------ #
    #logger.debug(f"race_laps_df driver list : \n{race_laps_df['DriverNumber'].unique()} \n")
    
    fuel_processing : FuelProcessing = FuelProcessing(race_laps_df)
    race_laps_with_fuel = fuel_processing.calculateFCL(lap_length=lap_length, track_length=track_length, fuel_start=fuel_start)
    
    # print(f"Fuel calculations done in {time.time() - start:.2f} seconds.")
    
    # start = time.time()
    # -----------TELEMETRY------------ #
    telemetry_df = race_telemetry
    
    #logger.debug(f"telemetry_df driver list : \n{telemetry_df['DriverNumber'].unique()} \n")

    tel_processing = TelemetryProcessing(telemetry_df, acceleration_computations=AccelerationComputations())
    tel_processing.calculate_mean_lap_speed()
    tel_processing.calculate_accelerations()
    tel_processing.normalize_drs()
    
    lap_telemetry = tel_processing.get_single_lap_data()
    
    print("\nlap_telemetry ", lap_telemetry.columns)
    print("\nrace_laps_with_fuel ", race_laps_with_fuel.columns)
    lap_df = race_laps_with_fuel.join(lap_telemetry, on=['DriverNumber', 'LapNumber'], how="left")

    # start = time.time()
    # -----------DTW------------ #
    # dtw_computations = DTWComputations()
    # dist_df = dtw_computations.calculate_dtw(quali_session=quali_session, telemetry_df=telemetry_df, race_session=race_session)
    
    # lap_df = pd.merge(lap_df,dist_df, on=['DriverNumber', 'LapNumber'], how='left')

    #final_cols = ['LapNumber','Driver', 'Compound', 'TyreLife', 'StartFuel', 'FCL', 'LapTime', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'SumLonAcc', 'SumLatAcc', 'MeanLapSpeed', 'LonDistanceDTW', 'LatDistanceDTW']
    print("\nlap_df ", lap_df.columns)
    # -----------FINAL------------ #
    final_cols = ['LapNumber','Driver', 'Compound', 'TyreLife', 'StartFuel', 'FCL', 'LapTime', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'SumLonAcc', 'SumLatAcc', 'MeanLapSpeed']
    processed_df = lap_df.select(final_cols)
    
    #logger.debug(processed_df.head())
    
    #logging.info(f"DTW calculations done in {time.time() - start:.2f} seconds.")

    out_dir = save_path / str(year) / gp_name
    out_dir.mkdir(parents=True, exist_ok=True)
    
    #logger.info(f"Output directory: {out_dir}")
    
    out_file = out_dir / "session_dataset.parquet"
    
    logger.info(f"Saving processed data to {out_file}")
    
    print("df datatypes", processed_df.dtypes)
    
    processed_df.write.parquet(str(out_file))

    return {
        "processed_file": str(out_file)
    }

