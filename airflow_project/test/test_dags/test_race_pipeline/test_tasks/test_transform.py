from race_pipeline.tasks.transform import transform

import os
from pytest_mock.plugin import MockerFixture
import pytest_benchmark

import pytest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import *

from pathlib import Path 

@pytest.fixture
def spark_test():
    import shutil
    PROJ_ROOT = Path(__file__).resolve().parents[5]

    utils_dir = PROJ_ROOT / "airflow_project/utils"
    output_folder = PROJ_ROOT / "airflow_project/test/test_dags"
    output_folder.mkdir(parents=True, exist_ok=True)

    shutil.make_archive(
        base_name=str(output_folder / "deps"),  
        format="zip",
        root_dir=utils_dir.parent,  
        base_dir=utils_dir.name     
    )

    s : SparkSession = (SparkSession.builder
         .config("spark.submit.pyFiles", "airflow_project/test/test_dags/deps.zip")
         .master("local[2]")
         .appName("Test_pyspark")
         .getOrCreate()
        )
    yield s
    s.stop()

def test_transform(mocker : MockerFixture, temp_dir, benchmark, spark_test : SparkSession):
    
    PROJ_ROOT = Path(__file__).resolve().parents[5]

    ti = mocker.MagicMock()
    
    # "*" means no positional arguments are allowed, all arguments passed agter is must be keyword-only.
    # "**_" means we accept other keyword arguments but ignore them.
    # this simulates real xcom_pull that does not accept positional arguments and,
    # might accept many keyword arguments but we only want to use task_ids and key.
    import os

    args = ["--race_telemetry_file", str(PROJ_ROOT) + "/data/raw/2023/Bahrain Grand Prix/2023_Bahrain Grand Prix_race_telemetry.parquet",
            "--quali_telemetry_file", str(PROJ_ROOT) + "/data/raw/2023/Bahrain Grand Prix/2023_Bahrain Grand Prix_quali_telemetry.parquet",
            "--race_data_file", str(PROJ_ROOT) + "/data/raw/2023/Bahrain Grand Prix/2023_Bahrain Grand Prix_race_data.parquet",
            "--quali_data_file", str(PROJ_ROOT) + "/data/raw/2023/Bahrain Grand Prix/2023_Bahrain Grand Prix_quali_data.parquet",
            "--year", "2023",
            "--gp_name", "Bahrain Grand Prix",
            "--processed_base", f"{temp_dir}"
    ]

    # Call the transform function directly with the context
    result = transform(args=args, spark=spark_test)

    # Verify that the output file was created
    created_file = os.path.join(temp_dir, "2023", "Bahrain Grand Prix", "session_dataset.parquet")
    assert os.path.exists(created_file), f"Output file not found at {created_file}"
    
    # Compare the created parquet file with the expected one
    expected_file = str(PROJ_ROOT) + "/airflow_project/test/data/processed/Bahrain Grand Prix/session_dataset.parquet"
    
    schema = StructType(
        [
            StructField("LapNumber", IntegerType() ,False),
            StructField("Driver", StringType(), False),
            StructField("Compound", StringType(), False),            
            StructField("TyreLife", IntegerType(), False),
            StructField("StartFuel", DoubleType(), False),
            StructField("FCL", DoubleType(), False),
            StructField("LapTime", LongType(), False),
            StructField("SpeedI1", DoubleType(), True),
            StructField("SpeedI2", DoubleType(), True),
            StructField("SpeedFL", DoubleType(), True),
            StructField("SumLonAcc", DoubleType(), True),
            StructField("SumLatAcc", DoubleType(), True),
            StructField("MeanLapSpeed", DoubleType(), False),
        ]
    )
    
    expected_parq = spark_test.read.schema(schema=schema).parquet(expected_file)
    
    import pyspark.sql.functions as F
    expected_parq = expected_parq.withColumn("LapTime", F.col("LapTime") / F.lit(1000000000))
    expected_parq = expected_parq.sort("Driver", "LapNumber")
    
    created_parq_dir = Path(created_file)
    print(os.listdir(created_parq_dir))

    created_parq_path = list(created_parq_dir.rglob("part-*"))[0]

    created_parq = spark_test.read.parquet(str(created_parq_path))
    created_parq = created_parq.sort("Driver", "LapNumber")

    #created_parq = created_parq.withColumn("SumLonAcc", F.col("SumLonAcc") * F.lit(1000))
            
    print("\n----------------------")
    
    print("\n\n\n\n EXPECTED", expected_parq.show(truncate=False))
    
    print("\n----------------------\n")
    
    print("\n\n\n\n CREATED", created_parq.show(truncate=False))
    
    print("\n----------------------")

    assertDataFrameEqual(created_parq, expected_parq, atol=0.1) #atol=1e1
    raise NotImplementedError