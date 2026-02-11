from race_pipeline.tasks.transform import transform

import os
from pytest_mock.plugin import MockerFixture
import pytest_benchmark

import pytest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import *

@pytest.fixture
def spark_test():
    s = SparkSession.builder.appName("Test_pyspark").getOrCreate()
    yield s

def test_transform(mocker : MockerFixture, temp_dir, benchmark, spark_test : SparkSession):
    
    ti = mocker.MagicMock()
    
    # "*" means no positional arguments are allowed, all arguments passed agter is must be keyword-only.
    # "**_" means we accept other keyword arguments but ignore them.
    # this simulates real xcom_pull that does not accept positional arguments and,
    # might accept many keyword arguments but we only want to use task_ids and key.
    import os
    print(os.getcwd())
    
    def xcom_pull_side_effect(*, task_ids=None, key=None, **_):
        xcom_map = {
            ("extract", "quali_data_file") : "test/data/Bahrain Grand Prix/2023_Bahrain Grand Prix_quali_data.parquet",
            ("extract", "quali_telemetry_file") : "test/data/Bahrain Grand Prix/2023_Bahrain Grand Prix_quali_telemetry.parquet",
            ("extract", "race_data_file") : "test/data/Bahrain Grand Prix/2023_Bahrain Grand Prix_race_data.parquet",
            ("extract", "race_telemetry_file") : "test/data/Bahrain Grand Prix/2023_Bahrain Grand Prix_race_telemetry.parquet",
        }
        return xcom_map[(task_ids, key)]
    
    # patching the xcom_pull method with money function
    ti.xcom_pull.side_effect = xcom_pull_side_effect

    context = {"ti" : ti,
        "year" : 2023,
        "gp_name" :"Bahrain Grand Prix",
        "spark" : spark_test,
    }

    # Call the transform function directly with the context
    result = transform(
        year=context["year"],
        gp_name=context["gp_name"],
        save_path=temp_dir,
        spark=context["spark"],
        **{"ti": ti}
    )
    
    # Verify that the output file was created
    created_file = os.path.join(temp_dir, "2023", "Bahrain Grand Prix", "session_dataset.parquet")
    assert os.path.exists(created_file), f"Output file not found at {created_file}"
    
    # Compare the created parquet file with the expected one
    expected_file = "test/data/processed/Bahrain Grand Prix/session_dataset.parquet"
    
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
    
    if os.path.exists(expected_file):
        expected_parq = spark_test.read.schema(schema=schema).parquet(expected_file)
        
        import pyspark.sql.functions as F
        expected_parq = expected_parq.withColumn("LapTime", F.col("LapTime") / F.lit(1000000000))
        expected_parq = expected_parq.sort("Driver", "LapNumber")
        
        created_parq = spark_test.read.parquet(created_file)
        created_parq = created_parq.sort("Driver", "LapNumber")

        created_parq = created_parq.withColumn("SumLonAcc", F.col("SumLonAcc") * F.lit(1000))
                
        print("\n----------------------")
        
        print("\n\n\n\n EXPECTED", expected_parq.show(truncate=False))
        
        print("\n----------------------\n")
        
        print("\n\n\n\n CREATED", created_parq.show(truncate=False))
        
        print("\n----------------------")
        
        assertDataFrameEqual(created_parq, expected_parq, atol=1e1)
    else:
        # If no expected file exists, just verify that the created file can be read
        created_parq = spark_test.read.parquet(created_file)
        assert created_parq is not None, "Failed to read created parquet file"
        assert created_parq.count() > 0, "Created parquet file is empty"