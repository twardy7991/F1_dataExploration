from airflow.providers.standard.operators.python import PythonOperator

from airflow_project.dags.race_pipeline.tasks.transform import transform
import pandas as pd
from pandas.testing import assert_frame_equal

import os
from pytest_mock.plugin import MockerFixture

def test_transform(mocker : MockerFixture, temp_dir):
    
    ti = mocker.MagicMock()
    
    # "*" means no positional arguments are allowed, all arguments passed agter is must be keyword-only.
    # "**_" means we accept other keyword arguments but ignore them.
    # this simulates real xcom_pull that does not accept positional arguments and,
    # might accept many keyword arguments but we only want to use task_ids and key.
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
        "save_path" : temp_dir,
    }

    result = transform.function(
            **context
        )
    
    correct_list = os.listdir("test/data/processed/Bahrain Grand Prix")
    created_list = os.listdir(temp_dir + "/2023/Bahrain Grand Prix")
    
    assert created_list == correct_list
    
    correct_parq = pd.read_parquet("test/data/processed/Bahrain Grand Prix/" + correct_list[0])    
    created_parq = pd.read_parquet(temp_dir + "/2023/Bahrain Grand Prix/" + created_list[0])
    
    assert_frame_equal(correct_parq, created_parq)