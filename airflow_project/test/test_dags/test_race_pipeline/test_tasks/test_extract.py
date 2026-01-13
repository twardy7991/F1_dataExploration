from airflow.providers.standard.operators.python import PythonOperator

from airflow_project.dags.race_pipeline.tasks.extract import extract
import pandas as pd
from pandas.testing import assert_frame_equal

import os

def test_extract(temp_dir):
    
    result = extract.function(
            year=2023,
            gp_name="Bahrain Grand Prix",
            save_path=temp_dir,
        )
    
    print(result)
    print(os.getcwd())
    
    correct_list = os.listdir("test/data/Bahrain Grand Prix")
    created_list = os.listdir(temp_dir + "/2023/Bahrain Grand Prix")
    
    assert set(created_list) == set(correct_list)
    
    for i in range(0, len(correct_list)):
        correct_parq = pd.read_parquet("test/data/Bahrain Grand Prix/" + correct_list[i])    
        created_parq = pd.read_parquet(temp_dir + "/2023/Bahrain Grand Prix/" + created_list[i])
        
        assert_frame_equal(correct_parq, created_parq)