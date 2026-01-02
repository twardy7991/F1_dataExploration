from pathlib import Path
from race_pipeline.extract import extract
from race_pipeline.transform import transform
from race_pipeline.load import load
from airflow.sdk import dag, Param, task
from datetime import datetime
from airflow.utils import timezone

import logging
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_args = default_args = {"owner": "airflow", "start_date": datetime(2021, 1, 1)}

@dag(
    dag_id = "single_race_dag",
    schedule = None,
    start_date=datetime(2021, 1, 1),
    catchup = False,
    params={
        "year": Param(2023, type="integer"),
        "race_name": Param("Bahrain Grand Prix", type="string"),
        "raw_base": "./data/raw",
        "processed_base": "./data/processed",
    },
    # w teorii tu włożyć można parametry year i name, potem tylko brać wszystko z kwargow, ale nie wiem jak zadziała z całym sezonem
)
def etl_pipeline():
    
    # logging.basicConfig(filename=None, level=logging.DEBUG)

    @task(multiple_outputs=True)
    def get_params(**context):
        return {
            "year" : context["params"]["year"],
            "gp_name" : context["params"]["race_name"],
            "raw_base" : context["params"]["raw_base"],
            "processed_base" : context["params"]["processed_base"]
        }

    params = get_params()
    
    logger.info(f"STARTING RACE_DAG FOR {params["gp_name"]}")

    extract_session = extract(year = params["year"], gp_name = params["gp_name"], save_path = params["raw_base"])

    transform_session = transform(year = params["year"], gp_name = params["gp_name"], save_path = params["processed_base"])

    load_session = load(year = params["year"], gp_name = params["gp_name"])

    extract_session >> transform_session >> load_session
    
session_pipeline = etl_pipeline() 