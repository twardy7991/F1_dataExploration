from pathlib import Path
from race_pipeline.extract import extract
from race_pipeline.transform import build_session_dataset
from race_pipeline.load import save_processed_session
from airflow.sdk import dag, Param, task
from datetime import datetime
from airflow.utils import timezone

default_args = default_args = {"owner": "airflow", "start_date": datetime(2021, 1, 1)}

@dag(
    dag_id = "single_race_dag",
    schedule = None,
    start_date=datetime(2021, 1, 1),
    catchup = False,
    params={
        "year": Param(2023, type="integer"),
        "race_name": Param("Bahrain", type="string"),
        "raw_base": "./data/raw",
        "processed_base": "./data/processed",
    },
    # w teorii tu włożyć można parametry year i name, potem tylko brać wszystko z kwargow, ale nie wiem jak zadziała z całym sezonem
)
def etl_pipeline():

    @task(multiple_outputs=True)
    def get_params(**context):
        return {
            "year" : context["params"]["year"],
            "gp_name" : context["params"]["race_name"],
            "raw_base" : context["params"]["raw_base"],
            "processed_base" : context["params"]["processed_base"]
        }

    params = get_params()

    get_data = extract(year = params["year"], gp_name = params["gp_name"], save_path = params["raw_base"])

    build_dataset = build_session_dataset(year = params["year"], gp_name = params["gp_name"], save_path = params["processed_base"])

    save = save_processed_session()


    get_data >> build_dataset >> save # w teorii save mozna wyrzucić z tego łańcucha

race_pipeline = etl_pipeline() 