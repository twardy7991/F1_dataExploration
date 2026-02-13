from pathlib import Path
from datetime import datetime
import logging
import json

from airflow.sdk import dag, Param, task
from airflow.providers.apache.spark.operators.spark_pyspark import PySparkOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from race_pipeline.tasks.extract import extract
from race_pipeline.tasks.transform import transform
from race_pipeline.tasks.load import load

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_args = {"owner": "airflow", "start_date": datetime(2021, 1, 1)}

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
    
    gp_name = params["gp_name"]
    logger.info(f"STARTING RACE_DAG FOR {gp_name}")

    extract_session = extract(year = params["year"], gp_name = params["gp_name"], save_path = params["raw_base"])

    spark_context = [
        "--race_telemetry_file", "{{task_instance.xcom_pull(task_ids='extract', key='race_telemetry_file')}}",
        "--quali_telemetry_file", "{{task_instance.xcom_pull(task_ids='extract', key='quali_telemetry_file')}}",
        "--race_data_file", "{{task_instance.xcom_pull(task_ids='extract', key='race_data_file')}}",
        "--quali_data_file", "{{task_instance.xcom_pull(task_ids='extract', key='quali_data_file')}}",
        "--year", "{{task_instance.xcom_pull(task_ids='get_params', key='year')}}",
        "--gp_name", "{{task_instance.xcom_pull(task_ids='get_params', key='gp_name')}}",
        "--processed_base", "{{task_instance.xcom_pull(task_ids='get_params', key='processed_base')}}"
    ]
    
    transform_session = SparkSubmitOperator(application="dags/race_pipeline/tasks/transform.py",
                        task_id="spark_transform",
                        conf= {
                            "spark.driver.bindAddress": "0.0.0.0",
                            "spark.driver.host": "airflow-scheduler",
                            "spark.driver.port": "33597",
                            "spark.blockManager.port": "33600"
                        },
                        conn_id="spark_conn",
                        py_files="/opt/airflow/dags/deps.zip",
                        #py_files="/opt/airflow/dags/utils/acceleration_computations.py,/opt/airflow/dags/utils/fuel_processing.py,/opt/airflow/dags/utils/telemetry_processing.py,/opt/airflow/dags/race_pipeline/tasks/constants.py",
                        executor_cores=2,
                        executor_memory="4G",
                        num_executors=1,
                        spark_binary="spark-submit",
                        deploy_mode="client",
                        env_vars={"PYTHONPATH" : "/opt/airflow/dags"},
                        application_args=spark_context,
                        verbose = True,
                        retries = 0
                        )

    # transform_session = PySparkOperator(python_callable=transform, 
    #                                     task_id="spark_transform",
    #                                     conn_id="spark_conn",
    #                                     do_xcom_push=True, 
    #                                     multiple_outputs=True
    #                                 )
    #transform_session = transform(year = params["year"], gp_name = params["gp_name"], save_path = params["processed_base"])

    load_session = load(year = params["year"], gp_name = params["gp_name"])

    extract_session >> transform_session >> load_session
    
    # Ensure get_params runs before transform (for XCom availability)
    params >> transform_session
    
session_pipeline = etl_pipeline() 