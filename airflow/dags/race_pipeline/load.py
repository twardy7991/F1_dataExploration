from airflow.sdk import task
from pathlib import Path
#from sqlalchemy import 

@task
def save_processed_session(**context) -> None:
    ti = context["ti"]

    processed_file = ti.xcom_pull(
        task_ids="build_session_dataset",
        key="processed_file",
    )
    print(f"Retrieved processed file path from XCom: {processed_file}")
    path = Path(processed_file)
    if not path.exists():
        raise FileNotFoundError(path)

    print(f"Telemetry saved to {path}")