import pandas as pd
from pathlib import Path
from airflow.decorators import task
from sqlalchemy.dialects import 

DIRECTORY_PATH = "./data"

def save_session_df_to_pickle(**context) -> None:

    params = context["params"]
    gp_name = params["gp_name"]
    year = params["year"]
    session_type = session_type["session_type"]

    dir_path = Path(DIRECTORY_PATH)
    dir_path.mkdir(parents=True, exist_ok=True)

    output_file = dir_path / f"seasons/{year}/telemetry_{gp_name}_{session_type}.pkl"
    df.to_parquet(output_file)

    print(f"Telemetry saved to {output_file}")