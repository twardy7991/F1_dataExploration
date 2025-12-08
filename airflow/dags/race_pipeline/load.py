import pandas as pd
from pathlib import Path
from airflow.decorators import task

DIRECTORY_PATH = "./data"

@task
def save_session_df_to_pickle(df: pd.DataFrame, gp_name: str, session_type: str, year: int) -> None:
    dir_path = Path(DIRECTORY_PATH) / str(year)
    dir_path.mkdir(parents=True, exist_ok=True)

    output_file = dir_path / f"telemetry_{year}_{gp_name}_{session_type}.pkl"
    df.to_pickle(output_file)

    print(f"Telemetry saved to {output_file}")