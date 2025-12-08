import pandas as pd
from pathlib import Path

DIRECTORY_PATH = "./cache"

class RaceLoader:
    
    def __init__(self, directory_path : str = DIRECTORY_PATH):
        self.cache_path = directory_path
    
    def save_session_df_to_pickle(self, df: pd.DataFrame, gp_name: str, session_type: str, year: int) -> None:
        dir_path = Path(self.cache_path) / str(year)
        dir_path.mkdir(parents=True, exist_ok=True)

        output_file = dir_path / f"telemetry_{year}_{gp_name}_{session_type}.pkl"
        df.to_pickle(output_file)

        print(f"Telemetry saved to {output_file}")