from pipelines.race_pipeline.load.race_loader import RaceLoader
import pandas as pd
from pandas.testing import assert_frame_equal

def test_save_session_df_to_pickle(temp_cache_dir):
    
    race_loader = RaceLoader(directory_path=temp_cache_dir)
    
    df = pd.DataFrame([[1,2,3,4,5,6],[1,2,3,4,5,6],[1,2,3,4,5,6],[1,2,3,4,5,6]])
    
    year = "2026"
    gp_name = "gp"
    session_type = "Q"
    
    race_loader.save_session_df_to_pickle(df=df, gp_name=gp_name, session_type=session_type, year=year)
    
    saved_df = pd.read_pickle(f"{temp_cache_dir}/{year}/telemetry_{year}_{gp_name}_{session_type}.pkl")
    
    assert_frame_equal(df, saved_df)