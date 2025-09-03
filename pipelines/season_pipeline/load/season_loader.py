from typing import List
import pandas as pd

class SeasonLoader:
    def __init__(self):
        pass
    
    def save_season_dataset(self, df : pd.DataFrame) -> None:        
        df.to_pickle('season_full_NAs.pkl')