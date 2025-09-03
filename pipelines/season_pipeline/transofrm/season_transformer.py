from typing import List
import pandas as pd

class SeasonTransformer:

    def __init__(self):
        self.season_df = pd.DataFrame()
    
    def add_race_data(self, race_df):
        self.season_df = pd.concat([self.season_df, race_df], ignore_index=True)
        