from pipelines.season_pipeline.extract.season_extractor import SeasonExtractor
from pipelines.season_pipeline.transofrm.season_transformer import SeasonTransformer
from pipelines.season_pipeline.load.season_loader import SeasonLoader

# setup
year = 2023

# pipeline
extractor = SeasonExtractor()
transformer = SeasonTransformer()
loader = SeasonLoader()

for race in extractor.load_season_races_from_pickle(year):
    transformer.add_race_data(race_df=race)

loader.save_season_dataset(transformer.season_df)