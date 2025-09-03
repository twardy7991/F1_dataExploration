from pipelines.race_pipeline.extract.extractor import RaceExtractor
from pipelines.race_pipeline.transform.transformer import RaceTransformer
from pipelines.race_pipeline.load.race_loader import RaceLoader

# setup
races_2023 = [
    'Bahrain',
    'Saudi Arabia',
    'Australia',
    'Azerbaijan',
    'Miami',
    'Monaco',
    'Spain',
    'Canada',
    'Austria',
    'Great Britain',
    'Hungary',
    'Belgium',
    'Netherlands',
    'Italy',
    'Singapore',
    'Japan',
    'Qatar',
    'United States',
    'Mexico',
    'Brazil',
    'Las Vegas',
    'Abu Dhabi'
]
year = 2023

# pipeline
extractor = RaceExtractor()
transformer = RaceTransformer()
loader = RaceLoader()

for gp_name in races_2023:
    
    quali_session = extractor.get_session(year=year, gp_name=gp_name, session_type='Q')
    race_session = extractor.get_session(year=year, gp_name=gp_name, session_type='R')
    
    race_telemetry_df = extractor.get_session_telemetry(race_session)
    
    processed_df = transformer.build_session_dataset(gp_name=gp_name, 
                                                     race_session=race_session,
                                                     quali_session=quali_session,
                                                     race_telemetry=race_telemetry_df,
                                                     )

    loader.save_session_df_to_pickle(df=processed_df, gp_name=gp_name, session_type='R', year=year)
    
    print(f"{gp_name} processed and loaded into pickle")