
import pandas as pd
import numpy as np
import fastf1
import glob
import sys
import os
from sktime.distances import distance
from dtaidistance import dtw

sys.path.append('..')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from telemetry.telemetry_data_preprocessing import TelemetryProcessing
from loader import load_telemetry

# jak ja gardze file handiling w py (tak to dziala) XDDDDDD
cache_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'cache')
if not os.path.exists(cache_dir):
    os.makedirs(cache_dir)
fastf1.Cache.enable_cache(cache_dir)

TRACK_INFO = {
    'Bahrain': (5.412, 308.238),
    'Saudi Arabia': (6.174, 308.450),
    'Australia': (5.278, 306.124),
    'Azerbaijan': (6.003, 306.049),
    'Miami': (5.412, 308.326),
    'Monaco': (3.337, 260.286),
    'Spain': (4.675, 308.424),
    'Canada': (4.361, 305.270),
    'Austria': (4.318, 306.452),
    'Great Britain': (5.891, 306.198),
    'Hungary': (4.381, 306.670),
    'Belgium': (7.004, 308.052),
    'Netherlands': (4.259, 306.587),
    'Italy': (5.793, 306.720),
    'Singapore': (4.940, 308.706),
    'Japan': (5.807, 307.471),
    'Qatar': (5.419, 308.611),
    'United States': (5.513, 308.405),
    'Mexico': (4.304, 305.354),
    'Brazil': (4.309, 305.879),
    'Las Vegas': (6.201, 305.880),
    'Abu Dhabi': (5.281, 305.355),
}

def calculate_needed_fuel(lap_length, track_length, fuel_start=100, fuel_end=1):
    laps_per_track = int(track_length / lap_length)
    fuel_per_lap = (fuel_start - fuel_end) / laps_per_track
    return fuel_per_lap

def calculate_start_fuel(lap_number, fuel_per_lap, fuel_start=100):
    if lap_number == 1:
        return fuel_start
    else:
        return max(0, fuel_start - (lap_number - 1) * fuel_per_lap)


#obecnie nie potrzebne
def calculate_avg_fuel(row, fuel_per_lap, fuel_start=100):
    lap_number = row['LapNumber']

    if lap_number == 1:
        return fuel_start
    else:
        current_fuel = calculate_start_fuel(lap_number, fuel_per_lap, fuel_start)
        prev_fuel = calculate_start_fuel(lap_number - 1, fuel_per_lap, fuel_start)
        return (current_fuel + prev_fuel) / 2

def build_dataset(gp_name, year=2023, fuel_start=100):
    # track info
    try:
        lap_length, track_length = TRACK_INFO[gp_name]
    except KeyError:
        raise ValueError(f"No track information found for {gp_name}. Available tracks: {', '.join(TRACK_INFO.keys())}")
    
    #load track
    session = fastf1.get_session(year, gp_name, 'R')
    session.load()
    lap_df = session.laps
    main_cols = ['Driver', 'Team', 'Compound', 'TyreLife', 'LapTime', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'LapNumber', 'DriverNumber', 'LapNumber']
    
    # drop NA
    lap_df = lap_df[main_cols]
    #lap_df = lap_df[main_cols].dropna()
    # -----------PALIWO------------ 
    fuel_per_lap = calculate_needed_fuel(lap_length=lap_length, track_length=track_length, fuel_start=fuel_start)
    lap_df['StartFuel'] = lap_df.apply(lambda row: calculate_start_fuel(row['LapNumber'], fuel_per_lap, fuel_start), axis=1)
    
    # w srodku innej zeby trzymala zmienne, mozna na outer scope jak chcemy byc "czytelni"
    def calculate_FCL(row, fuel_penalty=0.03):
        if pd.isnull(row['LapTime']):
            return np.nan
        if row['LapNumber'] == 1:
            avg_fuel = fuel_start
        else:
            current_fuel = row['StartFuel']
            prev_fuel = fuel_start if row['LapNumber'] == 2 else row['StartFuel'] + fuel_per_lap
            avg_fuel = (current_fuel + prev_fuel) / 2
            
        fuel_correction = pd.Timedelta(seconds=fuel_penalty * avg_fuel)
        fcl = row['LapTime'] - fuel_correction
        return fcl.total_seconds() if not pd.isnull(fcl) else np.nan
    
    lap_df['FCL'] = lap_df.apply(calculate_FCL, axis=1)


    # -----------TELEMETRIA------------ 
    # na roznice miedzy fastf1 a mna
    gp_file_names = {
        'Saudi Arabia': 'Saudi',
        'Great Britain': 'Great Britain',
        'United States': 'USA',
        'Abu Dhabi': 'Abu Dhabi'
    }
    
    file_gp_name = gp_file_names.get(gp_name, gp_name)
    telemetry_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data_telemetry', f'{file_gp_name}.pkl')
    try:
        tel_df = load_telemetry(telemetry_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"Telemetry data not found for {gp_name}. Expected file: {telemetry_path}")

    tel = TelemetryProcessing(tel_df)
    
    tel.calculate_mean_lap_speed()
    tel.compute_accelerations()
    tel.normalize_drs()
    lap_telemetry = tel.get_single_lap_data()
    
    lap_df = pd.merge(
        lap_df,
        lap_telemetry,
        on=['DriverNumber', 'LapNumber'],
        how='left'
    )

    quali = fastf1.get_session(year, gp_name, 'Q')
    quali.load(telemetry=True, messages= False, weather=False)
    ref_singnal_df = pd.DataFrame()

    for driver in tel.data['DriverNumber'].unique():
        driver_laps = quali.laps.pick_drivers(driver)
        fastest_lap = driver_laps.pick_fastest()
        
        
        if fastest_lap is None and not driver_laps.empty:
            fastest_lap = session.laps.pick_drivers(driver)
        
        if fastest_lap is not None:
            q_lap = fastest_lap.get_telemetry()
            q_lap['DriverNumber'] = driver
            q_lap['LapNumber'] = 1.0
            
            q_lap = TelemetryProcessing(q_lap)
            q_lap.compute_accelerations()
            
            q_lap.data.drop(columns='LapNumber', inplace=True)
            
            ref_singnal_df = pd.concat([ref_singnal_df, q_lap.data], axis=0)

    distance_records = []
    ref_singnal_df = ref_singnal_df.set_index('DriverNumber')
    for driver, group in tel.data.groupby('DriverNumber'):

        ref_signal = ref_singnal_df.loc[driver]

        # Drop NaN values before converting to numpy arrays
        ref_lon = np.array(ref_signal['LonAcc'].dropna().values, dtype=np.double)
        ref_lat = np.array(ref_signal['LatAcc'].dropna().values, dtype=np.double)

        for lap in group['LapNumber'].unique():
            lap_df2 = group[group['LapNumber'] == lap]

            # Drop NaN values for lap data too
            lap_lon = np.array(lap_df2['LonAcc'].dropna().values, dtype=np.double)
            lap_lat = np.array(lap_df2['LatAcc'].dropna().values, dtype=np.double)
            

            # Compute DTW distances
            distances_lon = dtw.distance_fast(ref_lon, lap_lon)
            distances_lat = dtw.distance_fast(ref_lat, lap_lat)
            distance_records.append({
                    'DriverNumber': driver,
                    'LapNumber': lap,
                    'LonDistanceDTW': distances_lon,
                    'LatDistanceDTW': distances_lat
                })

    

    dist_df = pd.DataFrame(distance_records)
    lap_df = pd.merge(lap_df,dist_df, on=['DriverNumber', 'LapNumber'], how='left')

    #--------------POGODA-------------------------------    
    
    
    # ----------------SKLADANIE-------------------
    final_cols = ['LapNumber','Driver', 'Compound', 'TyreLife', 'StartFuel', 'FCL', 'LapTime', 'SpeedI1', 'SpeedI2', 'SpeedFL', 'SumLonAcc', 'SumLatAcc', 'MeanLapSpeed', 'LonDistanceDTW', 'LatDistanceDTW']
    preprocessed_df = lap_df[final_cols].reset_index(drop=True)
    print(preprocessed_df.head())
    print('Shape:', preprocessed_df.shape)
    return preprocessed_df

def season_dataset(year=2023, fuel_start=100):
    race_dfs = []
    
    for gp_name in TRACK_INFO.keys():
        try:
            print(f"Processing {gp_name} GP data...")
            df = build_dataset(gp_name=gp_name, year=year, fuel_start=fuel_start)
            # name
            df['Track'] = gp_name
            race_dfs.append(df)
            
        except Exception as e:
            print(f"Skipping {gp_name} GP: {str(e)}")
    
    # Combine all race dataframes
    if not race_dfs:
        raise ValueError("No race data was successfully processed")
    
    season_df = pd.concat(race_dfs, ignore_index=True)
    
    # Reorder columns to put Track at the beginning
    cols = ['Track'] + [col for col in season_df.columns if col != 'Track']
    season_df = season_df[cols]
    
    print("\nFull season dataset shape:", season_df.shape)
    
    return season_df


# wyscig
# gp_name='United States'
# df_race = build_dataset(gp_name=gp_name)
# print(df_race.head())
# df_race.to_pickle(f'{gp_name}_full_dataset.pkl')

#sezon
df_season = season_dataset()
print(df_season.head())
df_season.to_pickle('season_full_NAs.pkl')


# TODO/DO OBGADANIA
# pełny dataset robi sie około 22 minut !!!
# wyrzucanie prze isaccurate / deleted reason (metoda accurate i not_deleted z api)
# inne rzeczy z telemetri wczytywac idk
# pogoda
# uzycie tylko quicklaps
