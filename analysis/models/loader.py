#view only so we can compare

import argparse
import pickle
import os
import fastf1
from fastf1.core import Telemetry
import pandas as pd


def download_telemetry(gp, session_type, output_file=None):
    """
    Available GP names:

    Bahrain, Saudi, Australia, Azerbaijan, Miami, 
    Monaco, Spain, Canada, Austria, Great Britain, 
    Hungary, Belgium, Netherlands, Italy, Singapore, Japan, 
    Qatar, USA, Mexico, Brazil, Las Vegas, Abu Dhabi
    """
    if output_file is None:
        output_file = f'telemetry_{gp}_{session_type}.pkl'
    if not output_file.endswith('.pkl'):
        output_file += '.pkl'
        
    if not os.path.exists('cache'):
        os.makedirs('cache')
    fastf1.Cache.enable_cache('cache')
    

    try:
        if gp.isdigit():
            session = fastf1.get_session(2023, int(gp), session_type)
        else:
            session = fastf1.get_session(2023, gp, session_type)
        
        session.load(telemetry=True)
        df = session.laps

        telemetry_data = [
        lap.get_telemetry().assign(LapNumber=lap['LapNumber'])
        for _, lap in df.iterrows()
        if lap.get_telemetry() is not None and not lap.get_telemetry().empty
    ]

        telemetry_df = pd.concat(telemetry_data, ignore_index=True)
        telemetry_df.to_pickle(output_file, protocol=pickle.HIGHEST_PROTOCOL)
        
        print(f"Telemetry saved to {output_file}")
        
    except Exception as e:
        print(f"Error: {str(e)}")


def download_all_season(session_type):
    gps = [
        'Bahrain', 'Saudi', 'Australia', 'Azerbaijan', 'Miami', 'Monaco', 'Spain', 'Canada', 'Austria',
        'Great Britain', 'Hungary', 'Belgium', 'Netherlands', 'Italy', 'Singapore', 'Japan', 'Qatar',
        'USA', 'Mexico', 'Brazil', 'Las Vegas', 'Abu Dhabi'
    ]
    for gp in gps:
        print(f"Downloading {gp} {session_type}...")
        try:
            download_telemetry(gp, session_type)
        except Exception as e:
            print(f"Failed for {gp}: {e}")


# HELPER DO WRZUCENIA DO NOTEBOOKA
def load_telemetry(pickle_file):
    tel = pd.read_pickle(pickle_file)
    telemetry = Telemetry(tel)
    telemetry.has_channel = lambda c: c in telemetry.columns  
    telemetry['Time'] = pd.to_timedelta(telemetry['Time'])
    return telemetry

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and save F1 2023 telemetry data")
    parser.add_argument('--gp', help='Grand Prix name or round number (1-22)')
    parser.add_argument('--session', default='R', help='Session type (e.g., R, Q, FP1)')
    parser.add_argument('--output', default=None, help='Output filename')
    parser.add_argument('--all', action='store_true', help='Download all races for the season')
    args = parser.parse_args()

    if args.all:
        download_all_season(args.session)
    elif args.gp:
        download_telemetry(args.gp, args.session, args.output)
    else:
        print("Please provide --gp for a single race or --all for the whole season.")