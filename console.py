if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Download and save F1 2023 telemetry data")
    parser.add_argument('--gp', help='Grand Prix name or round number (1-22)', type = str | int)
    parser.add_argument('--session', default='R', help='Session type (e.g., R, Q, FP1)', type = str)
    parser.add_argument('--output', default=None, help='Output filename' , type = str)
    parser.add_argument('--all', action='store_true', help='Download all races for the season', type = bool)
    args : argparse.Namespace = parser.parse_args()
    
    extractor = APIExtractor()
    def download_all_season(session_type : str) -> None:
        
        gps = [
            'Bahrain', 'Saudi', 'Australia', 'Azerbaijan', 'Miami', 'Monaco', 'Spain', 'Canada', 'Austria',
            'Great Britain', 'Hungary', 'Belgium', 'Netherlands', 'Italy', 'Singapore', 'Japan', 'Qatar',
            'USA', 'Mexico', 'Brazil', 'Las Vegas', 'Abu Dhabi'
        ]
        for gp in gps:
            print(f"Downloading {gp} {session_type}...")
            try:
                extractor.save_session_telemetry(gp, session_type)
            except Exception as e:
                print(f"Failed for {gp}: {e}")
    
    if args.all:
        download_all_season(args.session)
    elif args.gp:
        extractor.save_session_telemetry(args.gp, args.session, args.output)
    else:
        print("Please provide --gp for a single race or --all for the whole season.")