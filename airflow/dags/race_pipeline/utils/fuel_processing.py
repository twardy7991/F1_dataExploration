import pandas as pd
import numpy as np

class FuelProcessing:
    
    def __init__(self, lap_df : pd.DataFrame):
        self.lap_df = lap_df.copy()
    # isnt race length better than track lenght? feels confusing
    def _calculate_needed_fuel(self, lap_length, track_length, fuel_start=100, fuel_end=1):
        laps_per_track = int(track_length / lap_length)
        fuel_per_lap = (fuel_start - fuel_end) / laps_per_track
        return fuel_per_lap

    def _calculate_start_fuel(self, lap_number, fuel_per_lap, fuel_start=100):
        if lap_number == 1:
            return fuel_start
        else:
            return max(0, fuel_start - (lap_number - 1) * fuel_per_lap)

    def _calculate_avg_fuel(self, row, fuel_per_lap, fuel_start=100):
        lap_number = row['LapNumber']

        if lap_number == 1:
            return fuel_start
        else:
            current_fuel = self._calculate_start_fuel(lap_number, fuel_per_lap, fuel_start)
            prev_fuel = self._calculate_start_fuel(lap_number - 1, fuel_per_lap, fuel_start)
            return (current_fuel + prev_fuel) / 2

    def calculateFCL(self, lap_length : float, track_length : float, fuel_start=100):
        
        fuel_per_lap = self._calculate_needed_fuel(lap_length=lap_length, track_length=track_length, fuel_start=fuel_start)
        self.lap_df['StartFuel'] = self.lap_df.apply(lambda row: self._calculate_start_fuel(row['LapNumber'], fuel_per_lap, fuel_start), axis=1)
        
        # w srodku innej zeby trzymala zmienne, mozna na outer scope jak chcemy byc "czytelni"
        def _calculate_FCL(row, fuel_penalty=0.03):
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
        
        self.lap_df['FCL'] = self.lap_df.apply(_calculate_FCL, axis=1)

        return self.lap_df