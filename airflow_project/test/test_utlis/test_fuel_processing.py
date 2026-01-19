from airflow_project.dags.utils import FuelProcessing

def test_calculate_needed_fuel(fuel_processing : FuelProcessing):
    
    lap_length = 10
    track_length = 100
    
    fuel_per_lap = fuel_processing._calculate_needed_fuel(lap_length=lap_length, track_length=track_length)
    
    assert fuel_per_lap == 9.9

def test_calculate_start_fuel(fuel_processing : FuelProcessing):
    
    fuel_per_lap = 1.0
    lap_number = 21
    
    start_fuel = fuel_processing._calculate_start_fuel(lap_number=lap_number, fuel_per_lap=fuel_per_lap)

    assert start_fuel == 80

def test_calculate_avg_fuel(fuel_processing : FuelProcessing):
    pass