# F1 Data Exploration Project

### Project Overview  
This project explores Formula One data with a focus on data extraction, evaluation, prediction, and explainable models. Currently divided into three main components:
1. Telemetry analysis
2. Tyre life and fuel analysis

## Data Model

### Variables
The model currently uses 13 variables for predictions:

**Dependent Variables:**
- `LapTime` - Raw lap time
- `FCL` (Fuel Corrected Lap Time) - Lap time with fuel load influence subtracted

**Independent Variables:**
| Variable | Description |
|----------|-------------|
| `Track` | Track name where lap was recorded |
| `Driver` | Driver code (e.g., VER, NOR) |
| `Compound` | Tyre type (e.g., SOFT, HARD) |
| `TyreLife` | Tyre wear (number of laps driven on this compound) |
| `StartFuel` | Percentage of fuel remaining relative to start value |
| `SpeedI1` | Speed trap sector 1 [km/h] |
| `SpeedI2` | Speed trap sector 2 [km/h] |
| `SpeedFL` | Finish line speed trap [km/h] |
| `SumLonAcc` | Sum of absolute longitudinal acceleration values from telemetry |
| `SumLatAcc` | Sum of absolute lateral acceleration values from telemetry |
| `MeanLapSpeed` | Mean speed from telemetry data |
| `LonDistanceDTW` | DTW value for longitudinal accelerations relative to best qualifying lap |
| `LatDistanceDTW` | DTW value for lateral accelerations relative to best qualifying lap |

## Project Structure

### Telemetry Analysis
- Located in `telemetry_data_preprocessing.py`
- Provides tools to compute essential metrics

### Fuel and Tyre Analysis
- Handled in `preliminary_data_analysis.py`
- Computes fuel level and tyre wear metrics

### Data Loading and Caching
- Uses FastF1 library to create a local cache
- Stores single-race telemetry data in individual files
- Implemented in `loader.py`

### Dataset Creation
- Combines race telemetries into a single `.pkl` file
- Calculates metrics across all seasons
- Implemented in `dataset_creator.py`

## Example Analysis
The models section includes:
- Basic regression models
- Advanced forest models
- Simple neural networks

See implementations in:
- `regression_forest.ipynb`
- `xgboost_keras.ipynb`
