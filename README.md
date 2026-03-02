# F1 Data Exploration Project

### Project Overview  
This project provides tools for fetching and processing F1 telemetry data. It aims to simulate a real-world scenario and uses Spark and Airflow to handle large amounts of data. The architecture remains flexible due to the use of multiple Docker containers that work together to provide a controllable data processing stream, including fetching, processing, and saving data to a database.

## Project Structure

### Airflow
- Located in `\airflow_project`
- Orchestrates fetching, computations and storage of the data

### Spark Computations
- Handled in `\spark`
- Computes tyre and acceleration metrics

### Storage
- Managed in `\database`
- Holds a PostgreSQL database for storage of computed lap and telemetry data

### Dashboard
- Loacated in `\dashboard`
- Prototype dashboard for fetching and presenting insights from processed data (still in progress)

### Telemetry and Fuel analysis
- In `\analysis`
- Currently prototypes, not integrated into data stream managed by airflow
- The models section includes:
  - Basic regression models
  - Advanced forest models
  - Simple neural networks
