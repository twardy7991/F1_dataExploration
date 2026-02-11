from pyspark.sql.types import *

TRACK_INFO = {
    'Bahrain Grand Prix': (5.412, 308.238),
    'Saudi Arabian Grand Prix': (6.174, 308.450),
    'Australian Grand Prix': (5.278, 306.124),
    'Azerbaijan Grand Prix': (6.003, 306.049),
    'Miami Grand Prix': (5.412, 308.326),
    'Monaco Grand Prix': (3.337, 260.286),
    'Spanish Grand Prix': (4.675, 308.424),
    'Canadian Grand Prix': (4.361, 305.270),
    'Austrian Grand Prix': (4.318, 306.452),
    'British Grand Prix': (5.891, 306.198),
    'Hungarian Grand Prix': (4.381, 306.670),
    'Belgian Grand Prix': (7.004, 308.052),
    'Dutch Grand Prix': (4.259, 306.587),
    'Italian Grand Prix': (5.793, 306.720),
    'Singapore Grand Prix': (4.940, 308.706),
    'Japanese Grand Prix': (5.807, 307.471),
    'Qatar Grand Prix': (5.419, 308.611),
    'United States Grand Prix': (5.513, 308.405),
    'Mexico City Grand Prix': (4.304, 305.354),
    'São Paulo Grand Prix': (4.309, 305.879),
    'Las Vegas Grand Prix': (6.201, 305.880),
    'Abu Dhabi Grand Prix': (5.281, 305.355),
}

LAPS_SCHEMA = StructType(
    [
        StructField("Time", DayTimeIntervalType(), False),
        StructField("Driver", StringType(), False),
        StructField("DriverNumber", StringType(), False),
        StructField("LapTime", LongType(), False),
        StructField("LapNumber", IntegerType() ,False),
        StructField("Stint", IntegerType() ,False),
        StructField("PitOutTime", DayTimeIntervalType() ,False),
        StructField("PitinTime", DayTimeIntervalType() ,False),
        StructField("Sector1Time", DayTimeIntervalType() ,False),
        StructField("Sector2Time", DayTimeIntervalType() ,False),
        StructField("Sector3Time", DayTimeIntervalType() ,False),
        StructField("Sector1SessionTime", DayTimeIntervalType() ,False),
        StructField("Sector2SessionTime", DayTimeIntervalType() ,False),
        StructField("Sector3SessionTime", DayTimeIntervalType() ,False),
        StructField("SpeedI1", DoubleType(), True),
        StructField("SpeedI2", DoubleType(), True),
        StructField("SpeedFL", DoubleType(), True),
        StructField("SpeedST", DoubleType(), True),
        StructField("IsPersonalBest", BooleanType(), False),
        StructField("Compound", StringType(), False),
        StructField("TyreLife", IntegerType(), False),
        StructField("FreshTyre", BooleanType(), False),
        StructField("Team", StringType(), False),
        StructField("LapStartTime", DayTimeIntervalType() ,False),
        StructField("LapStartDate", DayTimeIntervalType() ,False),
        StructField("TrackStatus", StringType(), False),
        StructField("Position", IntegerType(), False),
        StructField("Deleted", BooleanType(), True),
        StructField("DeletedReason", StringType(), True),
        StructField("FastF1Generated", BooleanType(), True),
        StructField("IsAccurate", BooleanType(), True),
    ]
)

TELEMETRY_SCHEMA = StructType(
[
    StructField("Date", LongType() ,False),
    StructField("SesssionTime", DayTimeIntervalType(), False),
    StructField("DriverAhead", StringType(), True),
    StructField("DistanceToDriverAhead", DoubleType(), True),
    StructField("Time", DayTimeIntervalType(), False),
    StructField("RPM", DoubleType(), False),
    StructField("Speed", DoubleType(), False),
    StructField("nGear", LongType(), False),
    StructField("Throttle", DoubleType(), False),
    StructField("Brake", BooleanType(), False),
    StructField("DRS", LongType(), False),
    StructField("Source", StringType(), True),
    StructField("Distance", DoubleType(), False),
    StructField("RelativeDistance", DoubleType(), False),
    StructField("Status", StringType(), False),
    StructField("X", DoubleType(), False),
    StructField("Y", DoubleType(), False),
    StructField("Z", DoubleType(), False),
    StructField("DriverNumber", StringType(), False),
    StructField("LapNumber", DoubleType(), False),
]
)

SCHEMA = {
    "race_data_file": LAPS_SCHEMA,
    "quali_data_file": LAPS_SCHEMA,
    "race_telemetry_file": TELEMETRY_SCHEMA,
    "quali_telemetry_file": TELEMETRY_SCHEMA
}