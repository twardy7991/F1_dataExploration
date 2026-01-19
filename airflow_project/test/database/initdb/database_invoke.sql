\c test_f1_db

DROP TABLE IF EXISTS session_data;

CREATE TABLE session_data (
    Year SMALLINT,
    Race TEXT,
    LapNumber SMALLINT,          -- int16
    Driver TEXT,                 -- string
    Compound TEXT,               -- string
    TyreLife SMALLINT,           -- int8
    StartFuel DOUBLE PRECISION,  -- double
    FCL DOUBLE PRECISION,        -- double (keeping numeric, not boolean)
    LapTime DOUBLE PRECISION, /*LapTime INTERVAL*/          -- duration[ns]
    SpeedI1 DOUBLE PRECISION,    -- double
    SpeedI2 DOUBLE PRECISION,    -- double
    SpeedFL DOUBLE PRECISION,    -- double
    SumLonAcc DOUBLE PRECISION,  -- double
    SumLatAcc DOUBLE PRECISION,  -- double
    MeanLapSpeed DOUBLE PRECISION
);

-- INSERT INTO session_data (
--     Year,
--     Race,
--     LapNumber,
--     Driver,
--     Compound,
--     TyreLife,
--     StartFuel,
--     FCL,
--     LapTime,
--     SpeedI1,
--     SpeedI2,
--     SpeedFL,
--     SumLonAcc,
--     SumLatAcc,
--     MeanLapSpeed
-- )
-- VALUES (
--     2024,
--     'Bahrain',
--     12,
--     'VER',
--     'SOFT',
--     7,
--     32.5,
--     0.0,
--     1234567890,
--     295.4,
--     301.2,
--     312.8,
--     145.73,
--     138.91,
--     201.6
-- );

