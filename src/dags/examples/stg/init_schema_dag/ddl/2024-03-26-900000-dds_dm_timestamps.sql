--DROP TABLE IF EXISTS dds.dm_timestamps;
-- Create a sequence for the id column
CREATE SEQUENCE IF NOT EXISTS dds.dm_timestamps_id_seq START WITH 1;

-- Create the table with the id column defaulting to the next value from the sequence
CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
    id    INT PRIMARY KEY DEFAULT nextval('dds.dm_timestamps_id_seq'),
    ts    TIMESTAMP UNIQUE NOT NULL,
    year  INT GENERATED ALWAYS AS (EXTRACT(YEAR FROM ts)) STORED,
    month INT GENERATED ALWAYS AS (EXTRACT(MONTH FROM ts)) STORED,
    day   INT GENERATED ALWAYS AS (EXTRACT(DAY FROM ts)) STORED,
    date  DATE GENERATED ALWAYS AS (ts::DATE) STORED,
    time  TIME GENERATED ALWAYS AS (ts::TIME) STORED
);

-- Set the ownership of the sequence to the id column of the table
ALTER SEQUENCE dds.dm_timestamps_id_seq OWNED BY dds.dm_timestamps.id;
