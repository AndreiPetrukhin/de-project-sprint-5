-- Drop the table if it already exists
-- DROP TABLE IF EXISTS dds.dm_deliveries CASCADE;

-- Create a sequence for the id column
CREATE SEQUENCE IF NOT EXISTS dds.dm_deliveries_seq START WITH 1;

-- Create the table with the specified columns
CREATE TABLE IF NOT EXISTS dds.dm_deliveries (
    id              INT PRIMARY KEY DEFAULT nextval('dds.dm_deliveries_seq'),
    delivery_id     VARCHAR UNIQUE NOT NULL,
    address         VARCHAR NOT NULL,
    delivery_ts     INT NOT NULL
);

-- Set the ownership of the sequence to the id column of the table
ALTER SEQUENCE dds.dm_deliveries_seq OWNED BY dds.dm_deliveries.id;
