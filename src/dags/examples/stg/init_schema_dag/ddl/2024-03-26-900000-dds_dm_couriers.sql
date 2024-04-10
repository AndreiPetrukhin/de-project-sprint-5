-- Drop the table and sequence if they exist
--DROP TABLE IF EXISTS dds.dm_couriers CASCADE;
--DROP SEQUENCE IF EXISTS dds.dm_couriers_seq;

-- Create a sequence for the id column
CREATE SEQUENCE IF NOT EXISTS dds.dm_couriers_seq START WITH 1;

-- Create the table with the specified columns
CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id           INT PRIMARY KEY DEFAULT nextval('dds.dm_couriers_seq'),
    courier_id   VARCHAR UNIQUE NOT NULL,
    courier_name VARCHAR NOT NULL
);

-- Set the ownership of the sequence to the id column of the table
ALTER SEQUENCE dds.dm_couriers_seq OWNED BY dds.dm_couriers.id;