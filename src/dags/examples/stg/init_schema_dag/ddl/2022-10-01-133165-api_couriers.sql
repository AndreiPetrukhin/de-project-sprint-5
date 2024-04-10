-- Drop the table if it already exists
-- DROP TABLE IF EXISTS stg.api_couriers CASCADE;

-- Create a sequence for the id column
CREATE SEQUENCE IF NOT EXISTS stg.api_couriers_seq START WITH 1;

-- Create the table with the specified columns
CREATE TABLE IF NOT EXISTS stg.api_couriers (
    id           INT PRIMARY KEY DEFAULT nextval('stg.api_couriers_seq'),
    courier_id   VARCHAR UNIQUE NOT NULL,
    object_value JSONB NOT NULL
);

-- Set the ownership of the sequence to the id column of the table
ALTER SEQUENCE stg.api_couriers_seq OWNED BY stg.api_couriers.id;