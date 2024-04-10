-- Drop the table if it already exists
-- DROP TABLE IF EXISTS dds.fct_deliveries CASCADE;

-- Create a sequence for the id column
CREATE SEQUENCE IF NOT EXISTS dds.fct_deliveries_seq START WITH 1;

-- Create the table with the specified columns
CREATE TABLE IF NOT EXISTS dds.fct_deliveries (
    id              INT PRIMARY KEY DEFAULT nextval('dds.fct_deliveries_seq'),
    delivery_id     INT UNIQUE NOT NULL,
    rate            INT CHECK (rate BETWEEN 1 AND 5),
    sum             NUMERIC(10, 2) NOT NULL,
    tip_sum         NUMERIC(10, 2) NOT NULL
);

-- Set the ownership of the sequence to the id column of the table
ALTER SEQUENCE dds.fct_deliveries_seq OWNED BY dds.fct_deliveries.id;
