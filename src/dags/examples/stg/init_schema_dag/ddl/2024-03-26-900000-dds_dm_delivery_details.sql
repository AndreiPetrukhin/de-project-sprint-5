-- Drop the table if it exists
--DROP TABLE IF EXISTS dds.dm_delivery_details CASCADE;

-- Create a sequence for the id column
CREATE SEQUENCE IF NOT EXISTS dds.dm_delivery_details_seq START WITH 1;

-- Create the table with the specified columns
CREATE TABLE IF NOT EXISTS dds.dm_delivery_details (
    id          INT PRIMARY KEY DEFAULT nextval('dds.dm_delivery_details_seq'),
    delivery_id INT UNIQUE NOT NULL REFERENCES dds.dm_deliveries(id),
    courier_id  INT REFERENCES dds.dm_couriers(id),
    order_id    INT NOT NULL REFERENCES dds.dm_orders(id)
);

-- Set the ownership of the sequence to the id column of the table
ALTER SEQUENCE dds.dm_delivery_details_seq OWNED BY dds.dm_delivery_details.id;
