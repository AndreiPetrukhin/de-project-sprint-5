--DROP TABLE IF EXISTS dds.dm_orders CASCADE;
-- Create a sequence for the id column
CREATE SEQUENCE IF NOT EXISTS dds.dm_orders_seq START WITH 1;

-- Create the table with the specified columns
CREATE TABLE IF NOT EXISTS dds.dm_orders (
    id            INT PRIMARY KEY DEFAULT nextval('dds.dm_orders_seq'),
    order_key     VARCHAR NOT NULL,
    order_status  VARCHAR NOT NULL,
    restaurant_id INT REFERENCES dds.dm_restaurants(id),
    timestamp_id  INT REFERENCES dds.dm_timestamps(id),
    user_id       INT REFERENCES dds.dm_users(id),
    CONSTRAINT unique_order_key_status UNIQUE (order_key, order_status)
);

-- Set the ownership of the sequence to the id column of the table
ALTER SEQUENCE dds.dm_orders_seq OWNED BY dds.dm_orders.id;