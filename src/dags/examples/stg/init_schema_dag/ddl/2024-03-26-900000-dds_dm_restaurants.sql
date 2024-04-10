--DROP TABLE IF EXISTS dds.dm_restaurants CASCADE;
-- Create a sequence for the id column
CREATE SEQUENCE IF NOT EXISTS dds.dm_restaurants_seq START WITH 1;

-- Create the table with the specified columns
CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
    id              INT PRIMARY KEY DEFAULT nextval('dds.dm_restaurants_seq'),
    restaurant_id   VARCHAR UNIQUE NOT NULL,
    restaurant_name VARCHAR NOT NULL,
    active_from     TIMESTAMP NOT NULL,
    active_to       TIMESTAMP NOT NULL DEFAULT '2099-12-31 00:00:00'
);

-- Set the ownership of the sequence to the id column of the table
ALTER SEQUENCE dds.dm_restaurants_seq OWNED BY dds.dm_restaurants.id;