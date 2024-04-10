-- DROP TABLE IF EXISTS dds.dm_products;
-- Create a sequence for the id column
CREATE SEQUENCE IF NOT EXISTS dds.dm_products_id_seq START WITH 1;

-- Create the table with the specified columns
CREATE TABLE IF NOT EXISTS dds.dm_products (
    id             INT PRIMARY KEY DEFAULT nextval('dds.dm_products_id_seq'),
    product_id     VARCHAR NOT NULL,
    product_name   TEXT NOT NULL,
    product_price  NUMERIC NOT NULL,
    active_from    TIMESTAMP NOT NULL,
    active_to      TIMESTAMP NOT NULL DEFAULT '2099-12-31 00:00:00',
    restaurant_id  INT REFERENCES dds.dm_restaurants(id)
);

-- Set the ownership of the sequence to the id column of the table
ALTER SEQUENCE dds.dm_products_id_seq OWNED BY dds.dm_products.id;