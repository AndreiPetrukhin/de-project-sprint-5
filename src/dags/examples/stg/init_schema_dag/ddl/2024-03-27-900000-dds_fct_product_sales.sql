--DROP TABLE IF EXISTS dds.fct_product_sales CASCADE;
-- Create a sequence for the id column
CREATE SEQUENCE IF NOT EXISTS dds.fct_product_sales_seq START WITH 1;

-- Create the table with the specified columns
CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
    id             INT PRIMARY KEY DEFAULT nextval('dds.fct_product_sales_seq'),
    product_id     INT REFERENCES dds.dm_products(id),
    order_id       INT REFERENCES dds.dm_orders(id),
    count          INT NOT NULL,
    price          NUMERIC(19, 5) NOT NULL,
    total_sum      NUMERIC(19, 5) NOT NULL,
    bonus_payment  NUMERIC(19, 5) NOT NULL,
    bonus_grant    NUMERIC(19, 5) NOT NULL,
    CONSTRAINT unique_product_order UNIQUE (product_id, order_id)
);

-- Set the ownership of the sequence to the id column of the table
ALTER SEQUENCE dds.fct_product_sales_seq OWNED BY dds.fct_product_sales.id;