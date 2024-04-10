--DROP TABLE IF EXISTS stg.ordersystem_orders;
CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
    id            SERIAL PRIMARY KEY,
    object_id     VARCHAR NOT NULL,
    object_value  TEXT NOT NULL,
    update_ts     TIMESTAMP WITHOUT TIME ZONE NOT NULL
);