--DROP TABLE IF EXISTS stg.ordersystem_restaurants;
CREATE TABLE IF NOT EXISTS stg.ordersystem_restaurants (
    id            INTEGER NOT NULL,
    object_id     VARCHAR NOT NULL,
    object_value  TEXT NOT NULL,
    update_ts     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (id)
);