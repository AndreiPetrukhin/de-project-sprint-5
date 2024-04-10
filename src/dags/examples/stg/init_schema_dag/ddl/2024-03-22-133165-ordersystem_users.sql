--DROP TABLE IF EXISTS stg.ordersystem_users;
CREATE TABLE IF NOT EXISTS stg.ordersystem_users (
    id            SERIAL PRIMARY KEY,
    object_id     VARCHAR UNIQUE NOT NULL,
    object_value  TEXT NOT NULL,
    update_ts     TIMESTAMP WITHOUT TIME ZONE NOT NULL
);