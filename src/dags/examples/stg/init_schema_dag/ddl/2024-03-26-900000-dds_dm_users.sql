--DROP TABLE IF EXISTS dds.dm_users;
CREATE TABLE IF NOT EXISTS dds.dm_users (
    id            SERIAL PRIMARY KEY,
    user_id     VARCHAR(256) UNIQUE NOT NULL,
    user_name  VARCHAR(256) NOT NULL,
    user_login     VARCHAR(256) NOT NULL
);