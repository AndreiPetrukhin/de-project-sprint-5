--DROP TABLE IF EXISTS stg.bonussystem_events;
CREATE TABLE IF NOT EXISTS stg.bonussystem_events (
    id INT PRIMARY KEY NOT NULL,
    event_ts TIMESTAMP NOT NULL,
    event_type TEXT NOT NULL,
    event_value TEXT NOT NULL,
    CHECK (event_type IN ('user_rank', 'user_balance', 'bonus_transaction'))
);