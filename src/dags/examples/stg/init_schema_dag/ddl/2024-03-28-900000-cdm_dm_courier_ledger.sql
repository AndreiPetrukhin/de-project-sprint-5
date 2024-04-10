CREATE SEQUENCE IF NOT EXISTS cdm.dm_courier_ledger_seq START WITH 1;

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id                     INT PRIMARY KEY DEFAULT nextval('cdm.dm_courier_ledger_seq'),
    courier_id             VARCHAR NOT NULL,
    courier_name           VARCHAR NOT NULL,
    settlement_year        INT NOT NULL,
    settlement_month       INT NOT NULL,
    orders_count           INT NOT NULL DEFAULT 0 CHECK (orders_count >= 0),
    orders_total_sum       NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0),
    order_processing_fee   NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0),
    courier_order_sum      NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= 0),
    courier_reward_sum     NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= 0),
    courier_tips_sum       NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= 0),
    rate_avg               NUMERIC(3, 2) NOT NULL CHECK (rate_avg >= 0 AND rate_avg <= 5),
    CONSTRAINT cdm_dm_courier_ledger_unique_courier_date UNIQUE (courier_id, settlement_year, settlement_month)
);

ALTER SEQUENCE cdm.dm_courier_ledger_seq OWNED BY cdm.dm_courier_ledger.id;