CREATE SEQUENCE IF NOT EXISTS cdm.dm_settlement_report_seq START WITH 1;

CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
    id                       INT PRIMARY KEY DEFAULT nextval('cdm.dm_settlement_report_seq'),
    restaurant_id            INT NOT NULL,
    restaurant_name          VARCHAR NOT NULL,
    settlement_date          DATE NOT NULL CHECK (settlement_date >= '2022-01-01' AND settlement_date < '2500-01-01'),
    orders_count             INT NOT NULL DEFAULT 0 CHECK (orders_count >= 0),
    orders_total_sum         NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0),
    orders_bonus_payment_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (orders_bonus_payment_sum >= 0),
    orders_bonus_granted_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (orders_bonus_granted_sum >= 0),
    order_processing_fee     NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (orders_bonus_granted_sum >= 0),
    restaurant_reward_sum    NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (orders_bonus_granted_sum >= 0),
    CONSTRAINT dm_settlement_report_unique_restaurant_date UNIQUE (restaurant_id, settlement_date)
);

ALTER SEQUENCE cdm.dm_settlement_report_seq OWNED BY cdm.dm_settlement_report.id;