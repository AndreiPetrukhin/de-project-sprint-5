-- /Users/apetrukh/Desktop/de_yandex/s5-lessons/dags/examples/cdm/sql/dm_courier_ledger.sql
INSERT INTO cdm.dm_courier_ledger (
    courier_id,
    courier_name,
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    order_processing_fee,
    rate_avg,
    courier_tips_sum,
    courier_order_sum,
    courier_reward_sum
)
WITH couriers_stat AS (
    SELECT
        dc.courier_id AS courier_id,
        dc.courier_name AS courier_name,
        dt."year" AS settlement_year,
        dt."month" AS settlement_month,
        COUNT(dd.order_id) AS orders_count,
        SUM(fd.sum) AS orders_total_sum,
        SUM(fd.sum) * 0.25 AS order_processing_fee,
        AVG(fd.rate) AS rate_avg,
        SUM(fd.tip_sum) AS courier_tips_sum
    FROM dds.fct_deliveries fd
    JOIN dds.dm_delivery_details dd ON dd.delivery_id = fd.delivery_id
    JOIN dds.dm_couriers dc ON dc.id = dd.courier_id
    JOIN dds.dm_deliveries d ON d.id = dd.delivery_id
    JOIN dds.dm_timestamps dt ON dt.id = d.delivery_ts
    WHERE dt."year" = {execution_date.year}
        AND dt."month" = {execution_date.month}
    GROUP BY dc.courier_id, dc.courier_name, dt."year", dt."month"
)
SELECT
    courier_id,
    courier_name,
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    order_processing_fee,
    rate_avg,
    courier_tips_sum,
    CASE
        WHEN rate_avg < 4 THEN GREATEST(orders_total_sum * 0.05, 100 * orders_count)
        WHEN rate_avg >= 4 AND rate_avg < 4.5 THEN GREATEST(orders_total_sum * 0.07, 150 * orders_count)
        WHEN rate_avg >= 4.5 AND rate_avg < 4.9 THEN GREATEST(orders_total_sum * 0.08, 175 * orders_count)
        ELSE GREATEST(orders_total_sum * 0.10, 200 * orders_count)
    END AS courier_order_sum,
    CASE
        WHEN rate_avg < 4 THEN GREATEST(orders_total_sum * 0.05, 100 * orders_count)
        WHEN rate_avg >= 4 AND rate_avg < 4.5 THEN GREATEST(orders_total_sum * 0.07, 150 * orders_count)
        WHEN rate_avg >= 4.5 AND rate_avg < 4.9 THEN GREATEST(orders_total_sum * 0.08, 175 * orders_count)
        ELSE GREATEST(orders_total_sum * 0.10, 200 * orders_count)
    END + courier_tips_sum * 0.95 AS courier_reward_sum
FROM couriers_stat
ON CONFLICT (courier_id, settlement_year, settlement_month)
DO UPDATE SET
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    rate_avg = EXCLUDED.rate_avg,
    courier_tips_sum = EXCLUDED.courier_tips_sum,
    courier_order_sum = EXCLUDED.courier_order_sum,
    courier_reward_sum = EXCLUDED.courier_reward_sum;