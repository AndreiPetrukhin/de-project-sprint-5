import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

log = logging.getLogger(__name__)

@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2024, 3, 28, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'settlement_report'],
    is_paused_upon_creation=False
)
def sprint5_example_cdm_settlement_report_dag():
    dwh_pg_connect = "PG_WAREHOUSE_CONNECTION"

    @task(task_id="transfer_data_to_dm_settlement_report")
    def transfer_data_to_dm_settlement_report():
        with PostgresHook(postgres_conn_id=dwh_pg_connect).get_conn() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute("""
                        INSERT INTO cdm.dm_settlement_report (
                            restaurant_id,
                            restaurant_name,
                            settlement_date,
                            orders_count,
                            orders_total_sum,
                            orders_bonus_payment_sum,
                            orders_bonus_granted_sum,
                            order_processing_fee,
                            restaurant_reward_sum
                        )
                        with closed_orders as (select distinct
                            co.id AS orders_id,
                            co.restaurant_id,
                            co.timestamp_id
                        FROM
                            dds.dm_orders co
                        where 
                        	co.order_status = 'CLOSED'
                        ),
                        orders_info as (select distinct
                            co.orders_id AS orders_id,
                            sum(o.total_sum) as total_sum,
                            sum(o.bonus_payment) as bonus_payment,
                            sum(o.bonus_grant) as bonus_grant,
                            co.restaurant_id,
                            co.timestamp_id
                        FROM
                            dds.fct_product_sales o
                            JOIN closed_orders co ON co.orders_id = o.order_id
                        group by 
                        	co.orders_id,
                        	co.restaurant_id,
                        	co.timestamp_id
                        )
                        select distinct
                            r.id AS restaurant_id,
                            r.restaurant_name,
                            dt.date AS settlement_date,
                            COUNT(oi.orders_id) AS orders_count,
                            SUM(oi.total_sum) AS orders_total_sum,
                            SUM(oi.bonus_payment) AS orders_bonus_payment_sum,
                            SUM(oi.bonus_grant) AS orders_bonus_granted_sum,
                            SUM(oi.total_sum) * 0.25 AS order_processing_fee,
                            SUM(oi.total_sum) - SUM(oi.bonus_payment) - SUM(oi.total_sum) * 0.25 AS restaurant_reward_sum
                        FROM
                            orders_info oi
                            JOIN dds.dm_restaurants r ON oi.restaurant_id = r.id
                            JOIN dds.dm_timestamps dt on dt.id = oi.timestamp_id
                        WHERE
                            dt.date >= (SELECT 
                                            COALESCE(MAX(settlement_date), '2022-01-01') 
                                        FROM cdm.dm_settlement_report
                                        WHERE restaurant_id = r.id)
                        GROUP BY
                            r.id,
                            r.restaurant_name,
                            dt.date
                        ON CONFLICT (restaurant_id, settlement_date)
                        DO UPDATE SET
                            orders_count = EXCLUDED.orders_count,
                            orders_total_sum = EXCLUDED.orders_total_sum,
                            orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                            orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                            order_processing_fee = EXCLUDED.order_processing_fee,
                            restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                    """)

                    conn.commit()
                    log.info("Transferred data to cdm.dm_settlement_report.")
                except Exception as e:
                    log.error(f"Error transferring data: {e}")
                    conn.rollback()
                    raise e

    transfer_data_to_dm_settlement_report()

sprint5_example_dm_settlement_report_dag = sprint5_example_cdm_settlement_report_dag()
