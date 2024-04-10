import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

log = logging.getLogger(__name__)

@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2024, 3, 26, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'product_sales'],
    is_paused_upon_creation=False
)
def sprint5_example_dds_fct_product_sales_dag():
    dwh_pg_connect = "PG_WAREHOUSE_CONNECTION"

    @task(task_id="extract_last_loaded_id")
    def extract_last_loaded_id():
        with PostgresHook(postgres_conn_id=dwh_pg_connect).get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT (workflow_settings::jsonb)->>'last_loaded_id' AS last_loaded_id
                    FROM dds.srv_wf_settings
                    WHERE workflow_key = 'stg_to_dds_product_sales'
                """)
                result = cursor.fetchone()
                return int(result[0]) if result else -1

    @task(task_id="transfer_data_to_fct_product_sales")
    def transfer_data_to_fct_product_sales(last_loaded_id):
        with PostgresHook(postgres_conn_id=dwh_pg_connect).get_conn() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute("""
                        WITH cte AS (
                            SELECT
                                jsonb_array_elements((o.object_value::jsonb)->'order_items')->>'id' AS product_id,
                                (o.object_value::jsonb)->>'_id' AS order_id,
                                (jsonb_array_elements((o.object_value::jsonb)->'order_items')->>'quantity')::int AS "count",
                                (jsonb_array_elements((o.object_value::jsonb)->'order_items')->>'price')::numeric(19,5) AS price
                            FROM
                                stg.ordersystem_orders o
                            WHERE o.id > %s AND (object_value::jsonb)->>'final_status'::text IN ('CLOSED')
                        ),
                        bonus_transactoins as (
                        SELECT
						    event_value::jsonb->>'order_id' AS order_id,
						    jsonb_array_elements(event_value::jsonb->'product_payments')->>'product_id' AS product_id,
						    (jsonb_array_elements(event_value::jsonb->'product_payments')->>'bonus_payment')::numeric AS bonus_payment,
						    (jsonb_array_elements(event_value::jsonb->'product_payments')->>'bonus_grant')::numeric AS bonus_grant
						FROM stg.bonussystem_events
						where
							event_type = 'bonus_transaction'
						)
                        INSERT INTO dds.fct_product_sales (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                        SELECT
                            dp.id AS product_id,
                            dmo.id AS order_id,
                            MAX(cte."count") as "count",
                            MAX(cte.price) as price,
                            MAX(cte."count") *  MAX(cte.price) AS total_sum,
                            MAX(coalesce(bt.bonus_payment, 0)) as bonus_payment,
                            MAX(coalesce(bt.bonus_grant, 0)) as bonus_grant
                        FROM cte
                        left JOIN dds.dm_products dp ON dp.product_id = cte.product_id
                        left JOIN dds.dm_orders dmo ON dmo.order_key = cte.order_id
                        left JOIN bonus_transactoins bt on bt.order_id = cte.order_id and bt.product_id = cte.product_id
                        GROUP BY 
                            dp.id,
                            dmo.id
                        ON CONFLICT (product_id, order_id) DO UPDATE SET
                            count = EXCLUDED.count,
                            price = EXCLUDED.price,
                            total_sum = EXCLUDED.total_sum,
                            bonus_payment = EXCLUDED.bonus_payment,
                            bonus_grant = EXCLUDED.bonus_grant
                    """, (last_loaded_id,))

                    cursor.execute("""
                        SELECT MAX(id) FROM stg.ordersystem_orders
                        WHERE id > %s
                    """, (last_loaded_id,))
                    new_last_loaded_id = cursor.fetchone()[0]

                    if new_last_loaded_id:
                        cursor.execute("""
                            INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                            VALUES ('stg_to_dds_product_sales', jsonb_build_object('last_loaded_id', %s))
                            ON CONFLICT (workflow_key) DO UPDATE SET
                                workflow_settings = jsonb_build_object('last_loaded_id', %s)
                        """, (new_last_loaded_id, new_last_loaded_id, ))

                    conn.commit()
                    log.info(f"Transferred data to dds.fct_product_sales and updated last_loaded_id to {new_last_loaded_id}.")
                except Exception as e:
                    log.error(f"Error transferring data: {e}")
                    conn.rollback()
                    raise e

    last_loaded_id = extract_last_loaded_id()
    transfer_data_to_fct_product_sales(last_loaded_id)

sprint5_example_dds_product_sales_dag = sprint5_example_dds_fct_product_sales_dag()