import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

log = logging.getLogger(__name__)

@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2024, 3, 26, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'products'],
    is_paused_upon_creation=False
)
def sprint5_example_dds_dm_products_dag():
    dwh_pg_connect = "PG_WAREHOUSE_CONNECTION"

    @task(task_id="extract_last_loaded_id")
    def extract_last_loaded_id():
        with PostgresHook(postgres_conn_id=dwh_pg_connect).get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT (workflow_settings::jsonb)->>'last_loaded_id' AS last_loaded_id
                    FROM dds.srv_wf_settings
                    WHERE workflow_key = 'stg_to_dds_products'
                """)
                result = cursor.fetchone()
                return int(result[0]) if result else -1

    @task(task_id="transfer_data_to_dm_products")
    def transfer_data_to_dm_products(last_loaded_id):
        with PostgresHook(postgres_conn_id=dwh_pg_connect).get_conn() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute("""
                        INSERT INTO dds.dm_products (product_id, product_name, product_price, active_from, restaurant_id)
                        SELECT DISTINCT
                            jsonb_array_elements((object_value::jsonb)->'menu')->>'_id' AS product_id,
                            jsonb_array_elements((object_value::jsonb)->'menu')->>'name' AS product_name,
                            (jsonb_array_elements((object_value::jsonb)->'menu')->>'price')::numeric AS product_price,
                            update_ts AS active_from,
                            id AS restaurant_id
                        FROM stg.ordersystem_restaurants
                        WHERE id > %s
                    """, (last_loaded_id,))

                    cursor.execute("""
                        SELECT MAX(id) FROM stg.ordersystem_restaurants
                        WHERE id > %s
                    """, (last_loaded_id,))
                    new_last_loaded_id = cursor.fetchone()[0]

                    if new_last_loaded_id:
                        cursor.execute("""
                            INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                            VALUES ('stg_to_dds_products', jsonb_build_object('last_loaded_id', %s))
                            ON CONFLICT (workflow_key) DO UPDATE SET
                                workflow_settings = jsonb_build_object('last_loaded_id', %s)
                        """, (new_last_loaded_id, new_last_loaded_id,))

                    conn.commit()
                    log.info(f"Transferred data to dds.dm_products and updated last_loaded_id to {new_last_loaded_id}.")
                except Exception as e:
                    log.error(f"Error transferring data: {e}")
                    conn.rollback()
                    raise e

    last_loaded_id = extract_last_loaded_id()
    transfer_data_to_dm_products(last_loaded_id)

sprint5_example_dds_products_dag = sprint5_example_dds_dm_products_dag()