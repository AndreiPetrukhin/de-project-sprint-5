import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

log = logging.getLogger(__name__)

@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2024, 3, 26, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'delivery_details'],
    is_paused_upon_creation=False
)
def sprint5_example_dds_dm_delivery_details_dag():
    dwh_pg_connect = "PG_WAREHOUSE_CONNECTION"

    @task(task_id="extract_last_loaded_id")
    def extract_last_loaded_id():
        with PostgresHook(postgres_conn_id=dwh_pg_connect).get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT (workflow_settings::jsonb)->>'last_loaded_id' AS last_loaded_id
                    FROM dds.srv_wf_settings
                    WHERE workflow_key = 'stg_to_dds_delivery_details'
                """)
                result = cursor.fetchone()
                return result[0] if result else '1945-05-09 00:00:00'

    @task(task_id="transfer_data_to_dm_delivery_details")
    def transfer_data_to_dm_delivery_details(last_loaded_id):
        with PostgresHook(postgres_conn_id=dwh_pg_connect).get_conn() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute("""
                        INSERT INTO dds.dm_delivery_details (delivery_id, courier_id, order_id)
                        SELECT DISTINCT
                            dd.id AS delivery_id,
                            dc.id AS courier_id,
                            do2.id AS order_id
                        FROM stg.api_deliveries ad
                        JOIN dds.dm_deliveries dd ON dd.delivery_id = (ad.object_value->>'delivery_id')::text
                        -- looks like not all couriers are tracked in api_couriers db -> left join
                        LEFT JOIN dds.dm_couriers dc ON dc.courier_id = (ad.object_value->>'courier_id')::text
                        JOIN dds.dm_orders do2 ON do2.order_key = (ad.object_value->>'order_id')::text
                        WHERE (ad.object_value->>'delivery_ts')::timestamp > %s::timestamp
                        ON CONFLICT (delivery_id) DO UPDATE SET
                            courier_id = EXCLUDED.courier_id,
                            order_id = EXCLUDED.order_id
                    """, (last_loaded_id,))

                    cursor.execute("""
                        SELECT MAX((ad.object_value->>'delivery_ts')::timestamp) FROM stg.api_deliveries ad
                        WHERE (ad.object_value->>'delivery_ts')::timestamp > %s::timestamp
                    """, (last_loaded_id,))
                    result = cursor.fetchone()
                    new_last_loaded_id = result[0] if result else None

                    if new_last_loaded_id:
                        cursor.execute("""
                            INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                            VALUES ('stg_to_dds_delivery_details', jsonb_build_object('last_loaded_id', %s))
                            ON CONFLICT (workflow_key) DO UPDATE SET
                                workflow_settings = jsonb_build_object('last_loaded_id', %s)
                        """, (new_last_loaded_id, new_last_loaded_id,))

                    conn.commit()
                    log.info(f"Transferred data to dds.dm_delivery_details and updated last_loaded_id to {new_last_loaded_id}.")
                except Exception as e:
                    log.error(f"Error transferring data: {e}")
                    conn.rollback()
                    raise e

    last_loaded_id = extract_last_loaded_id()
    transfer_data_to_dm_delivery_details(last_loaded_id)

sprint5_example_dds_delivery_details_dag = sprint5_example_dds_dm_delivery_details_dag()