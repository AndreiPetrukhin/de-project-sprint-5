import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

log = logging.getLogger(__name__)

@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2024, 3, 26, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'fct_delivery'],
    is_paused_upon_creation=False
)
def sprint5_example_dds_fct_delivery_dag():
    dwh_pg_connect = "PG_WAREHOUSE_CONNECTION"

    @task(task_id="extract_last_loaded_id")
    def extract_last_loaded_id():
        with PostgresHook(postgres_conn_id=dwh_pg_connect).get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT (workflow_settings::jsonb)->>'last_loaded_id' AS last_loaded_id
                    FROM dds.srv_wf_settings
                    WHERE workflow_key = 'stg_to_dds_fct_delivery'
                """)
                result = cursor.fetchone()
                return result[0] if result else '1945-05-09 00:00:00.000'

    @task(task_id="transfer_data_to_fct_delivery")
    def transfer_data_to_fct_delivery(last_loaded_id):
        with PostgresHook(postgres_conn_id=dwh_pg_connect).get_conn() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute("""
                        INSERT INTO dds.fct_deliveries (delivery_id, sum, rate, tip_sum)
                        SELECT
                            dd.id AS delivery_id,
                            (ad.object_value->>'sum')::numeric(10, 2) AS sum,
                            (ad.object_value->>'rate')::int AS rate,
                            (ad.object_value->>'tip_sum')::numeric(10, 2) AS tip_sum
                        FROM stg.api_deliveries ad
                        JOIN dds.dm_deliveries dd ON dd.delivery_id = (ad.object_value->>'delivery_id')::text
                        WHERE (ad.object_value->>'delivery_ts')::timestamp > %s
                        ON CONFLICT (delivery_id) DO UPDATE SET
                            sum = EXCLUDED.sum,
                            rate = EXCLUDED.rate,
                            tip_sum = EXCLUDED.tip_sum
                    """, (last_loaded_id,))

                    cursor.execute("""
                        SELECT MAX((ad.object_value->>'delivery_ts')::timestamp) FROM stg.api_deliveries ad
                        WHERE (ad.object_value->>'delivery_ts')::timestamp > %s
                    """, (last_loaded_id,))
                    result = cursor.fetchone()
                    new_last_loaded_id = result[0] if result else None

                    if new_last_loaded_id:
                        cursor.execute("""
                            INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                            VALUES ('stg_to_dds_fct_delivery', jsonb_build_object('last_loaded_id', %s))
                            ON CONFLICT (workflow_key) DO UPDATE SET
                                workflow_settings = jsonb_build_object('last_loaded_id', %s)
                        """, (new_last_loaded_id, new_last_loaded_id,))

                    conn.commit()
                    log.info(f"Transferred data to dds.fct_delivery and updated last_loaded_id to {new_last_loaded_id}.")
                except Exception as e:
                    log.error(f"Error transferring data: {e}")
                    conn.rollback()
                    raise e

    last_loaded_id = extract_last_loaded_id()
    transfer_data_to_fct_delivery(last_loaded_id)

dds_fct_delivery_dag_instance = sprint5_example_dds_fct_delivery_dag()