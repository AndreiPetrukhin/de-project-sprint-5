import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

log = logging.getLogger(__name__)

@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2024, 3, 26, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'couriers'],
    is_paused_upon_creation=False
)
def sprint5_example_dds_dm_couriers_dag():
    dwh_pg_connect = "PG_WAREHOUSE_CONNECTION"

    @task(task_id="extract_last_loaded_id")
    def extract_last_loaded_id():
        with PostgresHook(postgres_conn_id=dwh_pg_connect).get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT (workflow_settings::jsonb)->>'last_loaded_id' AS last_loaded_id
                    FROM dds.srv_wf_settings
                    WHERE workflow_key = 'stg_to_dds_couriers'
                """)
                result = cursor.fetchone()
                return int(result[0]) if result else -1

    @task(task_id="transfer_data_to_dm_couriers")
    def transfer_data_to_dm_couriers(last_loaded_id):
        with PostgresHook(postgres_conn_id=dwh_pg_connect).get_conn() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute("""
                        INSERT INTO dds.dm_couriers (courier_id, courier_name)
                        SELECT DISTINCT
                            courier_id,
                            (object_value::jsonb)->>'name'::text AS courier_name
                        FROM stg.api_couriers
                        WHERE id > %s
                        ON CONFLICT (courier_id) DO UPDATE SET
                            courier_name = EXCLUDED.courier_name
                    """, (last_loaded_id,))

                    cursor.execute("""
                        SELECT MAX(id) FROM stg.api_couriers
                        WHERE id > %s
                    """, (last_loaded_id,))
                    new_last_loaded_id = cursor.fetchone()[0]

                    if new_last_loaded_id:
                        cursor.execute("""
                            INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                            VALUES ('stg_to_dds_couriers', jsonb_build_object('last_loaded_id', %s))
                            ON CONFLICT (workflow_key) DO UPDATE SET
                                workflow_settings = jsonb_build_object('last_loaded_id', %s)
                            """, (new_last_loaded_id, new_last_loaded_id, ))

                    conn.commit()
                    log.info(f"Transferred data to dds.dm_couriers and updated last_loaded_id to {new_last_loaded_id}.")
                except Exception as e:
                    log.error(f"Error transferring data: {e}")
                    conn.rollback()
                    raise e

    last_loaded_id = extract_last_loaded_id()
    transfer_data_to_dm_couriers(last_loaded_id)

sprint5_example_dds_couriers_dag = sprint5_example_dds_dm_couriers_dag()
