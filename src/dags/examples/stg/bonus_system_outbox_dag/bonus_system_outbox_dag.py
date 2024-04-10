import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
from datetime import datetime

log = logging.getLogger(__name__)

@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2024, 3, 21, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'outbox', 'example'],
    is_paused_upon_creation=False
)
def sprint5_example_stg_outbox_dag():
    dwh_pg_connect = "PG_WAREHOUSE_CONNECTION"
    origin_pg_connect = "PG_ORIGIN_BONUS_SYSTEM_CONNECTION"

    @task(task_id="extract_last_loaded_id")
    def extract_last_loaded_id():
        with PostgresHook(postgres_conn_id=dwh_pg_connect).get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT (workflow_settings::jsonb)->>'last_loaded_id' AS last_loaded_id
                    FROM stg.srv_wf_settings
                    WHERE workflow_key = 'outbox_to_stg'
                """)
                result = cursor.fetchone()
                return int(result[0]) if result else -1

    @task(task_id="extract_new_events")
    def extract_new_events(last_loaded_id):
        with PostgresHook(postgres_conn_id=origin_pg_connect).get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, event_ts, event_type, event_value::text
                    FROM outbox
                    WHERE id > %s
                    ORDER BY id ASC
                """, (last_loaded_id,))
                rows = cursor.fetchall()  # Make sure this line is included to define 'rows'
                # Convert datetime objects to strings
                return [(row[0], row[1].isoformat() if isinstance(row[1], datetime) else row[1], row[2], row[3]) for row in rows]

    @task(task_id="process_new_events")
    def process_new_events(new_events):
        if new_events:
            log.info(f"Processing {len(new_events)} new events.")
            last_loaded_id = new_events[-1][0]
            with PostgresHook(postgres_conn_id=dwh_pg_connect).get_conn() as conn:
                with conn.cursor() as cursor:
                    try:
                        # Insert new events
                        cursor.executemany("""
                            INSERT INTO stg.bonussystem_events (id, event_ts, event_type, event_value)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET
                            event_ts = EXCLUDED.event_ts,
                            event_type = EXCLUDED.event_type,
                            event_value = EXCLUDED.event_value
                        """, new_events)

                        rowcount = cursor.rowcount
                        log.info(f"Inserted/updated {rowcount} rows in stg.bonussystem_events.")

                        # Update last loaded ID
                        cursor.execute("""
                            INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
                            VALUES ('outbox_to_stg', jsonb_build_object('last_loaded_id', %s))
                            ON CONFLICT (workflow_key) DO UPDATE
                            SET workflow_settings = jsonb_build_object('last_loaded_id', %s)
                        """, (last_loaded_id, last_loaded_id))

                        conn.commit()  # Commit the transaction
                        log.info(f"Successfully processed {len(new_events)} new events.")
                    except Exception as e:
                        log.error(f"Error processing new events: {e}")
                        conn.rollback()
        else:
            log.info("No new events to process.")


    last_loaded_id = extract_last_loaded_id()
    new_events = extract_new_events(last_loaded_id)
    process_new_events(new_events)

sprint5_example_stg_outbox_dag = sprint5_example_stg_outbox_dag()