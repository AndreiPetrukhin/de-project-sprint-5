import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

log = logging.getLogger(__name__)

@dag(
    schedule_interval='*/15 * * * *',  # Schedule set to run every 15 minutes
    start_date=pendulum.datetime(2024, 3, 21, tz="UTC"),  # Adjust to the current date
    catchup=False,  # No catch-up
    tags=['sprint5', 'stg', 'users', 'example'],  # Adjusted tags for this specific task
    is_paused_upon_creation=False  # The DAG is active upon creation
)
def sprint5_example_stg_bonus_system_users_dag():
    dwh_pg_connect = "PG_WAREHOUSE_CONNECTION"
    origin_pg_connect = "PG_ORIGIN_BONUS_SYSTEM_CONNECTION"

    @task(task_id="extract_users_data")
    def extract_users_data():
        source_hook = PostgresHook(postgres_conn_id=origin_pg_connect)
        src_conn = source_hook.get_conn()
        src_cursor = src_conn.cursor()
        src_cursor.execute("SELECT * FROM users")
        users_data = src_cursor.fetchall()
        src_cursor.close()
        src_conn.close()
        return users_data

    @task(task_id="load_users_data")
    def load_users_data(users_data):
        target_hook = PostgresHook(postgres_conn_id=dwh_pg_connect)
        target_conn = target_hook.get_conn()
        target_cursor = target_conn.cursor()
        insert_query = """
            INSERT INTO stg.bonussystem_users (id, order_user_id) 
            VALUES (%s, %s)
            ON CONFLICT (id) DO UPDATE SET
            order_user_id = excluded.order_user_id;
        """
        # Assuming the users_data tuple is structured as (id, order_user_id)
        target_cursor.executemany(insert_query, users_data)
        target_conn.commit()
        target_cursor.close()
        target_conn.close()

    users_data = extract_users_data()
    load_users_data(users_data)

stg_bonus_system_users_dag = sprint5_example_stg_bonus_system_users_dag()