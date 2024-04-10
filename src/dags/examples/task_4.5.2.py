import logging
from decimal import Decimal
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum

log = logging.getLogger(__name__)

@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2024, 3, 21, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'schema', 'ddl', 'example'],
    is_paused_upon_creation=False
)
def postgres_to_postgres_dag():

    create_stg_bonussystem_ranks = PostgresOperator(
        task_id='create_stg_bonussystem_ranks',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql="""
            CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                bonus_percent DECIMAL,
                min_payment_threshold DECIMAL
            );
        """
    )

    @task()
    def extract_data():
        source_hook = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
        conn = source_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM ranks")
        data = cursor.fetchall()
        # Convert Decimal objects to float
        data = [[float(value) if isinstance(value, Decimal) else value for value in row] for row in data]
        cursor.close()
        conn.close()
        return data

    @task()
    def load_data(data):
        target_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
        target_hook.insert_rows(table='stg.bonussystem_ranks', rows=data)

    data = extract_data()
    create_stg_bonussystem_ranks >> data >> load_data(data)

postgres_to_postgres = postgres_to_postgres_dag()