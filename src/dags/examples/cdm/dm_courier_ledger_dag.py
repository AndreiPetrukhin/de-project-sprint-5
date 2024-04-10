import os
import logging
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

log = logging.getLogger(__name__)

@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2024, 3, 28, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'courier_settlement_report'],
    is_paused_upon_creation=False
)
def sprint5_example_cdm_courier_settlement_report_dag():
    dwh_pg_connect = "PG_WAREHOUSE_CONNECTION"
    current_file_dir = os.path.dirname(__file__)
    sql_file_path = os.path.join(current_file_dir, "sql", "dm_courier_ledger.sql")

    def transfer_data_to_cdm_courier_settlement_report(execution_date, **kwargs):
        with open(sql_file_path, 'r') as file:
            sql_query = file.read()

        sql_query = sql_query.format(execution_date=execution_date)

        with PostgresHook(postgres_conn_id=dwh_pg_connect).get_conn() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(sql_query)
                    conn.commit()
                    log.info("Transferred data to cdm.dm_courier_settlement_report.")
                except Exception as e:
                    log.error(f"Error transferring data: {e}")
                    conn.rollback()
                    raise e

    transfer_data_task = PythonOperator(
        task_id="transfer_data_to_cdm_courier_settlement_report",
        python_callable=transfer_data_to_cdm_courier_settlement_report,
        provide_context=True
    )

    return transfer_data_task

sprint5_example_cdm_courier_settlement_report_dag_instance = sprint5_example_cdm_courier_settlement_report_dag()