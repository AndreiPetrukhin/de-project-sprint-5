import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from examples.stg.pg_saver_generic import PgSaver
from examples.stg.mongo_reader import MongoReader
from examples.stg.mongo_loader import DataLoader
from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2024, 3, 23, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'example', 'stg', 'origin'],
    is_paused_upon_creation=True
)
def sprint5_example_stg_order_system_orders():
    """
    DAG for loading order data from MongoDB into PostgreSQL.
    """

    # Connect to the DWH PostgreSQL database
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Get MongoDB connection variables from Airflow Variables
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_orders():
        """
        Task for loading order data from MongoDB into PostgreSQL.
        """
        # Initialize the PgSaver class
        pg_saver = PgSaver()

        # Initialize MongoDB connection
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Initialize the MongoReader class
        collection_reader = MongoReader(mongo_connect)

        # Initialize the DataLoader class
        data_loader = DataLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Define the SQL query and parameters template for inserting/updating data
        query = """
            INSERT INTO stg.ordersystem_orders(object_id, object_value, update_ts)
            VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
        """
        param_template = {
            "object_id": "_id",
            "object_value": "__convert_to_json__",
            "update_ts": "update_ts"
        }

        # Run the data loading process
        data_loader.run_copy("orders", query, param_template)

    load_orders_task = load_orders()

    # Set the execution order (only one task in this case)
    load_orders_task


order_stg_dag = sprint5_example_stg_order_system_orders()