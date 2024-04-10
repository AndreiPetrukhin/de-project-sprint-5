# /Users/apetrukh/Desktop/de_yandex/s5-lessons/dags/examples/stg/api_deliveries_dag/api_deliveries_dag.py
from airflow.decorators import dag, task
import pendulum
from examples.stg.api_utils import APIUtils
from examples.stg.db_utils import DBUtils
from datetime import datetime
from datetime import timedelta

@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2024, 4, 6, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'deliveries', 'example'],
    is_paused_upon_creation=False
)
def sprint5_example_stg_api_deliveries_dag():
    api_utils = APIUtils(api_endpoint_var="API_ENDPOINT")
    db_utils = DBUtils(db_conn_id_var="PG_WAREHOUSE_CONNECTION")
    workflow_key = "api_deliveries_last_ts"

    @task(task_id="extract_deliveries_data")
    def extract_deliveries_data():
        last_delivery_ts_str = db_utils.get_last_uploaded_id(workflow_key)
        last_delivery_ts = datetime.strptime(last_delivery_ts_str, '%Y-%m-%d %H:%M:%S') if last_delivery_ts_str else datetime.utcnow() - timedelta(days=7)
        params = {
            "from": last_delivery_ts.strftime('%Y-%m-%d %H:%M:%S'),
            "sort_field": "_id",
            "sort_direction": "asc"
        }

        all_deliveries_data = api_utils.extract_data(endpoint="deliveries", params=params, paginate=True, limit=50)
        return [(delivery["delivery_id"], delivery) for delivery in all_deliveries_data]

    def parse_datetime(dt_str):
        try:
            return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')

    @task(task_id="load_deliveries_data_and_update_last_ts")
    def load_deliveries_data_and_update_last_ts(deliveries_data):
        if deliveries_data:
            last_ts = max(parse_datetime(delivery[1]['delivery_ts']) for delivery in deliveries_data)
            db_utils.load_data_and_update_last_id(data=deliveries_data, table_name="stg.api_deliveries",
                                                columns=["delivery_id", "object_value"],
                                                workflow_key=workflow_key, last_id=last_ts)

    deliveries_data = extract_deliveries_data()
    load_deliveries_data_and_update_last_ts(deliveries_data)

stg_api_deliveries_dag = sprint5_example_stg_api_deliveries_dag()
