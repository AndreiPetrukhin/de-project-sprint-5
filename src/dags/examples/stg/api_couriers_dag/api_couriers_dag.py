# /Users/apetrukh/Desktop/de_yandex/s5-lessons/dags/examples/stg/api_couriers_dag/api_couriers_dag.py
from airflow.decorators import dag, task
import pendulum
from examples.stg.api_utils import APIUtils
from examples.stg.db_utils import DBUtils

@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2024, 4, 6, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'couriers', 'example'],
    is_paused_upon_creation=False
)
def sprint5_example_stg_api_couriers_dag():
    api_utils = APIUtils(api_endpoint_var="API_ENDPOINT")
    db_utils = DBUtils(db_conn_id_var="PG_WAREHOUSE_CONNECTION")
    workflow_key = "api_couriers_last_id"

    @task(task_id="extract_couriers_data")
    def extract_couriers_data():
        last_uploaded_id = db_utils.get_last_uploaded_id(workflow_key)
        params = {
            "sort_field": "id",
            "sort_direction": "asc"
        }
        all_couriers_data = api_utils.extract_data(endpoint="couriers", params=params, paginate=True, limit=50)

        # Filter couriers based on last_uploaded_id
        filtered_data = [courier for courier in all_couriers_data if courier["_id"] > last_uploaded_id] if last_uploaded_id else all_couriers_data
        return [(courier["_id"], courier) for courier in filtered_data]

    @task(task_id="load_couriers_data_and_update_last_id")
    def load_couriers_data_and_update_last_id(couriers_data):
        if couriers_data:
            last_id = max(courier[0] for courier in couriers_data)
            db_utils.load_data_and_update_last_id(data=couriers_data, table_name="stg.api_couriers", columns=["courier_id", "object_value"], workflow_key=workflow_key, last_id=last_id)

    couriers_data = extract_couriers_data()
    load_couriers_data_and_update_last_id(couriers_data)

stg_api_couriers_dag = sprint5_example_stg_api_couriers_dag()
