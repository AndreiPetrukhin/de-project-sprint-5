# /Users/apetrukh/Desktop/de_yandex/s5-lessons/dags/examples/stg/api_restaurants_dag/api_restaurants_dag.py
from airflow.decorators import dag, task
import pendulum
from examples.stg.api_utils import APIUtils
from examples.stg.db_utils import DBUtils

@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2024, 3, 21, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'restaurants', 'example'],
    is_paused_upon_creation=False
)
def sprint5_example_stg_api_restaurants_dag():
    api_utils = APIUtils(api_endpoint_var="API_ENDPOINT")
    db_utils = DBUtils(db_conn_id_var="PG_WAREHOUSE_CONNECTION")
    workflow_key = "api_restaurants_last_id"

    @task(task_id="extract_restaurants_data")
    def extract_restaurants_data():
        last_uploaded_id = db_utils.get_last_uploaded_id(workflow_key)
        params = {
            "sort_field": "id",
            "sort_direction": "asc"
        }
        all_restaurants_data = api_utils.extract_data(endpoint="restaurants", params=params, paginate=True, limit=50)

        # Filter restaurants based on last_uploaded_id
        filtered_data = [restaurant for restaurant in all_restaurants_data if restaurant["_id"] > last_uploaded_id] if last_uploaded_id else all_restaurants_data
        return [(restaurant["_id"], restaurant) for restaurant in filtered_data]

    @task(task_id="load_restaurants_data_and_update_last_id")
    def load_restaurants_data_and_update_last_id(restaurants_data):
        if restaurants_data:
            last_id = max(restaurant[0] for restaurant in restaurants_data)
            db_utils.load_data_and_update_last_id(data=restaurants_data, table_name="stg.api_restaurants", columns=["restaurant_id", "object_value"], workflow_key=workflow_key, last_id=last_id)

    restaurants_data = extract_restaurants_data()
    load_restaurants_data_and_update_last_id(restaurants_data)

stg_api_restaurants_dag = sprint5_example_stg_api_restaurants_dag()