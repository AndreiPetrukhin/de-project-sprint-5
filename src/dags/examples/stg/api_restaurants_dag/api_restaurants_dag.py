# /Users/apetrukh/Desktop/de_yandex/s5-lessons/dags/examples/stg/api_restaurants_dag/api_restaurants_dag.py
from airflow.decorators import dag, task
import pendulum
from examples.stg.api_utils import APIUtils

@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2024, 3, 21, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'restaurants', 'example'],
    is_paused_upon_creation=False
)
def sprint5_example_stg_api_restaurants_dag():
    api_utils = APIUtils(api_endpoint_var="API_ENDPOINT", db_conn_id_var="PG_WAREHOUSE_CONNECTION")
    workflow_key = "api_restaurants_last_id"

    @task(task_id="extract_restaurants_data")
    def extract_restaurants_data():
        last_uploaded_id = api_utils.get_last_uploaded_id(workflow_key)
        all_restaurants_data = []
        page = 0
        limit = 50

        while True:
            endpoint = f"restaurants?sort_field=id&sort_direction=asc&limit={limit}&offset={page * limit}"
            restaurants_data = api_utils.extract_data(endpoint=endpoint)
            if not restaurants_data:
                break  # No more data

            filtered_data = [restaurant for restaurant in restaurants_data if restaurant["_id"] > last_uploaded_id] if last_uploaded_id else restaurants_data
            all_restaurants_data.extend([(restaurant["_id"], restaurant) for restaurant in filtered_data])

            if len(restaurants_data) < limit:
                break  # Last page

            page += 1

        return all_restaurants_data

    @task(task_id="load_restaurants_data_and_update_last_id")
    def load_restaurants_data_and_update_last_id(restaurants_data):
        if restaurants_data:
            last_id = max(restaurant[0] for restaurant in restaurants_data)
            api_utils.load_data_and_update_last_id(data=restaurants_data, table_name="stg.api_restaurants", columns=["restaurant_id", "object_value"], workflow_key=workflow_key, last_id=last_id)

    restaurants_data = extract_restaurants_data()
    load_restaurants_data_and_update_last_id(restaurants_data)

stg_api_restaurants_dag = sprint5_example_stg_api_restaurants_dag()