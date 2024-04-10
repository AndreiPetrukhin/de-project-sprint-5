# /Users/apetrukh/Desktop/de_yandex/s5-lessons/dags/examples/stg/api_utils.py
import requests
import json
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

class APIUtils:
    def __init__(self, api_endpoint_var, db_conn_id_var):
        self.api_endpoint = Variable.get(api_endpoint_var)
        self.db_conn_id = db_conn_id_var
        self.x_api_key = Variable.get("X_API_KEY")
        self.x_cohort = Variable.get("X_COHORT")
        self.x_nickname = Variable.get("X_NICKNAME")

    def extract_data(self, endpoint):
        full_url = f"{self.api_endpoint}{endpoint}"
        headers = {
            "X-Nickname": self.x_nickname,
            "X-Cohort": self.x_cohort,
            "X-API-KEY": self.x_api_key
        }
        response = requests.get(full_url, headers=headers)
        return response.json()

    def get_last_uploaded_id(self, workflow_key):
        target_hook = PostgresHook(postgres_conn_id=self.db_conn_id)
        target_conn = target_hook.get_conn()
        target_cursor = target_conn.cursor()
        query = f"SELECT workflow_settings FROM stg.srv_wf_settings WHERE workflow_key = %s;"
        target_cursor.execute(query, (workflow_key,))
        result = target_cursor.fetchone()
        target_cursor.close()
        target_conn.close()
        try:
            return json.loads(result[0])['last_loaded_id'] if result and result[0] else None
        except json.JSONDecodeError:
            return None

    def load_data_and_update_last_id(self, data, table_name, columns, workflow_key, last_id):
        target_hook = PostgresHook(postgres_conn_id=self.db_conn_id)
        target_conn = target_hook.get_conn()
        target_cursor = target_conn.cursor()

        # Convert dict to JSON string for insertion
        data = [(item[0], json.dumps(item[1])) if isinstance(item[1], dict) else item for item in data]

        # Load data
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join(columns)
        insert_query = f"""
            INSERT INTO {table_name} ({column_names})
            VALUES ({placeholders})
            ON CONFLICT ({columns[0]}) DO UPDATE SET
            {', '.join([f'{col} = excluded.{col}' for col in columns[1:]])};
        """
        target_cursor.executemany(insert_query, data)

        # Update last uploaded ID as a JSON object
        last_id_str = last_id.strftime('%Y-%m-%d %H:%M:%S') if isinstance(last_id, datetime) else last_id
        settings_json = json.dumps({"last_loaded_id": last_id_str})
        update_query = """
            INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
            VALUES (%s, %s)
            ON CONFLICT (workflow_key) DO UPDATE SET
            workflow_settings = excluded.workflow_settings;
        """
        target_cursor.execute(update_query, (workflow_key, settings_json))

        # Commit transaction
        target_conn.commit()
        target_cursor.close()
        target_conn.close()
