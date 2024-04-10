# /Users/apetrukh/Desktop/de_yandex/s5-lessons/dags/examples/stg/api_utils.py
import requests
import json
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

class APIUtils:
    def __init__(self, api_endpoint_var):
        self.api_endpoint = Variable.get(api_endpoint_var)
        self.x_api_key = Variable.get("X_API_KEY")
        self.x_cohort = Variable.get("X_COHORT")
        self.x_nickname = Variable.get("X_NICKNAME")

    def extract_data(self, endpoint, params=None, paginate=False, limit=50):
        full_url = f"{self.api_endpoint}{endpoint}"
        headers = {
            "X-Nickname": self.x_nickname,
            "X-Cohort": self.x_cohort,
            "X-API-KEY": self.x_api_key
        }
        all_data = []
        offset = 0

        while True:
            if paginate:
                params = params or {}
                params['offset'] = offset
                params['limit'] = limit

            response = requests.get(full_url, headers=headers, params=params)
            data = response.json()

            if not data:
                break  # No more data

            all_data.extend(data)

            if len(data) < limit:
                break  # Last page
            offset += limit

        return all_data