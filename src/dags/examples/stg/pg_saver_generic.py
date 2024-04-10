from datetime import datetime
from typing import Any, Dict

from lib.dict_util import json2str
from psycopg import Connection


class PgSaver:

    def save_object(self, conn: Connection, query: str, params: Dict[str, Any]):
        """Saves an object to the PostgreSQL database using a given query and parameters.

        Args:
            conn (Connection): The database connection.
            query (str): The SQL query to execute.
            params (Dict[str, Any]): The parameters for the SQL query.
        """
        with conn.cursor() as cur:
            cur.execute(query, params)

""" usage exmaple
pg_saver = PgSaver()
conn = get_connection()  # Assume this function returns a psycopg Connection object

object_id = "123"
str_val = json2str(val)
update_ts = datetime.now()
table_name = "stg.ordersystem_restaurants"

query = f"
    INSERT INTO {table_name}(object_id, object_value, update_ts)
    VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
    ON CONFLICT (object_id) DO UPDATE
    SET
        object_value = EXCLUDED.object_value,
        update_ts = EXCLUDED.update_ts;
"

params = {
    "object_id": object_id,
    "object_value": str_val,
    "update_ts": update_ts
}

pg_saver.save_object(conn, query, params)
"""