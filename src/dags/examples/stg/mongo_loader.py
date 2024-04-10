from datetime import datetime
from logging import Logger
from typing import Dict

from examples.stg import EtlSetting, StgEtlSettingsRepository
from examples.stg.pg_saver_generic import PgSaver
from examples.stg.mongo_reader import MongoReader
from lib import PgConnect
from lib.dict_util import json2str

class DataLoader:
    """Class for loading data from MongoDB collections to PostgreSQL."""

    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    WF_KEY_PREFIX = "example_ordersystem_"  # Prefix for workflow key
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: MongoReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        """Initializes the DataLoader with necessary components.

        Args:
            collection_loader (MongoReader): Object to read data from MongoDB.
            pg_dest (PgConnect): PostgreSQL connection object.
            pg_saver (PgSaver): Object to save data into PostgreSQL.
            logger (Logger): Logger object for logging.
        """
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self, collection_name: str, query: str, param_template: Dict[str, str]) -> int:
        """Copies data from a specified MongoDB collection to PostgreSQL using a provided query and parameter template.

        Args:
            collection_name (str): Name of the MongoDB collection to copy data from.
            query (str): The SQL query template for inserting/updating data in PostgreSQL.
            param_template (Dict[str, str]): Template for parameters to be used in the SQL query.

        Returns:
            int: Number of documents copied.
        """
        # Start a transaction
        with self.pg_dest.connection() as conn:
            # Generate a dynamic workflow key based on the collection name
            wf_key = self.WF_KEY_PREFIX + collection_name + "_origin_to_stg_workflow"
            
            # Get or create the workflow setting
            wf_setting = self.settings_repository.get_setting(conn, wf_key)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=wf_key,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            # Get the last loaded timestamp from the workflow settings
            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"Starting to load from last checkpoint: {last_loaded_ts}")

            # Load data from the MongoDB collection
            load_queue = self.collection_loader.get_collection_data(collection_name, last_loaded_ts, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} documents to sync from {collection_name} collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            # Process and save each document
            i = 0
            for d in load_queue:
                d['_id'] = str(d['_id'])
                params = {
                    key: json2str(d) if value == "__convert_to_json__" else d[value]
                    for key, value in param_template.items()
                }
                self.pg_saver.save_object(conn, query, params)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {i} documents of {len(load_queue)} while syncing {collection_name}.")

            # Update the workflow setting with the new last loaded timestamp
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["update_ts"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
