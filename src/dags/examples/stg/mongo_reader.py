from datetime import datetime
from typing import Dict, List

from lib import MongoConnect

class MongoReader:
    """Class for reading data from MongoDB collections."""

    def __init__(self, mc: MongoConnect) -> None:
        """Initializes the MongoReader with a MongoConnect object.

        Args:
            mc (MongoConnect): Object to connect to MongoDB.
        """
        self.dbs = mc.client()

    def get_collection_data(self, collection_name: str, load_threshold: datetime, limit) -> List[Dict]:
        """Retrieves data from a specified MongoDB collection.

        Args:
            collection_name (str): Name of the MongoDB collection to read data from.
            load_threshold (datetime): Timestamp to filter documents that were updated after this time.
            limit (int): Maximum number of documents to retrieve.

        Returns:
            List[Dict]: A list of dictionaries representing the documents retrieved from the collection.
        """
        # Create a filter to retrieve documents updated after the load_threshold
        filter = {'update_ts': {'$gt': load_threshold}}

        # Sort the documents by the update timestamp in ascending order
        sort = [('update_ts', 1)]

        # Retrieve documents from the MongoDB collection using the filter and sort criteria, with a limit on the number of documents
        docs = list(self.dbs.get_collection(collection_name).find(filter=filter, sort=sort, limit=limit))
        return docs