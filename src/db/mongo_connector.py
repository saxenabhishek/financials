from pymongo import MongoClient


class MongoConnector:
    """
    A class to connect to a MongoDB database with automatic closing on deletion.
    """

    def __init__(self, host="localhost", port=27017, username=None, password=None):
        """
        Initializes the connection to the MongoDB database.

        Args:
          host (str, optional): The hostname or IP address of the MongoDB server. Defaults to "localhost".
          port (int, optional): The port number of the MongoDB server. Defaults to 27017.
          username (str, optional): The username for authentication (if required). Defaults to None.
          password (str, optional): The password for authentication (if required). Defaults to None.
        """
        connectionString = (
            f"mongodb://{username}:{password}@{host}:{port}/"
            if username and password
            else f"mongodb://{host}:{port}/"
        )
        self.client = MongoClient(connectionString)
        self.db = None

    def get_database(self, database_name):
        return self.client[database_name]

    def __del__(self):
        """
        Closes the connection to the MongoDB database when the object is garbage collected.
        """
        if self.client:
            self.client.close()


# Example usage
mongo_connector = MongoConnector()
database = mongo_connector.get_database("my_database")
