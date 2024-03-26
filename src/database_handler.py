from tinydb import TinyDB
from src.utils import get_logger

log = get_logger(__name__)


class DatabaseHandler:
    _db_file = "master_db.json"

    def __init__(self):
        log.debug("Initializing the database handler")
        self.db = TinyDB(self._db_file)
        self._init_tables()

    def _init_tables(self):
        """
        Initialize the tables in the database.
        """
        log.debug("Initializing the tables in the database")
        self.transactions = self.db.table("transactions")

    def __del__(self):
        """
        Close the database connection when the object is destroyed.
        """
        log.debug("Closing the database connection")
        self.db.close()
