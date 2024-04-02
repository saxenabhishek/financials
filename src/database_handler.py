from tinydb import TinyDB, Query
from src.utils import get_logger
from tinydb.operations import delete
import pandas as pd
from enum import Enum

from src.vendors.zomato.mapper import MapZomatoData
from src.vendors.zepto.mapper import MapZeptoData
from datetime import datetime
from typing import Optional


log = get_logger(__name__)


class TransactionIndicator(Enum):
    """
    Enum representing the status of a transaction.

    All transactions start with a status of 'Pending'. They can then either become 'Settled' or 'Needs Split'.
    If a transaction is marked as 'Needs Split', it will eventually become 'Settled'.
    """

    SETTLED = "Settled"
    NEEDS_SPLIT = "Needs Split"
    PENDING = "Pending"

    IN_PROCESS = "In Process"
    UNKNOWN = "Unknown"


class Category(Enum):
    ESSENTIAL_NEED = "essential_need"  # Items crucial for survival and well-being
    BASIC_NEED = "basic_need"  # Items necessary for a comfortable standard of living
    WANT = "want"  # Items desired but not essential
    INVESTMENT = (
        "investment"  # Items that provide future benefit (financial or otherwise)
    )
    LUXURY = "luxury"  # Non-essential items that provide comfort or enjoyment
    UNKNOWN = "unknown"  # Items that do not fit into any of the above categories


class DatabaseHandler:
    _db_file = "master_db.json"
    delete = delete
    zepto_mapper = MapZeptoData()
    Query = Query
    INDICATOR = TransactionIndicator

    def __init__(self):
        log.debug("Initializing the database handler")
        self.db = TinyDB(
            self._db_file,
            indent=4,
            separators=(",", ": "),
            sort_keys=True,
        )
        self._init_tables()

    def filter_transactions_by_date(
        self, transactions, start: Optional[str] = None, end: Optional[str] = None
    ):
        if start:
            start_date = datetime.strptime(start, "%Y-%m-%d")
            transactions = [
                txn
                for txn in transactions
                if datetime.strptime(txn["ValueDate"], "%Y-%m-%d") >= start_date
            ]

        if end:
            end_date = datetime.strptime(end, "%Y-%m-%d")
            transactions = [
                txn
                for txn in transactions
                if datetime.strptime(txn["ValueDate"], "%Y-%m-%d") <= end_date
            ]

        return transactions

    def get_first_and_last_transaction_date(self, transactions) -> tuple[str, str]:
        # Find the first and last transaction dates
        max_date_transaction = max(transactions, key=lambda x: x["ValueDate"])
        min_date_transaction = min(transactions, key=lambda x: x["ValueDate"])

        return (
            min_date_transaction["ValueDate"],
            max_date_transaction["ValueDate"],
        )

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

    def write_transactions(self, transaction: list):
        df = pd.concat(transaction)

        df["TransactionIndicator"] = TransactionIndicator.PENDING.value
        df["Category"] = Category.UNKNOWN.value

        zomato_mapper = MapZomatoData(df)

        zomato_transactions, _ = zomato_mapper.doMapping()
        # zepto_transactions, _ = self.zepto_mapper.doMapping()

        df = df.merge(
            zomato_transactions[["RefNo", "dictData"]], on="RefNo", how="left"
        )
        df.rename(columns={"dictData": "ZomatoDictData"}, inplace=True)
        df["ValueDate"] = pd.to_datetime(df["ValueDate"]).dt.date.astype(str)
        df["ZomatoDictData"].fillna("", inplace=True)
        log.info(f"Writing {len(df)} transactions to the database")
        log.info(f"First transaction: {df.iloc[0]}")

        self.transactions.insert_multiple(df.to_dict(orient="records"))

    def get_transaction_by_status(self, status: TransactionIndicator):
        transactions = self.transactions.search(
            Query().TransactionIndicator == status.value
        )
        return transactions
