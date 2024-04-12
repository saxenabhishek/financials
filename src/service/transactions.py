from src.db import mongo
from datetime import datetime
from typing import Optional
from src.service.const import TransactionIndicator


class TransactionService:
    def __init__(self):
        self.db = mongo["transactions"]

    def get_last_transaction_date(
        self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None
    ) -> datetime:
        query = self._add_query_range(start_date, end_date)

        last_transaction = self.db.find_one(query, sort=[("ValueDate", -1)])
        if last_transaction:
            return last_transaction["ValueDate"]
        raise Exception("No transactions found in the database")

    def get_all_transactions(
        self,
        cols: Optional[dict] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        indicator: Optional[TransactionIndicator] = None,
    ):
        query = self._add_query_range(start_date, end_date)
        if indicator:
            query = self._add_indicator_to_query(query, indicator)
        return self.db.find(query, cols) if cols else self.db.find(query)

    def get_pending_transactions(
        self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None
    ):
        return self._get_transactions_by_indicator(
            TransactionIndicator.PENDING, start_date, end_date
        )

    def get_split_transactions(
        self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None
    ):
        return self._get_transactions_by_indicator(
            TransactionIndicator.NEEDS_SPLIT, start_date, end_date
        )

    def get_settled_transactions(
        self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None
    ):
        return self._get_transactions_by_indicator(
            TransactionIndicator.SETTLED, start_date, end_date
        )

    def _add_query_range(
        self, start_date: Optional[datetime], end_date: Optional[datetime]
    ) -> dict:
        query = {}
        if start_date and end_date:
            query = {"ValueDate": {"$gte": start_date, "$lte": end_date}}
        elif start_date:
            query = {"ValueDate": {"$gte": start_date}}
        elif end_date:
            query = {"ValueDate": {"$lte": end_date}}
        return query

    def _get_transactions_by_indicator(
        self,
        indicator: TransactionIndicator,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ):
        query: dict = self._add_query_range(start_date, end_date)
        query = self._add_indicator_to_query(query, indicator)
        return self.db.find(query, {"_id": 0})

    def _add_indicator_to_query(self, query: dict, indicator: TransactionIndicator):
        query["TransactionIndicator"] = indicator.value
        return query

    def update_transaction(self, id: str, notes: str, type: TransactionIndicator):
        set = {"Notes": notes, "TransactionIndicator": type.value}
        if notes is None:
            set.pop("Notes")
        return self.db.update_one({"_id": id}, {"$set": set})

    @staticmethod
    def generate_tailwind_colors():
        colors = {
            TransactionIndicator.SETTLED.value: "bg-green-200 text-green-700",
            TransactionIndicator.NEEDS_SPLIT.value: "bg-yellow-200 text-yellow-700",
            TransactionIndicator.PENDING.value: "bg-blue-200 text-blue-700",
            TransactionIndicator.MAPPED.value: "bg-gray-200 text-gray-700",
        }

        return colors
