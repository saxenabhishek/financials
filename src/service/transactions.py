from src.db import mongo
from datetime import datetime
from typing import Optional
from src.service.const import TransactionIndicator
from src.service.vendor import Vendor


class TransactionService:
    def __init__(self):
        self.db = mongo["transactions"]

    def get_last_transaction_date(
        self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None
    ) -> datetime:
        query = {}
        query = self._add_query_range(query, start_date, end_date)

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
        phrase: Optional[Vendor.vendors_type] = None,
        combine_with_vendor_data: bool = False,
        sort_by: Optional[str] = None,
    ):
        # Jump to different flow to show mapped data
        if combine_with_vendor_data and phrase:
            return self.get_all_vendor_transactions(
                phrase, cols, start_date, end_date, indicator, sort_by
            )
        query: dict = {}
        query = self._add_query_range(query, start_date, end_date)
        if indicator:
            query = self._add_indicator_to_query(query, indicator)
        if phrase:
            query = self._add_phrase_to_query(query, phrase)
        transactions = self.db.find(query, cols) if cols else self.db.find(query)
        if sort_by:
            return transactions.sort(sort_by, -1)
        return transactions

    def get_all_vendor_transactions(
        self,
        phrase: Vendor.vendors_type,
        cols: Optional[dict] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        indicator: Optional[TransactionIndicator] = None,
        sort_by: Optional[str] = None,
    ):
        local_field = Vendor.get_transaction_foreign_field(phrase)
        collection = Vendor.get_collection(phrase)

        match_query_args = [{local_field: {"$exists": True}}]
        match_query_args.append(self._add_query_range({}, start_date, end_date))
        if indicator:
            match_query_args.append(self._add_indicator_to_query({}, indicator))
        return self.db.aggregate(
            [
                {"$match": {"$and": match_query_args}},
                {
                    "$lookup": {
                        "from": collection.name,
                        "localField": local_field,
                        "foreignField": "_id",
                        "as": "special",
                    }
                },
                {"$unwind": {"path": "$special"}},
                {"$project": cols},
                {"$sort": {sort_by: 1}},
            ]
        )

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
        self, query, start_date: Optional[datetime], end_date: Optional[datetime]
    ) -> dict:
        if start_date and end_date:
            query["ValueDate"] = {"$gte": start_date, "$lte": end_date}
        elif start_date:
            query["ValueDate"] = {"$gte": start_date}
        elif end_date:
            query["ValueDate"] = {"$lte": end_date}
        return query

    def _get_transactions_by_indicator(
        self,
        indicator: TransactionIndicator,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ):
        query = {}
        query: dict = self._add_query_range({}, start_date, end_date)
        query = self._add_indicator_to_query(query, indicator)
        return self.db.find(query, {"_id": 0})

    def _add_indicator_to_query(self, query: dict, indicator: TransactionIndicator):
        query["TransactionIndicator"] = indicator.value
        return query

    def update_transaction(
        self,
        transaction_id: str,
        new_indicator: TransactionIndicator,
        notes: str,
    ):
        """
        Updates a transaction in the database and records the indicator change with a timestamp in the history field.

        Args:
            transaction_id (str): The unique identifier of the transaction.
            notes (str, optional): Optional notes to update. Defaults to None.
            new_indicator (TransactionIndicator): The new transaction indicator.

        Returns:
            pymongo.results.UpdateResult: The result of the update operation.
        """

        update_data = {}
        if notes is not None:
            update_data["Notes"] = notes
        update_data["TransactionIndicator"] = new_indicator.value

        # Get the previous indicator value (if available)
        previous_indicator = self.db.find_one(
            {"_id": transaction_id}, projection={"TransactionIndicator": 1}
        )
        previous_indicator = (
            previous_indicator.get("TransactionIndicator")
            if previous_indicator
            else None
        )

        # Create a history entry with previous and new indicator values
        history_entry = {
            "timestamp": datetime.now(),
            "previous_indicator": previous_indicator,
            "new_indicator": new_indicator.value,
        }

        # Update the transaction and add/update the history field
        update_result = self.db.update_one(
            {"_id": transaction_id},
            {
                "$set": update_data,
                "$addToSet": {"History": history_entry},
            },
        )

        return update_result

    def _add_phrase_to_query(self, query: dict, phrase: Vendor.vendors_type):
        regex_phrase = Vendor.get_narration_regex(phrase)
        query["Narration"] = {"$regex": regex_phrase, "$options": "i"}
        return query

    @staticmethod
    def generate_tailwind_colors():
        colors = {
            TransactionIndicator.SETTLED.value: "bg-green-200 text-green-700",
            TransactionIndicator.NEEDS_SPLIT.value: "bg-yellow-200 text-yellow-700",
            TransactionIndicator.PENDING.value: "bg-blue-200 text-blue-700",
            TransactionIndicator.MAPPED.value: "bg-gray-200 text-gray-700",
        }

        return colors
