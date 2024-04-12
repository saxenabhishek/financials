from src.bank_parser.hdfc_parser import HdfcExcelDataReader
from src.bank_parser.icici_parser import IciciExcelDataReader
from src.vendors.zomato.order_parser import OrderParser as ZomatoOrderParser
from src.utils import get_logger, get_all_file_paths, read_json_files_from_folder
import pandas as pd
from src.service.const import TransactionIndicator, Category
from src.db import mongo
import os
from pymongo.results import InsertManyResult
from datetime import timedelta

log = get_logger(__name__)


class DataIngestionService:
    def __init__(self):
        log.info("Initializing Data Reader and writer Objects")
        self.hdfc_files = get_all_file_paths(r"bank_transactions\hdfc_data", ".xls")
        self.icici_files = get_all_file_paths(r"bank_transactions\icici_data", ".xls")
        self.zomato_files = get_all_file_paths(r"zomato_orders", ".json")

        self.hdfc_parser = HdfcExcelDataReader(self.hdfc_files)
        self.icici_parser = IciciExcelDataReader(self.icici_files)

        self.zomato_parser = ZomatoOrderParser(
            read_json_files_from_folder("zomato_orders")
        )

        self.transactions = mongo["transactions"]
        self.zomato_collection = mongo["zomato"]

    def ingest_parsed_data(self, parser, bank_name, toCSV=False):
        # Check if parser is valid
        if not parser.invalid_init:
            bank_df = parser.read_data()
            if toCSV:
                bank_df.to_csv(f"{bank_name}_data.csv", index=False)
            return bank_df
        else:
            log.warning(f"No valid {bank_name} file paths were provided.")
            return pd.DataFrame()

    def ingest_transactions(self, toCSV=False, debug=True) -> InsertManyResult:
        log.info("Ingesting data from HDFC and ICICI Excel files...")

        hdfc_df = self.ingest_parsed_data(self.hdfc_parser, "hdfc", toCSV)
        icici_df = self.ingest_parsed_data(self.icici_parser, "icici", toCSV)

        df = pd.concat([hdfc_df, icici_df], ignore_index=True)

        # If DataFrame is still empty, no valid file paths were provided
        if df.empty:
            log.error("No valid Bank file paths were provided.")
            return InsertManyResult(acknowledged=False, inserted_ids=[])

        df["TransactionIndicator"] = TransactionIndicator.PENDING.value
        df["Category"] = Category.UNKNOWN.value

        # All transactions with zero withdrawal amount are considered settled
        df.loc[df["WithdrawalAmt"] == 0, "TransactionIndicator"] = (
            TransactionIndicator.SETTLED.value
        )

        if debug:
            self.transactions.drop()

        records = df.to_dict(orient="records")

        # Insert records into MongoDB collection
        log.info("Inserting records into MongoDB...")
        result = InsertManyResult(acknowledged=True, inserted_ids=[])
        try:
            result = self.transactions.insert_many(records, ordered=False)
        except Exception as e:
            log.error(f"Error inserting records: {e}")

        return result

    def ingest_data(self, toCSV=False, debug=False) -> int:
        transaction_result = self.ingest_transactions(toCSV, debug)
        vendor_result = self.ingest_vendor_data(toCSV, debug)

        moved_files = self.move_processed_files_to_old()
        modified_documents = self.map_transactions_to_vendors()

        return (
            len(transaction_result.inserted_ids)
            + len(vendor_result.inserted_ids)
            + modified_documents
            + moved_files
        )

    def map_transactions_to_vendors(self) -> int:
        log.info("Mapping transactions to vendor data")
        modified_count = self.find_vendor_matches_update_db(
            "zomato",
            self.zomato_collection,
            {
                "status": 6,
            },
        )
        return modified_count

    def find_vendor_matches_update_db(
        self, vendor_phrase, vendor_collection, additional_filters={}
    ) -> int:
        """
        This function finds matching transactions and updates the database accordingly.

        Args:
            vendor_phrase (str): The vendor phrase to match.
            vendor_collection (MongoDB Collection): The vendor collection to search in.
            additional_filters (dict, optional): Additional filters for the search. Defaults to {}.

        Returns:
            int: The number of modified transactions.
        """

        # Find transactions that match the vendor phrase
        matching_transactions = self.transactions.find(
            {
                "Narration": {"$regex": vendor_phrase, "$options": "i"},
                vendor_phrase: {"$exists": False},
            }
        )

        result = 0
        for txn in matching_transactions:
            value_date = txn.get("ValueDate")
            next_date = value_date + timedelta(days=1)
            gtlsdate = {"$gte": value_date, "$lt": next_date}

            # Find matches in the vendor collection
            matches = vendor_collection.find(
                {
                    "totalCost": txn.get("WithdrawalAmt"),
                    "orderDate": gtlsdate,
                    **additional_filters,
                }
            )

            n = 0
            matched_ids = []
            for match in matches:
                n += 1
                matched_ids.append(match.get("_id"))

            # If there is exactly one match, update the transaction
            if n == 1:
                res = self.transactions.update_one(
                    {"_id": txn.get("_id")}, {"$set": {vendor_phrase: matched_ids[0]}}
                )
                result += res.modified_count
            elif n > 1:
                # If there are multiple matches, log a warning and do not update the transaction
                log.warn(
                    f"Multiple matches found for transaction: {txn.get('_id')}, not updating. Matches: {matched_ids}"
                )
        if result == 0:
            log.warn(f"No matches found for vendor: {vendor_phrase}")

        return result

    def ingest_vendor_data(self, toCSV=False, debug=False) -> InsertManyResult:
        log.info("Ingesting data from Vendor JSON files...")

        log.debug("Zomato Data Ingestion")

        zomato_df = self.ingest_parsed_data(self.zomato_parser, "zomato", toCSV)

        # this is a simplification since diff vendors would be in diff collections
        if zomato_df.empty:
            log.error("No valid vendor file paths were provided.")
            return InsertManyResult(acknowledged=False, inserted_ids=[])

        records = zomato_df.to_dict(orient="records")

        result = InsertManyResult(acknowledged=True, inserted_ids=[])
        try:
            result = self.zomato_collection.insert_many(records, ordered=False)
        except Exception as e:
            log.error(f"Error inserting records: {e}")

        return result

    def move_processed_files_to_old(self) -> int:
        log.info("renaming processed files...")

        valid_files = [
            file
            for file in self.hdfc_files + self.icici_files + self.zomato_files
            if "old" not in file
        ]

        if len(valid_files) == 0:
            log.info("No files to move to .old directory.")

        for file in valid_files:
            parent_dir = os.path.dirname(file)
            old_dir = os.path.join(parent_dir, ".old")

            os.makedirs(old_dir, exist_ok=True)
            os.rename(file, os.path.join(old_dir, os.path.basename(file)))
        return len(valid_files)
