import os
from datetime import timedelta

import pandas as pd
from pymongo.results import InsertManyResult

from src.bank_parser.hdfc_parser import HdfcExcelDataReader
from src.bank_parser.icici_parser import IciciExcelDataReader
from src.db import mongo
from src.service.const import Category, TransactionIndicator
from src.utils import get_all_file_paths, get_logger, read_json_files_from_folder
from src.vendors.zepto.order_parser import OrderParser as ZeptoOrderParser
from src.vendors.zomato.order_parser import OrderParser as ZomatoOrderParser

from src.service.vendor import Vendor

log = get_logger(__name__)


class DataIngestionService:
    def __init__(self):
        log.info("Initializing Data Reader and writer Objects")

        self.hdfc_files = get_all_file_paths(r"bank_transactions\hdfc_data", ".xls")
        self.icici_files = get_all_file_paths(r"bank_transactions\icici_data", ".xls")

        self.zomato_files = get_all_file_paths(
            Vendor.get_data_folder("zomato"), ".json"
        )
        self.zepto_files = get_all_file_paths(Vendor.get_data_folder("zepto"), ".json")

        self.hdfc_parser = HdfcExcelDataReader(self.hdfc_files)
        self.icici_parser = IciciExcelDataReader(self.icici_files)

        self.zomato_parser = ZomatoOrderParser(
            read_json_files_from_folder(Vendor.get_data_folder("zomato"))
        )

        self.zepto_parser = ZeptoOrderParser(
            read_json_files_from_folder(Vendor.get_data_folder("zepto"))
        )

        self.transactions = mongo["transactions"]

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
            log.warn("No valid Bank file paths were provided.")
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
            log.warn(f"Error inserting records: {e}")

        return result

    def ingest_data(self, toCSV=False, debug=False) -> int:
        transaction_result = self.ingest_transactions(toCSV, debug)
        vendor_result = self.ingest_vendor_data(toCSV, debug)

        moved_files = self.move_processed_files_to_old()
        modified_documents = self.map_transactions_to_vendors()

        return (
            len(transaction_result.inserted_ids)
            + vendor_result
            + modified_documents
            + moved_files
        )

    def map_transactions_to_vendors(self) -> int:
        log.info("Mapping transactions to vendor data")
        modified_count = 0
        modified_count += self.find_vendor_matches_update_db(
            "zomato",
            {
                "status": 6,
            },
        )
        modified_count += self.find_vendor_matches_update_db(
            "zepto",
            {
                "status": "DELIVERED",
            },
        )
        return modified_count

    def find_vendor_matches_update_db(
        self, vendor_phrase: Vendor.vendors_type, additional_filters={}
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

        field = Vendor.get_transaction_foreign_field(vendor_phrase)

        matching_transactions = self.transactions.find(
            {
                "Narration": {
                    "$regex": Vendor.get_narration_regex(vendor_phrase)[0],
                    "$options": "i",
                },
                field: {"$exists": False},
            }
        )

        result = 0
        for txn in matching_transactions:
            value_date = txn.get("ValueDate")
            next_date = value_date + timedelta(days=1)
            gtltdate = {"$gte": value_date, "$lt": next_date}

            # Find matches in the vendor collection
            matches = Vendor.get_collection(vendor_phrase).find(
                {
                    "totalCost": txn.get("WithdrawalAmt"),
                    "orderDate": gtltdate,
                    **additional_filters,
                }
            )

            matched_ids = []
            for match in matches:
                matched_ids.append(match.get("_id"))

            # If there is exactly one match, update the transaction
            if len(matched_ids) == 1:
                res = self.transactions.update_one(
                    {"_id": txn.get("_id")}, {"$set": {field: matched_ids[0]}}
                )
                result += res.modified_count
            elif len(matched_ids) > 1:
                # If there are multiple matches, log a warning and do not update the transaction
                log.warn(
                    f"Multiple matches found for transaction: {txn.get('_id')}, not updating. Matches: {matched_ids}"
                )
        if result == 0:
            log.warn(f"No matches found for vendor: {vendor_phrase}")

        return result

    def insert_vendor(self, vendor_phrase) -> InsertManyResult:
        toCSV = False

        self.parser = Vendor.get_parser(vendor_phrase)(
            read_json_files_from_folder(Vendor.get_data_folder(vendor_phrase))
        )

        df = self.ingest_parsed_data(self.parser, vendor_phrase, toCSV)

        if df.empty:
            log.warn(f"No valid {vendor_phrase} file paths were provided.")
            return InsertManyResult(acknowledged=True, inserted_ids=[])

        records = df.to_dict(orient="records")

        result = InsertManyResult(acknowledged=True, inserted_ids=[])
        try:
            result = Vendor.get_collection(vendor_phrase).insert_many(
                records, ordered=False
            )
        except Exception as e:
            log.warn(f"Error inserting records: {e}")

        return result

    def ingest_vendor_data(self, toCSV=False, debug=False) -> int:
        log.info("Ingesting data from Vendor JSON files...")

        records_inserted = 0
        for vendor_phrase in ["zomato", "zepto"]:
            log.debug(f"Inserting vendor {vendor_phrase}")
            result = self.insert_vendor(vendor_phrase)
            records_inserted += len(result.inserted_ids)

        return records_inserted

    def move_processed_files_to_old(self) -> int:
        log.info("renaming processed files...")

        valid_files = [
            file
            for file in self.hdfc_files
            + self.icici_files
            + self.zomato_files
            + self.zepto_files
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
