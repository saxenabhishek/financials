from src.bank_parser.hdfc_parser import HdfcExcelDataReader
from src.bank_parser.icici_parser import IciciExcelDataReader
from src.utils import (
    get_logger,
    get_all_file_paths,
)
import pandas as pd
from src.service.const import TransactionIndicator, Category
from src.db import mongo
from src.vendors.zomato.mapper import MapZomatoData
import os


log = get_logger(__name__)


class DataIngestionService:
    def __init__(self):
        log.info("Initializing Data Readers Objects")
        self.hdfc_files = get_all_file_paths(r"bank_transactions\hdfc_data")
        self.icici_files = get_all_file_paths(r"bank_transactions\icici_data")
        self.hdfc_parser = HdfcExcelDataReader(self.hdfc_files)
        self.icici_parser = IciciExcelDataReader(self.icici_files)

    def ingest_data(self, toCSV=False) -> int:
        log.info("Ingesting data from HDFC and ICICI Excel files...")
        hdfc_df = self.hdfc_parser.read_data()
        icici_df = self.icici_parser.read_data()

        if toCSV:
            hdfc_df.to_csv("hdfc_data.csv", index=False)
            icici_df.to_csv("icici_data.csv", index=False)

        df = pd.concat([hdfc_df, icici_df], ignore_index=True)

        df["TransactionIndicator"] = TransactionIndicator.PENDING.value
        df["Category"] = Category.UNKNOWN.value

        # All transactions with zero withdrawal amount are considered settled
        df.loc[df["WithdrawalAmt"] == 0, "TransactionIndicator"] = (
            TransactionIndicator.SETTLED.value
        )

        transactions = mongo["transactions"]

        # Drop existing collection records for testing purposes
        transactions.drop()

        records = df.to_dict(orient="records")

        # Insert records into MongoDB collection
        log.info("Inserting records into MongoDB...")
        try:
            result = transactions.insert_many(records, ordered=False)
        except Exception as e:
            log.error(f"Error inserting records: {e}")

        log.info("renaming processed files...")
        for file in self.hdfc_files + self.icici_files:
            os.rename(file, file + ".old")

        return len(result.inserted_ids)

    def _map_transactions_to_vendors(self, df: pd.DataFrame) -> pd.DataFrame:
        # will figuire out later
        log.info("Mapping transactions to vendors...")
        # ideally this can all be done in the database
        zomato_mapper = MapZomatoData(df)
        zomato_transactions, _ = zomato_mapper.doMapping()
        df = df.merge(
            zomato_transactions[["RefNo", "dictData"]],
            on="RefNo",
            how="left",
        )
        df.rename(columns={"dictData": "ZomatoDictData"}, inplace=True)
        df["ZomatoDictData"].fillna({}, inplace=True)
        return df
