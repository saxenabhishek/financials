from src.utils import get_logger, read_json_files_from_folder
from src.vendors.zomato.order_parser import OrderParser
import pandas as pd
from datetime import datetime


log = get_logger(__name__)


class MapZomatoData:
    folder_path = "zomato_orders"

    def __init__(self, transactions_df):
        # take in df which will be used to map data here instead of reading from file
        self.transactions_df = transactions_df

        log.info("reading zomato order data...")
        self.read_order_data()
        log.info("Reading icic transactions data...")
        self.filter_vendor_data()

    @staticmethod
    def serialize_timestamp(obj):
        if isinstance(obj, datetime):
            return obj.timestamp()

    @staticmethod
    def row_to_dict(row):
        row_dict = row.to_dict()
        row_dict["orderDate"] = row_dict["orderDate"].timestamp()
        # for key in row_dict:
        #     if isinstance(row_dict[key], datetime):
        #         row_dict[key] = row_dict[key].timestamp()
        return row_dict

    def read_order_data(self):

        order_parser = OrderParser(read_json_files_from_folder(self.folder_path))

        self.orders_df, self.dishes_df = order_parser.create_dataframe()
        self.orders_df = self.orders_df[self.orders_df["paymentStatus"] == 1]
        self.orders_df = self.orders_df.drop(
            ["restaurantRating", "paymentStatus", "status"], axis=1
        )
        self.orders_df["dictData"] = self.orders_df.apply(self.row_to_dict, axis=1)

        self.orders_df.sort_values(by="orderDate", inplace=True)

        # only added to help in merging
        self.orders_df["date"] = self.orders_df["orderDate"].dt.date

    def filter_vendor_data(self):
        self.transactions_df["ValueDate"] = pd.to_datetime(
            self.transactions_df["ValueDate"]
        ).dt.date

        phrase = "zomato"
        subset = self.transactions_df[
            self.transactions_df["Narration"].str.contains(phrase, case=False)
        ]
        self.icici_data = subset

    def doMapping(self):
        self.icici_data.sort_values(by="ValueDate", inplace=True)
        joined_df = pd.merge(
            self.icici_data,
            self.orders_df[["dictData", "totalCost", "date"]],
            how="outer",
            left_on=["WithdrawalAmt", "ValueDate"],
            right_on=["totalCost", "date"],
        )

        joined_df.drop(["totalCost", "date"], axis=1, inplace=True)

        nan_mask = joined_df.isna().any(axis=1)

        # Filter the DataFrame to get rows with NaN values
        unmerged_df = joined_df[nan_mask]
        merged_df = joined_df[~nan_mask]

        log.info(f"Number of merged rows: {merged_df.shape[0]}")
        log.info(
            f"Number of orders which aren't mapped : {merged_df.shape[0] - self.orders_df.shape[0]}"
        )

        return merged_df, unmerged_df
