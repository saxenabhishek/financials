from src.vendors.zepto.order_parser import OrderParser
from src.utils import read_json_files_from_folder, get_logger
import pandas as pd
from datetime import timedelta

log = get_logger(__name__)


class MapZeptoData:
    folder_path = "zepto_orders"

    def __init__(self):
        log.info("reading zepto order data...")

        self._read_order_data()

        log.info("Reading icic transactions data...")
        self._read_icici_data()

    def _read_order_data(self):
        order_parser = OrderParser(read_json_files_from_folder(self.folder_path))

        self.orders_df, self.items_df = order_parser.create_dataframe()

        self.orders_df = self.orders_df[self.orders_df["paymentStatus"] == "SUCCESS"]
        # self.orders_df = self.orders_df.drop(
        #     ["deliveryAddress", "restaurantRating", "paymentStatus", "status"], axis=1
        # )

        self.orders_df.sort_values(by="orderDate", inplace=True)

        self.orders_df["date"] = self.orders_df["orderDate"].dt.date

    def _read_icici_data(self):
        df = pd.read_csv("icici_data.csv")
        df["ValueDate"] = pd.to_datetime(df["ValueDate"]).dt.date

        phrase = "zepto"  # replace 'your_phrase' with the phrase you are looking for
        subset = df[df["Narration"].str.contains(phrase, case=False)]

        self.icici_data = subset.copy()
        self.icici_data.sort_values(by="ValueDate", inplace=True)

    def doMapping(self, time_window=timedelta(hours=12)):

        joined_df = pd.merge(
            self.icici_data,
            self.orders_df,
            how="left",
            left_on=["Withdrawal Amount (INR )", "Value Date"],
            right_on=["totalCost", "date"],
        )

        nan_mask = joined_df.isna().any(axis=1)

        # Filter the DataFrame to get rows with NaN values
        unmerged_df = joined_df[nan_mask]
        merged_df = joined_df[~nan_mask]

        log.info(f"Number of merged rows: {merged_df.shape[0]}")
        log.info(
            f"Number of orders which aren't mapped : {merged_df.shape[0] - self.orders_df.shape[0]}"
        )
        log.info(f"Number of transactions which aren't mapped : {unmerged_df.shape[0]}")

        return joined_df, unmerged_df
