from src.utils import get_logger, read_json_files_from_folder
from src.vendors.zomato.order_parser import OrderParser
import pandas as pd
from datetime import timedelta


log = get_logger(__name__)


class MapZomatoData:
    folder_path = "zomato_orders"

    def __init__(self):
        # take in df which will be used to map data here instead of reading from file
        log.info("reading zomato order data...")
        self.read_order_data()

        log.info("Reading icic transactions data...")
        self.read_icici_data()

    def read_order_data(self):
        order_parser = OrderParser(read_json_files_from_folder(self.folder_path))

        self.orders_df, self.dishes_df = order_parser.create_dataframe()
        self.orders_df = self.orders_df[self.orders_df["paymentStatus"] == 1]
        self.orders_df = self.orders_df.drop(
            ["restaurantRating", "paymentStatus", "status"], axis=1
        )

        self.orders_df.sort_values(by="orderDate", inplace=True)

        self.orders_df["date"] = self.orders_df["orderDate"].dt.date

    def read_icici_data(self):
        df = pd.read_csv("icici_data.csv")
        df["ValueDate"] = pd.to_datetime(df["ValueDate"]).dt.date

        phrase = "zomato"
        subset = df[df["Narration"].str.contains(phrase, case=False)]
        self.icici_data = subset

    def doMapping(self, time_window=timedelta(hours=12)):
        self.icici_data.sort_values(by="ValueDate", inplace=True)
        joined_df = pd.merge(
            self.icici_data,
            self.orders_df,
            how="outer",
            left_on=["WithdrawalAmt", "ValueDate"],
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
        # # Drop rows with NaN values
        # joined_df = joined_df.dropna()

        return merged_df, unmerged_df
