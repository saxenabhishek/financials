import pandas as pd
from src.utils import get_logger
from typing import List

log = get_logger(__name__)


class ZeptoOrderParser:
    invalid_init = False

    def __init__(self, json_data_list: list[dict]):
        self.json_data_list = json_data_list
        if len(self.json_data_list) == 0:
            self.invalid_init = True
        log.info(f"total json data files: {len(json_data_list)}")

    def _parse_orders(self) -> List[dict]:
        orders = []
        for json_data in self.json_data_list:
            for order_data in json_data.get("orders", []):
                order = {
                    "_id": order_data["id"],
                    "totalCost": order_data["grandTotalAmount"],
                    "orderDate": order_data["placedTime"],
                    "status": order_data["status"],
                    "paymentStatus": order_data["paymentStatus"],
                    "itemQuantity": order_data["itemQuantityCount"],
                    "deliveryTimeSec": order_data["totalDeliveryTimeInSeconds"],
                }
                order["items"] = [
                    {"name": product["name"], "quantity": product["count"]}
                    for product in order_data.get("productsNamesAndCounts", [])
                ]
                orders.append(order)

        return orders

    def read_data(self) -> pd.DataFrame:
        if self.invalid_init:
            raise ValueError("No valid file paths were provided.")

        orders_data = self._parse_orders()

        orders_df = pd.DataFrame(orders_data)

        orders_df["_id"] = orders_df["_id"].astype(str)
        # Convert orderDate to datetime
        orders_df["orderDate"] = pd.to_datetime(orders_df["orderDate"])

        # Convert totalCost to numeric and divide by 100
        orders_df["totalCost"] = pd.to_numeric(orders_df["totalCost"]) / 100

        # Sort orders_df by orderDate
        orders_df = orders_df.sort_values(by="orderDate")

        return orders_df
