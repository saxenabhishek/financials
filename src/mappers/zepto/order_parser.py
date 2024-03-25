import pandas as pd
from src.utils import get_logger

log = get_logger(__name__)


class OrderParser:
    def __init__(self, json_data_list):
        self.json_data_list = json_data_list
        log.info(f"total json data files: {len(json_data_list)}")

    def _parse_orders(self):
        orders = []
        items = []

        for json_data in self.json_data_list:
            for order_data in json_data.get("orders", []):
                order = {
                    "orderId": order_data["id"],
                    "totalCost": order_data["grandTotalAmount"],
                    "orderDate": order_data["placedTime"],
                    "status": order_data["status"],
                    "paymentStatus": order_data["paymentStatus"],
                    "itemQuantity": order_data["itemQuantityCount"],
                }
                orders.append(order)

                for product in order_data.get("productsNamesAndCounts", []):
                    dish = {
                        "orderId": order_data["id"],
                        "dishName": product["name"],
                        "quantity": product["count"],
                    }
                    items.append(dish)

        return orders, items

    def create_dataframe(self):
        orders_data, items_data = self._parse_orders()

        orders_df = pd.DataFrame(orders_data)
        dishes_df = pd.DataFrame(items_data)

        # Convert orderDate to datetime
        orders_df["orderDate"] = pd.to_datetime(orders_df["orderDate"])

        # Convert totalCost to numeric and divide by 100
        orders_df["totalCost"] = pd.to_numeric(orders_df["totalCost"]) / 100

        # Drop duplicates
        orders_df.drop_duplicates(inplace=True)
        dishes_df.drop_duplicates(inplace=True)

        # Sort orders_df by orderDate
        orders_df = orders_df.sort_values(by="orderDate")

        return orders_df, dishes_df
