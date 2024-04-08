import pandas as pd
import re


class OrderParser:
    invalid_init = False

    def __init__(self, json_data_list: list[dict]):
        self.json_data_list = json_data_list
        if len(self.json_data_list) == 0:
            self.invalid_init = True

    def _extract_dishes(self, dish_string):
        dishes = []
        # Split the dish string into individual items
        items = dish_string.split(", ")
        # Extract quantity and dish name for each item
        for item in items:
            # Use regular expression to extract quantity and dish name
            match = re.match(r"(\d+)\s*x\s*(.*)", item)
            if match:
                quantity = int(match.group(1))
                name = match.group(2).strip()
                dishes.append(dict(name=name, quantity=quantity))

        return dishes

    def _parse_orders(self):
        orders = []
        for json_data in self.json_data_list:
            for order_id, order_info in json_data["entities"]["ORDER"].items():
                order = {
                    "_id": order_info["orderId"],
                    "totalCost": order_info["totalCost"],
                    "orderDate": order_info["orderDate"],
                    "status": order_info["status"],
                    "deliveryAddress": order_info["deliveryDetails"]["deliveryAddress"],
                    "restaurantName": order_info["resInfo"]["name"],
                    "restaurantRating": order_info["resInfo"]["rating"][
                        "aggregate_rating"
                    ],
                    "restaurantThumb": order_info["resInfo"]["thumb"],
                    "paymentStatus": order_info.get("paymentStatus", ""),
                    "dishString": order_info.get("dishString", ""),
                }
                orders.append(order)
        return orders

    def read_data(self):
        if self.invalid_init:
            raise ValueError("No valid file paths were provided.")
        orders_data = self._parse_orders()
        orders_df = pd.DataFrame(orders_data)

        orders_df["_id"] = orders_df["_id"].astype(str)
        orders_df["totalCost"] = pd.to_numeric(orders_df["totalCost"].str[1:])

        orders_df["orderDate"] = pd.to_datetime(
            orders_df["orderDate"], format="%B %d, %Y at %I:%M %p"
        )
        orders_df["status"] = orders_df["status"].astype(int)
        orders_df["paymentStatus"] = orders_df["paymentStatus"].astype(int)

        orders_df.drop_duplicates(inplace=True)

        orders_df["dishes"] = orders_df["dishString"].apply(self._extract_dishes)

        orders_df.sort_values(by="orderDate", inplace=True)
        return orders_df
