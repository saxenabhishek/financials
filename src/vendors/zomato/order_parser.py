import pandas as pd
import re


class OrderParser:
    def __init__(self, json_data_list):
        self.json_data_list = json_data_list

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
                dish_name = match.group(2).strip()
                dishes.append((dish_name, quantity))
        return [
            dict(dish_name=dish_name, quantity=quantity)
            for dish_name, quantity in dishes
        ]

    def _parse_orders(self):
        orders = []
        dishes = []
        for json_data in self.json_data_list:
            for order_id, order_info in json_data["entities"]["ORDER"].items():
                order = {
                    "orderId": order_info["orderId"],
                    "totalCost": order_info["totalCost"],
                    "orderDate": order_info["orderDate"],
                    "status": order_info["status"],
                    "deliveryAddress": order_info["deliveryDetails"]["deliveryAddress"],
                    "restaurantName": order_info["resInfo"]["name"],
                    "restaurantRating": order_info["resInfo"]["rating"][
                        "aggregate_rating"
                    ],
                    "paymentStatus": order_info.get("paymentStatus", ""),
                    "dishString": order_info.get("dishString", ""),
                }
                for element in self._extract_dishes(order_info.get("dishString", "")):
                    dishes.append(dict(orderId=order_id, **element))
                orders.append(order)
        return orders, dishes

    def create_dataframe(self):
        orders_data, dishes_data = self._parse_orders()
        orders_df = pd.DataFrame(orders_data)
        dishes_df = pd.DataFrame(dishes_data)

        orders_df["orderId"] = orders_df["orderId"].astype(str)
        dishes_df["orderId"] = dishes_df["orderId"].astype(str)

        orders_df["orderDate"] = pd.to_datetime(
            orders_df["orderDate"], format="%B %d, %Y at %I:%M %p"
        )

        orders_df["totalCost"] = pd.to_numeric(orders_df["totalCost"].str[1:])

        orders_df.drop_duplicates(inplace=True)
        dishes_df.drop_duplicates(inplace=True)

        orders_df = orders_df.sort_values(by="orderDate")
        return orders_df, dishes_df
