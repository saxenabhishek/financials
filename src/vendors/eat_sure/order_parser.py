from src.vendors.parser import Parser
from typing import List
import pandas as pd

from src.utils import get_logger

log = get_logger(__name__)


class EatSureOrderParser(Parser):
    def __init__(self, json_data_list: list[dict]):
        super().__init__(json_data_list)

    def extract_order_details(self, order_data: dict):
        details = {}

        # Extract order details
        details["_id"] = order_data["order_id"]
        details["orderDate"] = order_data["order_date"]
        details["status"] = order_data["status"]
        details["totalAmount"] = order_data["total_amount"]
        details["deliveryCharges"] = order_data["delivery_charges"]
        details["packagingCharges"] = order_data["packaging_charges"]

        # Extract payment method and status
        if "payment_status" in order_data:
            details["paymentStatus"] = order_data["payment_status"]

        if order_data["payment_mode_used"]:
            if len(order_data["payment_mode_used"]) > 1:
                log.error(
                    f"Assumption of one payment mode has failed for {order_data['order_id']}"
                )
            mode = order_data["payment_mode_used"][0]
            payment_mode_details = {
                "paid_by": mode["paid_by"],
                "display_name": mode["display_name"],
                "totalCost": mode["amount"],
            }
            details.update(payment_mode_details)

        # Extract store details
        if order_data["products"]:
            products = []
            log.debug(
                "has Product: %s %s", order_data["order_id"], order_data["products"]
            )
            for product in order_data["products"]:
                product_details = {
                    "name": product["name"],
                    "quantity": product["quantity"],
                    "price": product["price"],
                }
                products.append(product_details)
            details["products"] = products
            details["isCombo"] = False
        elif order_data["brands"]:
            log.debug("has Brands: %s %s", order_data["order_id"], order_data["brands"])
            for brand in order_data["brands"]:
                combo_details_list = []
                for combo_item in brand["combo"]:
                    combo_details = {
                        "name": combo_item["name"],
                        "quantity": combo_item["quantity"],
                        "price": combo_item["price_with_tax"],
                    }
                    combo_details_list.append(combo_details)
            details["products"] = combo_details_list
            details["isCombo"] = True
        return details

    def _parse_orders(self) -> List[dict]:
        orders = []
        for json_data in self.json_data_list:
            past_orders = json_data["data"]["pastOrders"]
            for order_data in past_orders:
                orders.append(self.extract_order_details(order_data))
        return orders

    def _read_data(self) -> pd.DataFrame:
        df = pd.DataFrame(self._parse_orders())

        df["_id"] = df["_id"].astype(str)

        df["orderDate"] = pd.to_datetime(df["orderDate"])

        cols_to_convert = ['totalCost', 'totalAmount', 'deliveryCharges']
        df[cols_to_convert] = df[cols_to_convert].apply(pd.to_numeric)

        df.sort_values("orderDate", inplace=True)
        return df
