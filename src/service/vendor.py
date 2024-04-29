from typing import Literal, List, Dict
from src.db import mongo
from src.vendors.zepto.order_parser import ZeptoOrderParser as ZeptoOrderParser
from src.vendors.zomato.order_parser import OrderParser as ZomatoOrderParser
from src.vendors.eat_sure import EatSureOrderParser

from pymongo import collection


class Vendor:

    vendor_list = ["zomato", "zepto", "blinkit", "eatSure", "starbucks", "compass"]

    # TODO: read values from vendor_list not sure if it is possible to
    vendors_type = Literal[
        "zomato", "zepto", "blinkit", "eatSure", "starbucks", "compass"
    ]

    # TODO: make this a consistent pydantic model
    vendor_data: Dict[vendors_type, dict] = {
        "zomato": {
            "regex": ["zomato"],
            "field": "zomato",
            "collection": "zomato",
            "folder": "zomato_orders",
            "parser": ZomatoOrderParser,
        },
        "zepto": {
            "regex": ["zepto"],
            "field": "zepto",
            "collection": "zepto",
            "folder": "zepto_orders",
            "parser": ZeptoOrderParser,
        },
        "blinkit": {
            "regex": ["blin", "grofer"],
            "field": "blinkit",
            "collection": "blinkit",
            "folder": "blinkit_orders",
            "parser": lambda x: x,
        },
        "eatSure": {
            "regex": ["rebel", "59724445"],
            "field": "eatSure",
            "collection": "eatSure",
            "folder": "eat_sure_orders",
            "parser": EatSureOrderParser,
        },
        "starbucks": {
            "regex": ["starb"],
            "field": "starbucks",
            "collection": "starbucks",
            "folder": "",
            "parser": None,
        },
        "compass": {
            "regex": ["compa", "33345433"],
            "field": "compass",
            "collection": "compass",
            "folder": "",
            "parser": None,
        },
    }

    @classmethod
    def get_narration_regex(cls, phrase: vendors_type) -> str:
        regex_strings = cls.vendor_data[phrase]["regex"]
        if len(regex_strings) > 1:
            regex_phrase = "|".join(regex_strings)
        else:
            regex_phrase = regex_strings[0]
        return regex_phrase

    @classmethod
    def get_transaction_foreign_field(cls, phrase: vendors_type) -> str:
        return cls.vendor_data[phrase]["field"]

    @classmethod
    def get_collection(cls, phrase: vendors_type) -> collection.Collection:
        return mongo[cls.vendor_data[phrase]["collection"]]

    @classmethod
    def get_data_folder(cls, phrase: vendors_type):
        return cls.vendor_data[phrase]["folder"]

    @classmethod
    def get_parser(cls, phrase: vendors_type):
        return cls.vendor_data[phrase]["parser"]
