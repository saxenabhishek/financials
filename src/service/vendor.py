from typing import Literal, List, Dict


class Vendor:

    vendor_list = ["zomato", "zepto"]

    # TODO: read values from vendor_list not sure if it is possible to
    vendors_type = Literal["zomato", "zepto"]

    # TODO: make this a consistent pydantic model
    vendor_data: Dict[vendors_type, dict] = {
        "zomato": {"regex": ["zomato"], "field": "zomato", "collection": "zomato"},
        "zepto": {"regex": ["zepto"], "field": "zepto", "collection": "zepto"},
    }

    @classmethod
    def get_narration_regex(cls, phrase: vendors_type) -> List[str]:
        return cls.vendor_data[phrase]["regex"]

    @classmethod
    def get_transaction_foreign_field(cls, phrase: vendors_type) -> str:
        return cls.vendor_data[phrase]["field"]

    @classmethod
    def get_collection(cls, phrase: vendors_type) -> str:
        return cls.vendor_data[phrase]["collection"]
