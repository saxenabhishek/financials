import pandas as pd
from src.utils import get_logger
from typing import List
from abc import abstractmethod, ABCMeta

log = get_logger(__name__)


class Parser:
    __metaclass__ = ABCMeta
    invalid_init = False

    def __init__(self, json_data_list: list[dict]):
        self.json_data_list = json_data_list
        if len(self.json_data_list) == 0:
            self.invalid_init = True
        log.info(f"total json data files: {len(json_data_list)}")

    @abstractmethod
    def _parse_orders(self) -> List[dict]:
        raise NotImplementedError

    @abstractmethod
    def _read_data(self) -> pd.DataFrame:
        raise NotImplementedError

    def read_data(self) -> pd.DataFrame:
        if self.invalid_init:
            raise ValueError("No valid file paths were provided.")

        return self._read_data()
