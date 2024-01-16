import pickle

from abc import ABC, abstractmethod
from typing import Any
from pyarrow import serialize_pandas, deserialize_pandas, ArrowInvalid
from datetime import datetime
from pandas import DataFrame, to_datetime
from dateutil.parser import parse


class XcomBackend(ABC):

    def __init__(self):
        pass

    @staticmethod
    def to_bytes(value: Any):
        """
        Convert value to bytes
        :param value:
        :return:
        """
        if isinstance(value, DataFrame):
            return serialize_pandas(value).to_pybytes()
        elif isinstance(value, datetime):
            return str(value)
        else:
            return pickle.dumps(value)

    @staticmethod
    def from_bytes(value: Any):
        """
        Converts a value from bytes using a hierarchical process
        since we don't know what the exact type will be
        :param value:
        :return:
        """
        try:
            # try deserializing to pandas object
            return deserialize_pandas(value)
        except (ArrowInvalid, Exception):
            # check if in bytes or a str
            data = pickle.loads(value)

            try:
                if isinstance(data, str):
                    parse(data)
                    return to_datetime(data)
                else:
                    return data
            except ValueError:
                return data

    @abstractmethod
    def push(self, key: str, value: Any) -> None:
        pass

    @abstractmethod
    def pull(self, key: str) -> Any:
        pass
