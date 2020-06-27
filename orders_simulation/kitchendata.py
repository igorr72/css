import json

from dataclasses import dataclass
from typing import List, Optional
from pathlib import Path


@dataclass
class Order:
    id: str
    name: str
    temp: str
    shelfLife: int
    decayRate: float


@dataclass
class Config:
    capacity_hot: int
    capacity_cold: int
    capacity_frozen: int
    capacity_overflow: int
    intake_orders_per_sec: int
    pickup_min_sec: int
    pickup_max_sec: int


def load_json(filename: str, errors_sink):
    """Reusable function to read a file & parse json. Returns None in case of errors."""

    if not Path(filename).is_file():
        print(f"load_json: required file does not exist: '{filename}'", file=errors_sink)
        return None

    try:
        with open(filename, "r") as f:
            data = json.load(f)

    except json.JSONDecodeError as e:
        print(f"load_json: JSONDecodeError from file '{filename}'; {e}", file=errors_sink)
        return None

    return data


def load_orders(filename: str, errors_sink) -> Optional[List[Order]]:
    """Loading kitchen orders from json into Order class"""

    orders = load_json(filename, errors_sink)
    if orders == None:
        return None

    if not isinstance(orders, list):
        print(f"load_orders: Expected a list of orders, got {orders.__class__.__name__}", file=errors_sink)
        return None

    try:
        obj_list = [Order(**order) for order in orders]

    except Exception as e:
        print(f"load_orders: Can't convert json data into {Order}: {e}", file=errors_sink)
        return None

    return obj_list[:13]


def load_config(filename: str, errors_sink) -> Optional[Config]:
    """Loading configuration parameters from json into Config class"""

    config = load_json(filename, errors_sink)
    if config == None:
        return None

    try:
        cfg = Config(**config)

    except Exception as e:
        print(f"load_config: Can't convert json data into {Config}: {e}", file=errors_sink)
        return None

    return cfg
