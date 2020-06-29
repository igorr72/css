import json

from dataclasses import dataclass
from typing import List, Dict, Optional
from pathlib import Path

OVERFLOW = "overflow"
WASTE = "waste"

shelf_types = ["hot", "cold", "frozen"]


@dataclass
class Order:
    id: str
    name: str
    temp: str
    shelfLife: int
    decayRate: float


@dataclass
class Config:
    capacity: Dict[str, int]
    intake_orders_per_sec: int
    pickup_min_sec: float
    pickup_max_sec: float


def load_json(filename: str, errors_sink):
    """Reusable function to read a file & parse json. Returns None in case of errors."""

    if not Path(filename).is_file():
        print(
            f"load_json: required file does not exist: '{filename}'", file=errors_sink)
        return None

    try:
        with open(filename, "r") as f:
            data = json.load(f)

    except json.JSONDecodeError as e:
        print(
            f"load_json: JSONDecodeError from file '{filename}'; {e}", file=errors_sink)
        return None

    return data


def load_orders(filename: str, errors_sink) -> Optional[List[Order]]:
    """Loading kitchen orders from json into Order class"""

    orders = load_json(filename, errors_sink)
    if orders == None:
        return None

    if not isinstance(orders, list):
        print(
            f"load_orders: Expected a list of orders, got {orders.__class__.__name__}", file=errors_sink)
        return None

    try:
        obj_list = [Order(**order) for order in orders]

        allowed_shelves = set([t for t in shelf_types])
        temps = set([order.temp for order in obj_list])

        if not temps.issubset(allowed_shelves):
            print(
                f"load_orders: unexpected temperature(s): {temps.difference(allowed_shelves)}", file=errors_sink)
            return None

    except Exception as e:
        print(
            f"load_orders: Can't convert json data into {Order}: {e}", file=errors_sink)
        return None

    return obj_list[:13]


def load_config(filename: str, errors_sink) -> Optional[Config]:
    """Loading configuration parameters from json into Config class"""

    config = load_json(filename, errors_sink)
    if config == None:
        return None

    try:
        cfg = Config(**config)

        expected = sorted(shelf_types + [OVERFLOW])
        configured = sorted(list(cfg.capacity.keys()))

        if expected != configured:
            print(
                f"load_config: expected shelves {expected}; got {configured}", file=errors_sink)
            return None

    except Exception as e:
        print(
            f"load_config: Can't convert json data into {Config}: {e}", file=errors_sink)
        return None

    return cfg
