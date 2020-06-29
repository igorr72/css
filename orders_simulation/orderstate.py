import time

from dataclasses import dataclass

from .kitchendata import Order

@dataclass
class OrderState:
    order: Order
    order_recieved: float
    wasted: bool = False
    shelf: str = None

    def value(self) -> float:
        """Calculate the value for the order on a shelf"""
        return 0.5

@dataclass
class Waste:
    order_num: int
    courier_arrived_at: float
    order_state: OrderState
