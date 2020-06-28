from dataclasses import dataclass

from .kitchendata import Order

@dataclass
class OrderState:
    order: Order
    order_start: float
