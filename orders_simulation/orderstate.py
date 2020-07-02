import time

from dataclasses import dataclass
from typing import List

from .kitchendata import Order


@dataclass
class ShelfHistory:
    shelf: str
    added_at: float = time.time()
    removed_at: float = None
    value: float = None
    pickup_ttl: int = None


@dataclass
class OrderState:
    order: Order
    history: List[ShelfHistory]
    pickup_sec: int
    last_ttl: int
    last_value: float

    def __init__(self, order: Order, init_state: ShelfHistory, pickup_sec: int = 300):
        self.order = order
        self.history = [init_state]
        self.pickup_sec = pickup_sec
        self.last_ttl = self.ttl()
        self.last_value = self.value()

    def close(self, value: float = None, removed_at: float = None):
        """Close last entry in history list with timestamp and value.
        It can be called directly, but mostly from append_state() method"""

        last_stage = self.history[-1]
        last_stage.removed_at = removed_at if removed_at else time.time()

        # recalculate ttl and value
        self.last_ttl = self.pickup_ttl()
        self.last_value = value if value else self.value()

        # store both values in closed last_stage
        last_stage.pickup_ttl = self.last_ttl
        last_stage.value = self.last_value

    def move(self, state: ShelfHistory, value: float = None):
        """Close last entry in history and immediately add a new state"""

        now = time.time()
        self.close(removed_at=now, value=value)

        # removing discrepancy between two consequtive calls of time.time()
        state.added_at = now
        self.history.append(state)

    def decay_modifiers(self) -> List[int]:
        return [
            1 if self.order.temp == hist.shelf else 2
            for hist in self.history
        ]

    def decay_rates(self) -> float:
        """Sub-expression in calculating the value of the shelved order"""

        return [self.order.decayRate * modifier for modifier in self.decay_modifiers()]

    def ages(self) -> float:
        """Sub-expression in calculating the age of the shelved order"""

        res = []

        for hist in self.history:
            removed_at = hist.removed_at if hist.removed_at else time.time()
            res.append(removed_at - hist.added_at)

        return res

    def value(self) -> float:
        """Calculate the value for the order for entire shelf history"""

        decays = [a * d for a, d in zip(self.ages(), self.decay_rates())]

        return 1.0 - sum(decays) / self.order.shelfLife

    def ttl(self) -> float:
        """Calculate TTL (time to live) if order remains on current shelf"""

        # Calculate TTL (time to live) when order's value would become zero
        # a1*d1 + a2*d2 + a3*d3 == shelfLife
        # a3(aka ttl) = (shelfLife - a1*d1 - a2*d2) / d3

        ages = self.ages()
        decay_rates = self.decay_rates()

        prior_decays = [a * d for a, d in zip(ages[:-1], decay_rates[:-1])]

        return (self.order.shelfLife - sum(prior_decays)) / decay_rates[-1]

    def pickup_ttl(self) -> float:
        """Difference between order TTL and worst case pickup time"""

        # Use the information about dispatch (max time for courier to arrive)
        time_to_pickup = self.pickup_sec - sum(self.ages())

        return self.ttl() - time_to_pickup
