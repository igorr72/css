import time

from dataclasses import dataclass
from typing import List

from .kitchendata import Order


@dataclass
class ShelfHistory:
    shelf: str
    reason: str
    added_at: float = time.time()
    removed_at: float = None


@dataclass
class OrderState:
    order: Order
    history: List[ShelfHistory]

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

    def pickup_ttl(self, pickup_max_sec: int) -> float:
        """Difference between order TTL and worst case pickup time"""

        # Use the information about dispatch (max time for courier to arrive)
        max_time_to_pickup = pickup_max_sec - sum(self.ages())

        return self.ttl() - max_time_to_pickup
