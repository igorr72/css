import time

from dataclasses import dataclass

from .kitchendata import Order

@dataclass
class OrderState:
    order: Order
    order_recieved: float
    wasted: bool = False
    shelf: str = None

    def decay_modifier(self) -> int:
        if self.order.temp == self.shelf:
            return 1 # shelf where it should be

        return 2 # overflow shelf

    def decay_rate(self) -> float:
        """Sub-expression in calculating the value of the shelved order"""

        return self.order.decayRate * self.decay_modifier()

    def age(self) -> float:
        """Sub-expression in calculating the age of the shelved order"""

        return time.time() - self.order_recieved

    def value(self) -> float:
        """Calculate the value for the order on a shelf"""

        return 1.0 - self.decay_rate() * self.age() / self.order.shelfLife

    def pickup_ttl(self, pickup_max_sec: int) -> float:
        """Difference between order TTL and MAX time to pickup"""

        # Calculate TTL (time to live) when order value becomes zero
        # self.decay_rate() * age / self.order.shelfLife == 1
        ttl = self.order.shelfLife / self.decay_rate()

        # Use the information about dispatch (max time for courier to arrive)
        max_time_to_pickup = pickup_max_sec - self.age()

        return ttl - max_time_to_pickup

@dataclass
class Waste:
    order_num: int
    courier_arrived_at: float
    order_state: OrderState
