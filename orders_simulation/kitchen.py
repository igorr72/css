import time
import random
import logging

from queue import Queue
from threading import Thread, Lock
from typing import List, Dict, Tuple

from .kitchendata import Order, Config, shelf_types, WASTE, OVERFLOW
from .orderstate import OrderState, ShelfHistory

from sys import maxsize as MAXINT
from collections import Counter


def set_logger(debug_level: int) -> None:
    """Configure logger for kitchen orders troubleshooting"""

    if debug_level == 1:
        level = logging.INFO
    elif debug_level == 2:
        level = logging.DEBUG
    else:
        level = logging.WARNING

    logging.basicConfig(
        format='%(asctime)s %(levelname)s (thread: %(threadName)s) %(filename)s/%(funcName)s: %(message)s',
        level=level
    )


def min_value(orders_with_metric: Dict[int, float]) -> Tuple[int, float]:
    """Finds and returns the order number with smallest value associated with it"""

    min_val = MAXINT

    for num, val in orders_with_metric.items():
        if val < min_val:
            min_val = val
            order_num = num

    return order_num, min_val


class Kitchen:
    """Kitchen is the main class that processes all orders and manages all async threads"""

    def __init__(self, orders: List[Order], config: Config) -> None:
        self.logger = logging.getLogger(__name__)
        self.orders = orders
        self.config = config

        # Collecting all dispatch threads to wait for them before closing main thread
        self.dispatch_queue = Queue()

        # lock will protect modification of a global data structure orders_state
        self.lock = Lock()

        # Keep individual states for each order
        self.orders_state: Dict[int, OrderState] = {}

    def input_delay(self) -> float:
        """Calculates a delay for a single order"""

        return 1.0 / self.config.intake_orders_per_sec

    def cleanup(self) -> None:
        """Throw away orders with value <= 0"""

        for order_num, state in self.active_orders():
            val = state.value()

            if val <= 0:
                state.move(state=ShelfHistory(shelf=WASTE), value=val)

                self.logger.error(
                    f"order {order_num} became unhealthy, value: {val}")
                self.logger.debug(f"order {order_num} details: {state}")

    def accept_orders(self) -> None:
        """Simulating real-world input queue"""

        delay = self.input_delay()
        self.logger.info(f"Start accepting orders with delay={delay} sec")

        for order_num, order in enumerate(self.orders):
            time.sleep(delay)

            fulfill = Thread(target=self.fulfill_order, args=(
                order_num, order,), name=f"fulfill_order_{order_num}", daemon=True)
            fulfill.start()

    def shelf_orders(self, shelf: str) -> List[Tuple[int, OrderState]]:
        """Return a list of orders for a particular shelf"""

        return [
            (order_num, state)
            for order_num, state in self.orders_state.items()
            if state.history[-1].shelf == shelf
        ]

    def unfinished_orders(self):
        """Generate a list of active orders for any shelf"""

        return [
            (order_num, state)
            for order_num, state in self.orders_state.items()
            if state.history[-1].removed_at == None
        ]

    def active_orders(self):
        """Generate a list of active orders except WASTE"""

        return [
            (order_num, state)
            for order_num, state in self.unfinished_orders()
            if state.history[-1].shelf != WASTE
        ]

    def shelves_count(self) -> Counter:
        """Get a snapshot of all orders and return a count for orders on each shelf"""

        # only active orders for normal shelfs
        shelves_names = [
            state.history[-1].shelf
            for _, state in self.active_orders()
        ]

        # all orders (historical) for the [virtual] WASTE shelf
        waste_names = [WASTE for _ in self.shelf_orders(WASTE)]

        return Counter(shelves_names + waste_names)

    def find_recoverable_orders(self, count: Counter, order_nums: List[int]) -> Dict[int, float]:
        """Check all non-OVERFLOW shelves to find ones that are not full"""

        recoverable = {}

        for order_num in order_nums:
            shelf = self.orders_state[order_num].order.temp
            if count[shelf] < self.config.capacity[shelf]:
                utilization = count[shelf] / self.config.capacity[shelf]
                recoverable[order_num] = utilization

        return recoverable

    def make_room(self, count: Counter, desired_shelf: str) -> str:
        """Returns a shelf name where to put a new order. This function might modify
        the other orders states to make room if all shelfs are full"""

        if count[desired_shelf] < self.config.capacity[desired_shelf]:
            return desired_shelf

        if count[OVERFLOW] < self.config.capacity[OVERFLOW]:
            self.logger.warning(
                f"shelf {desired_shelf} is full ({count[desired_shelf]}/{self.config.capacity[desired_shelf]})" +
                f"; will use {OVERFLOW} ({count[OVERFLOW]}/{self.config.capacity[OVERFLOW]})")
            return OVERFLOW

        self.logger.warning(
            f"{OVERFLOW} shelf is FULL ({count[OVERFLOW]}/{self.config.capacity[OVERFLOW]})")

        orders_ttl = {
            order_num: state.pickup_ttl()
            for order_num, state in self.shelf_orders(OVERFLOW)
            if state.history[-1].removed_at == None  # active orders only
        }

        recoverable = self.find_recoverable_orders(count, orders_ttl.keys())

        if recoverable:
            # will recover order with smallest shelf utilization (i.e. more free space)
            order_num, _ = min_value(recoverable)

            state = self.orders_state[order_num]
            state.move(ShelfHistory(shelf=state.order.temp))

            self.logger.warning(
                f"order {order_num} recovered from {OVERFLOW} back to desired shelf")
        else:
            # throw away the order with smallest TTL
            order_num, pickup_ttl = min_value(orders_ttl)

            state = self.orders_state[order_num]
            state.move(ShelfHistory(shelf=WASTE))

            self.logger.error(
                f"order {order_num} with lowest pickup_ttl={pickup_ttl} goes to {WASTE}")

        self.logger.debug(
            f"order {order_num} details: {self.orders_state[order_num]}")

        return OVERFLOW

    def snapshot(self):
        """Dumps the current state of all shelves to logger"""

        for shelf, count in self.shelves_count().items():
            capacity = self.config.capacity[shelf] if shelf in self.config.capacity else "UNLIMITED"
            self.logger.debug(
                f"SNAPSHOT: shelf: {shelf}, count: {count}/{capacity}")

    def fulfill_order(self, order_num: int, order: Order) -> None:
        """Main logic: create new state for the order; dispatch courier"""

        with self.lock:
            # remove stale orders and get fresh count afterwards
            self.cleanup()
            count = self.shelves_count()

            # find a proper shelf for new order
            shelf = self.make_room(count, order.temp)

            # simulate pickup delay
            delay = int(random.uniform(
                self.config.pickup_min_sec, self.config.pickup_max_sec))

            # add new order into our main data structure
            self.orders_state[order_num] = OrderState(
                order=order, init_state=ShelfHistory(shelf), pickup_sec=delay)

            self.logger.info(
                f"put new order {order_num} onto shelf {shelf}; pickup in {delay} sec")

            # dumps fresh counters after new order was added
            self.snapshot()

        courier = Thread(target=self.dispatch_order, args=(
            order_num, delay,), name=f"dispatch_order_{order_num}", daemon=True)
        courier.start()

        # will wait for that thread to finish in main thread
        self.dispatch_queue.put(courier)

    def dispatch_order(self, order_num: int, delay: int) -> None:
        """Dispatch courier as soon as order is received"""

        time.sleep(delay)

        with self.lock:
            # remove all stale orders before attempting to pickup specific order
            self.cleanup()

            # close current order whether it was just moved to waste by cleanup or not
            state = self.orders_state[order_num]
            state.close()

            last_shelf = state.history[-1]

            if last_shelf.shelf == WASTE:
                when = last_shelf.removed_at - last_shelf.added_at
                self.logger.error(
                    f"delivery failed as order {order_num} wasted {when} sec ago")
            else:
                self.logger.info(
                    f"order {order_num} delivered successfully from shelf {last_shelf.shelf}; value: {state.last_value}")

            self.logger.debug(f"order {order_num} details: {state}")

    def run(self, debug_level: int = 0) -> None:
        """Main function (main thread) to iterate over all orders"""

        set_logger(debug_level)

        self.logger.info(f"Config: {self.config}")
        self.logger.warning(
            f"Start kitchen: order count={len(self.orders)}")

        # consume from order queue at given rate
        self.accept_orders()

        # waiting for all dispatched couriers to finish
        while not self.dispatch_queue.empty():
            t = self.dispatch_queue.get()
            t.join()

        self.logger.warning(
            f"Stop kitchen: unfinished orders={len(self.unfinished_orders())}")

        wasted = self.shelf_orders(WASTE)
        self.logger.warning(f"Wasted orders count={len(wasted)}")
        self.logger.debug(f"Wasted orders details: {wasted}")
