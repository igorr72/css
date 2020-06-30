import time
import random
import logging

from queue import Queue
from threading import Thread, Lock
from typing import List, Dict, Tuple

from .kitchendata import Order, Config, shelf_types, WASTE, OVERFLOW
from .orderstate import OrderState, ShelfHistory

from sys import maxsize as MAXINT


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
    """Finds and returns the order number with smallest value"""

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

        # Counters for each shelf
        self.shelves: Dict[str, int] = {
            shelf: 0 for shelf in shelf_types + [OVERFLOW]}

    def input_delay(self) -> float:
        """Calculates a delay for a single order"""

        return 1.0 / self.config.intake_orders_per_sec

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

    def active_orders(self):
        """Generate a list of active orders"""

        return [
            (order_num, state)
            for order_num, state in self.orders_state.items()
            if state.history[-1].removed_at == None
        ]

    def find_recoverable_orders(self, order_nums: List[int]) -> Dict[int, float]:
        """Check all non-OVERFLOW shelves to find ones that are not full"""

        recoverable = {}

        for order_num in order_nums:
            shelf = self.orders_state[order_num].order.temp
            if self.shelves[shelf] < self.config.capacity[shelf]:
                utilization = 1.0 * \
                    self.shelves[shelf]/self.config.capacity[shelf]
                recoverable[order_num] = utilization

        return recoverable

    def recover_order(self, order_num: int):
        """Recover given order from overflow shelf to its original shelf"""

        state = self.orders_state[order_num]  # just a pointer to a data
        shelf = state.order.temp

        now = time.time()
        state.history[-1].removed_at = now  # close current state (overflow)
        hist = ShelfHistory(shelf=shelf, added_at=now, reason="recovery")

        state.history.append(hist)
        self.shelves[shelf] += 1  # added order to that shelf

        self.logger.warning(
            f"order {order_num} recovered from {OVERFLOW} back to {shelf}")

    def remove_from_overflow(self, ttl_orders: Dict[int, float]):
        """Remove order with lowest pickup_ttl from overflow shelf"""

        order_num, ttl = min_value(ttl_orders)

        # throw away the order with smallest TTL
        now = time.time()
        self.orders_state[order_num].history[-1].removed_at = now
        hist = ShelfHistory(shelf=WASTE, added_at=now, reason="overflow_full")

        self.orders_state[order_num].history.append(hist)

        self.logger.error(
            f"overflow shelf is full, moving previous order {order_num} to waste: pickup_ttl={ttl}")

    def make_room(self, desired_shelf: str) -> str:
        """Returns a shelf name where to put a new order. This function might modify
        the other orders states to make room if all shelfs are full"""

        if self.shelves[desired_shelf] < self.config.capacity[desired_shelf]:
            self.logger.info(
                f"requested shelf {desired_shelf} has enough room: OK")
            self.shelves[desired_shelf] += 1
            return desired_shelf

        if self.shelves[OVERFLOW] < self.config.capacity[OVERFLOW]:
            self.logger.warning(
                f"requested shelf {desired_shelf} is full: use overflow")
            self.shelves[OVERFLOW] += 1
            return OVERFLOW

        ttl_orders = {
            order_num: state.pickup_ttl(self.config.pickup_max_sec)
            for order_num, state in self.shelf_orders(OVERFLOW)
            if state.history[-1].removed_at == None  # active orders only
        }

        if len(ttl_orders) != self.config.capacity[OVERFLOW]:
            self.logger.error(
                f"INTERNAL ERROR: overflow shelf expected to be full ({self.config.capacity[OVERFLOW]}) but it was not: {len(ttl_orders)}")
            raise RuntimeError("INTERNAL ERROR")

        recoverable = self.find_recoverable_orders(ttl_orders.keys())
        if recoverable:
            self.logger.debug(f"found recoverable orders: {recoverable}")
            # will recover order with smallest shelf utilization (i.e. more free space)
            order_num, _ = min_value(recoverable)
            self.recover_order(order_num)
        else:
            self.logger.debug(
                f"throw-away candidates from overflow shelf: {ttl_orders}")
            self.remove_from_overflow(ttl_orders)

        return OVERFLOW

    def log_snapshot_all(self):
        """Dumps the current state of all shelves to logger"""

        for shelf, count in self.shelves.items():
            self.logger.debug(
                f"SNAPSHOT: shelf={shelf}, count={count}, capacity={self.config.capacity[shelf]}")

    def fulfill_order(self, order_num: int, order: Order) -> None:
        """Main logic: create new state for the order; dispatch courier"""

        with self.lock:
            shelf = self.make_room(order.temp)
            hist = ShelfHistory(shelf=shelf, reason="new_order")
            self.logger.info(f"put new order {order_num} onto shelf {shelf}")
            self.orders_state[order_num] = OrderState(
                order=order, history=[hist])
            self.log_snapshot_all()

        courier = Thread(target=self.dispatch_order, args=(
            order_num,), name=f"dispatch_order_{order_num}", daemon=True)
        courier.start()
        self.dispatch_queue.put(courier)  # will wait for that thread to finish

    def dispatch_order(self, order_num: int) -> None:
        """Dispatch courier as soon as order is received"""

        delay = random.uniform(self.config.pickup_min_sec,
                               self.config.pickup_max_sec)
        self.logger.info(
            f"dispatching courier for order_num {order_num} with delay {delay}")
        time.sleep(delay)

        with self.lock:
            state = self.orders_state[order_num]
            last_shelf = state.history[-1]
            last_shelf.removed_at = time.time()  # pick-up time stamp

            if last_shelf.shelf == WASTE:
                when = last_shelf.removed_at - last_shelf.added_at
                self.logger.error(
                    f"delivery failed as order {order_num} wasted {when} sec ago")
            else:
                self.logger.info(
                    f"order {order_num} delivered successfully from shelf {last_shelf.shelf}; value: {state.value()}")
                self.logger.debug(
                    f"order {order_num} full state with history: {state}")
                self.shelves[last_shelf.shelf] -= 1

    def run(self, debug_level: int = 0) -> None:
        """Main function (main thread) to iterate over all orders"""

        set_logger(debug_level)

        self.logger.info(f"Config: {self.config}")
        self.logger.warning(
            f"Start processing: order count={len(self.orders)}")

        # consume from order queue at given rate
        self.accept_orders()

        # waiting for all dispatched couriers to finish
        while not self.dispatch_queue.empty():
            t = self.dispatch_queue.get()
            self.logger.debug(f"waiting for {t} to finish")
            t.join()

        self.logger.warning(
            f"Finish processing: active orders={len(self.active_orders())}")

        wasted = self.shelf_orders(WASTE)
        self.logger.warning(f"Wasted orders count={len(wasted)}")
        self.logger.debug(f"Wasted orders={wasted}")
