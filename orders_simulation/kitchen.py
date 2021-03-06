import time
import random
import logging

from queue import Queue
from threading import Thread, Lock, Event
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

        # Keep an Event for each dispatch thread to be able to terminate
        # the delivery in case the order expires pior actual pickup
        self.dispatch_events = {}

        # one event to manage cleanup thread
        self.cleanup_event = Event()

        # lock will protect modification of a global data structure orders_state
        self.lock = Lock()

        # Keep individual states for each order
        self.orders_state: Dict[int, OrderState] = {}

    def input_delay(self) -> float:
        """Calculates a delay for a single order"""

        return 1.0 / self.config.intake_orders_per_sec

    def terminate_delivery(self, order_num) -> None:
        """Terminate delivery because order moved to waste"""

        self.dispatch_events[order_num].set()  # terminate delivery

        self.logger.debug(
            f"order {order_num} sending Event signal to terminate delivery")

    def active_orders(self) -> Dict[int, OrderState]:
        """Active orders from every shelf except WASTE.
        WASTE shelf orders are always in closed state."""

        return {
            order_num: state
            for order_num, state in self.orders_state.items()
            if not state.closed()
        }

    def waste_orders(self) -> Dict[int, OrderState]:
        """All orders from waste shelf (historical)"""

        return {
            order_num: state
            for order_num, state in self.orders_state.items()
            if state.current_shelf() == WASTE
        }

    def shelf_orders(self, shelf: str) -> Dict[int, OrderState]:
        """Active orders from a particular shelf"""

        return {
            order_num: state
            for order_num, state in self.active_orders().items()
            if state.current_shelf() == shelf
        }

    def count_shelves(self) -> Counter:
        """Get a snapshot of all orders and return a count for orders on each shelf"""

        # only active orders for normal shelfs
        shelves_names = [
            state.current_shelf()
            for _, state in self.active_orders().items()
        ]

        # all orders (historical) for the [virtual] WASTE shelf
        waste_names = [WASTE for _ in self.waste_orders()]

        return Counter(shelves_names + waste_names)

    def remove_unhealty(self) -> Tuple[int, int]:
        """Throw away orders with value <= 0. 
        Returs two counters: (active_orders, expired_orders)."""

        active = 0
        expired = 0

        for order_num, state in self.active_orders().items():
            active += 1
            val = state.value()

            if val <= 0:
                self.logger.error(
                    f"order {order_num} STATUS=unhealthy age={state.total_age()}, value={val}")

                expired += 1
                state.move_to_waste(value=val)
                self.terminate_delivery(order_num)

                self.logger.debug(
                    f"order {order_num} details: {state}")

        return active, expired

    def find_recoverable_orders(self) -> Dict[int, float]:
        """Check all non-OVERFLOW shelves to find ones that are not full"""

        recoverable = {}
        counter = self.count_shelves()

        for order_num, state in self.shelf_orders(OVERFLOW).items():
            desired_shelf = state.order.temp
            if counter[desired_shelf] < self.config.capacity[desired_shelf]:
                util = counter[desired_shelf] / \
                    self.config.capacity[desired_shelf]
                recoverable[order_num] = util

        return recoverable

    def recover_from_overflow(self) -> int:
        """Recover one order from overflow shelf, if possible"""

        recoverable = self.find_recoverable_orders()

        if recoverable:
            # will recover order with smallest shelf utilization (i.e. more free space)
            order_num, _ = min_value(recoverable)

            state = self.orders_state[order_num]
            state.move(ShelfHistory(shelf=state.order.temp))

            self.logger.warning(
                f"order {order_num} STATUS=recovered from {OVERFLOW} back to desired shelf {state.order.temp}")

            return 1  # one order recovered

        return 0  # did not recover anything

    def cleanup(self) -> None:
        """Run periodic checks for unhealthy orders and overflow orders"""

        while True:
            self.cleanup_event.wait(self.config.cleanup_delay)

            with self.lock:
                active, expired = self.remove_unhealty()
                recovered = self.recover_from_overflow()

            if expired > 0 or recovered > 0:
                self.logger.debug(
                    f"cleanup summary: checked {active} orders, expired {expired}, recovered {recovered}")

            if self.cleanup_event.is_set():
                break

    def accept_orders(self) -> None:
        """Simulating real-world input queue"""

        delay = self.input_delay()
        self.logger.info(f"Start accepting orders with delay={delay} sec")

        for order_num, order in enumerate(self.orders):
            time.sleep(delay)

            fulfill = Thread(target=self.fulfill_order, args=(
                order_num, order,), name=f"fulfill_order_{order_num}", daemon=True)
            fulfill.start()

    def make_room(self, desired_shelf: str) -> str:
        """Returns a shelf name where to put a new order. This function might modify
        the other orders states to make room if all shelfs are full"""

        count = self.count_shelves()

        if count[desired_shelf] < self.config.capacity[desired_shelf]:
            return desired_shelf

        if count[OVERFLOW] < self.config.capacity[OVERFLOW]:
            self.logger.warning(
                f"shelf {desired_shelf} is full ({count[desired_shelf]}/{self.config.capacity[desired_shelf]})" +
                f"; will use {OVERFLOW} ({count[OVERFLOW]}/{self.config.capacity[OVERFLOW]})")
            return OVERFLOW

        self.logger.warning(
            f"{OVERFLOW} shelf is FULL ({count[OVERFLOW]}/{self.config.capacity[OVERFLOW]})")

        if self.recover_from_overflow() == 0:

            orders_ttl = {
                order_num: state.pickup_ttl()
                for order_num, state in self.shelf_orders(OVERFLOW).items()
            }

            # throw away the order with smallest TTL
            order_num, pickup_ttl = min_value(orders_ttl)

            self.logger.error(
                f"order {order_num} STATUS=discarded (no space) with lowest pickup_ttl={pickup_ttl}")

            self.orders_state[order_num].move_to_waste()
            self.terminate_delivery(order_num)

            self.logger.debug(
                f"order {order_num} details: {self.orders_state[order_num]}")

        return OVERFLOW

    def snapshot(self):
        """Dumps the current state of all shelves to logger"""

        counter = self.count_shelves()

        for shelf in shelf_types + [OVERFLOW, WASTE]:
            count = counter[shelf]
            if shelf in self.config.capacity:
                capacity = self.config.capacity[shelf]
                status = "FULL" if count == capacity else "OK"
            else:
                capacity = "UNLIMITED"
                status = "--->"

            self.logger.debug(
                f"SNAPSHOT: shelf {shelf.ljust(9)} {status.ljust(4)} {count}/{capacity}")

    def fulfill_order(self, order_num: int, order: Order) -> None:
        """Main logic: create new state for the order; dispatch courier"""

        with self.lock:
            # find a proper shelf for new order
            shelf = self.make_room(order.temp)

            # simulate pickup delay
            delay = random.randint(
                self.config.pickup_min_sec, self.config.pickup_max_sec)

            # add new order into our main data structure
            self.orders_state[order_num] = OrderState(
                order=order, init_state=ShelfHistory(shelf), pickup_sec=delay)

            self.logger.info(
                f"order {order_num} STATUS=new shelf={shelf}; pickup in {delay} sec")

            # dumps fresh counters after new order was added
            self.snapshot()

            # add new Event in case we need to terminate pickup prematurely
            self.dispatch_events[order_num] = Event()

        courier = Thread(target=self.dispatch_order, args=(
            order_num, delay,), name=f"dispatch_order_{order_num}", daemon=True)

        self.dispatch_queue.put(courier)
        courier.start()

    def dispatch_order(self, order_num: int, delay: int) -> None:
        """Dispatch courier as soon as order is received"""

        self.logger.debug(f"order {order_num} pickup delay {delay}")

        self.dispatch_events[order_num].wait(delay)

        with self.lock:
            # close current order whether it was just moved to waste by cleanup or not
            state = self.orders_state[order_num]

            if state.current_shelf() == WASTE:
                self.logger.error(
                    f"order {order_num} STATUS=pickup_canceled age={state.total_age()}")
            else:
                state.close()

                self.logger.info(
                    f"order {order_num} STATUS=delivered age={state.total_age()}, value={state.last_value}")

            self.logger.debug(f"order {order_num} details: {state}")

    def run(self, debug_level: int = 0) -> None:
        """Main function (main thread) to iterate over all orders"""

        set_logger(debug_level)

        self.logger.info(self.config)
        self.logger.warning(
            f"Start kitchen: order count={len(self.orders)}")

        # start cleanup process in background
        Thread(target=self.cleanup, name=f"cleanup", daemon=True).start()

        # consume from order queue at given rate in main thread
        self.accept_orders()

        # waiting for all dispatched couriers to finish
        while not self.dispatch_queue.empty():
            t = self.dispatch_queue.get()
            t.join()

        # stop cleanup thread
        self.cleanup_event.set()

        self.logger.warning(
            f"Stop kitchen: unfinished orders={len(self.active_orders())}")

        wasted = self.waste_orders()
        self.logger.warning(f"Wasted orders count={len(wasted)}")
        self.logger.debug(f"Wasted orders details: {wasted}")
