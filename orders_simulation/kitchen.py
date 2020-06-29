import time
import random
import logging

from queue import Queue
from threading import Thread, Lock
from typing import List, Dict, Tuple

from .kitchendata import Order, Config, shelf_types
from .orderstate import OrderState, Waste

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
        level = level
    )

def min_ttl(ttl_orders: List[Tuple[int, float]]) -> int:
    """Finds and retunrs the order number with smallest TTL (time-to-live)"""

    order_num, min_ttl = ttl_orders[0]

    for num, ttl in ttl_orders:
        if ttl < min_ttl:
            min_ttl = ttl
            order_num = num

    return order_num

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

        # Keep statistics for wasted orders
        self.stats_waste: List[Waste] = []

        # Counters for each shelf
        self.shelves: Dict[str, int] = { shelf: 0 for shelf in shelf_types }


    def input_delay(self) -> float:
        """Calculates a delay for a single order"""

        return 1.0 / self.config.intake_orders_per_sec


    def accept_orders(self) -> None:
        """Simulating real-world input queue"""

        delay = self.input_delay()
        self.logger.info(f"Start accepting orders with delay={delay} sec")

        for order_num, order in enumerate(self.orders):
            time.sleep(delay)

            fulfill = Thread(target=self.fulfill_order, args=(order_num,order,), name=f"fulfill_order_{order_num}", daemon=True)
            fulfill.start()


    def make_room(self, desired_shelf: str) -> str:
        """Returns a shelf name where to put a new order. This function might modify
        the other orders states to make room if all shelfs are full"""

        if self.shelves[desired_shelf] < self.config.capacity[desired_shelf]:
            self.shelves[desired_shelf] += 1
            return desired_shelf

        if self.shelves["overflow"] < self.config.capacity["overflow"]:
            self.shelves["overflow"] += 1
            return "overflow"

        ttl_orders = [
            (order_num, state.pickup_ttl(self.config.pickup_max_sec))
            for order_num, state in self.orders_state.items() if state.shelf == "overflow"
        ]

        order_num = min_ttl(ttl_orders)

        # throw away the order with smallest TTL
        self.orders_state[order_num].wasted = True

        return "overflow"


    def fulfill_order(self, order_num: int, order: Order) -> None:
        """Main logic: create new state for the order; dispatch courier"""

        with self.lock:
            self.logger.info(f"with global lock: add new OrderState for order {order_num}")
            shelf = self.make_room(order.temp)
            self.orders_state[order_num] = OrderState(order, time.time(), shelf=shelf)
        
        courier = Thread(target=self.dispatch_order, args=(order_num,), name=f"dispatch_order_{order_num}", daemon=True)
        courier.start()
        self.dispatch_queue.put(courier) # will wait for that thread to finish


    def dispatch_order(self, order_num: int) -> None:
        """Dispatch courier as soon as order is received"""

        delay = random.uniform(self.config.pickup_min_sec, self.config.pickup_max_sec)
        self.logger.info(f"dispatching courier for order_num {order_num} with delay {delay}")
        time.sleep(delay)

        with self.lock:
            state = self.orders_state[order_num]
            if state.wasted:
                self.logger.error(f"order {order_num} is a waste: delivery failed")
                self.stats_waste.append(Waste(order_num, time.time(), state))
            else:
                self.logger.info(f"order {order_num} delivered successfully; value: {state.value()}")
                self.logger.debug(f"order {order_num} state: {state}")

            del self.orders_state[order_num]


    def run(self, debug_level: int = 0) -> None:
        """Main function (main thread) to iterate over all orders"""

        set_logger(debug_level)

        self.logger.info(f"Config: {self.config}")
        self.logger.warning(f"Start processing: order count={len(self.orders)}")

        # consume from order queue at given rate
        feed = Thread(target=self.accept_orders, name="accept_orders", daemon=True)
        feed.start()

        delay = 2 * self.input_delay()
        self.logger.info(f"waiting for two first orders before checking dispatch queue")
        time.sleep(delay)
        
        # waiting for all dispatched couriers to finish
        while not self.dispatch_queue.empty():
            t = self.dispatch_queue.get()
            self.logger.debug(f"waiting for {t} to finish")
            t.join()

        self.logger.warning(f"Finish processing: remaining orders={len(self.orders_state)}")
        self.logger.warning(f"Stats: wasted orders={len(self.stats_waste)}")
