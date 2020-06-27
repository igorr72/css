import time
import random
import logging

from queue import Queue
from threading import Thread, Lock
from typing import List, Dict

from kitchendata import Order, Config
from orderstate import OrderState

# Collecting all dispatch threads to wait for them before closing main thread
dispatch_queue = Queue()

# lock will protect modification of a global data structure orders_state
lock = Lock()

# Each order will have an individual state, protected by an individual lock
orders_state = {}

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

class Kitchen:
    """Kitchen is the main class that processes all orders and manages all async threads"""

    def __init__(self, orders: List[Order], config: Config) -> None:
        self.logger = logging.getLogger(__name__)
        self.orders = orders
        self.config = config


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


    def fulfill_order(self, order_num: int, order: Order) -> None:
        """Main logic: create new state for the order; dispatch courier"""

        with lock:
            self.logger.info(f"with global lock: add new OrderState for order {order_num}")
            orders_state[order_num] = OrderState(order, time.time())
        
        courier = Thread(target=self.dispatch_order, args=(order_num,), name=f"dispatch_order_{order_num}", daemon=True)
        courier.start()
        dispatch_queue.put(courier) # will wait for that thread to finish


    def dispatch_order(self, order_num: int) -> None:
        """Dispatch courier as soon as order is received"""

        delay = random.uniform(self.config.pickup_min_sec, self.config.pickup_max_sec)
        self.logger.info(f"dispatching courier for order_num {order_num} with delay {delay}")
        time.sleep(delay)

        with lock:
            self.logger.info(f"with global lock: remove OrderState for order {order_num}")
            del orders_state[order_num]


    def run(self, debug_level: int = 0) -> None:
        """Main function (main thread) to iterate over all orders"""

        set_logger(debug_level)

        self.logger.debug(f"Orders: {self.orders}")
        self.logger.info(f"Config: {self.config}")

        self.logger.warning(f"Start processing: order count={len(self.orders)}")

        # consume from order queue at given rate
        feed = Thread(target=self.accept_orders, name="accept_orders", daemon=True)
        feed.start()

        delay = 3 * self.input_delay()
        self.logger.info(f"waiting for {delay} sec before checking dispatch queue")
        time.sleep(delay)
        
        # waiting for all dispatched couriers to finish
        while not dispatch_queue.empty():
            t = dispatch_queue.get()
            self.logger.debug(f"waiting for {t} to finish")
            t.join()

        self.logger.warning(f"Finish processing: order count={len(orders_state)}")
        assert len(orders_state) == 0
