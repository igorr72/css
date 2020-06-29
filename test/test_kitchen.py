import pytest
import sys
import pathlib
import time
import logging

from unittest.mock import Mock, patch
from threading import Lock
from collections import Counter

from orders_simulation.kitchen import Kitchen, set_logger, min_ttl
from orders_simulation.kitchendata import load_orders, load_config, Order
from orders_simulation.orderstate import OrderState

cur_dir = pathlib.Path(__file__).parent

orders_path = cur_dir.joinpath("fixture", "orders.json")
config_path = cur_dir.joinpath("fixture", "config.json")

orders = load_orders(orders_path, errors_sink = sys.stderr)
config = load_config(config_path, errors_sink = sys.stderr)


def test_input_delay():
    test_kitchen = Kitchen(orders, config)
    test_kitchen.config.intake_orders_per_sec = 5

    assert test_kitchen.input_delay() == 0.2 # 1.0 / 5

def test_min_ttl():
    arr = [(2, 0.1)]
    assert min_ttl(arr) == 2

    arr.append((3, -0.1))
    assert min_ttl(arr) == 3

def test_accept_orders():
    """Testing two things: input rate(exec time) & total number of orders"""

    test_kitchen = Kitchen(orders, config)
    test_kitchen.config.intake_orders_per_sec = 10

    lock = Lock() # just in case when input rate is very high...    
    called_count = 0
    accuracy = 0.1

    expected = test_kitchen.input_delay() * len(orders)
    
    def mock_fulfill_order(order_num: int, order: Order):
        nonlocal called_count, lock
        with lock:
            called_count += 1

    test_kitchen.fulfill_order = mock_fulfill_order

    start = time.time()
    test_kitchen.accept_orders()
    elapsed = time.time() - start

    assert called_count == len(orders)
    assert abs(elapsed - expected) < accuracy


def test_make_room():
    test_kitchen = Kitchen(orders, config)

    assert test_kitchen.shelves["hot"] == 0 # before
    assert test_kitchen.make_room("hot") == "hot"
    assert test_kitchen.shelves["hot"] == 1 # after

    # simulate overflow of "hot" shelf
    test_kitchen.shelves["hot"] = test_kitchen.config.capacity["hot"]

    assert test_kitchen.shelves["overflow"] == 0 # before
    assert test_kitchen.make_room("hot") == "overflow"
    assert test_kitchen.shelves["overflow"] == 1 # after

    # simulate overflow of "overflow" shelf
    test_kitchen.shelves["overflow"] = test_kitchen.config.capacity["overflow"]

    # overflow shelf supposed to have at least one order (it should be full in fact)
    order = Order(id="xxx", name="taco", temp="hot", shelfLife=1, decayRate=1)
    test_kitchen.orders_state[30] = OrderState(order, time.time(), shelf="overflow")

    assert test_kitchen.orders_state[30].wasted == False # before make_room
    assert test_kitchen.make_room("hot") == "overflow"
    assert test_kitchen.orders_state[30].wasted == True # after make_room


def test_fulfill_order():
    """Test two things: new Order state created; new dispatch_order issued"""

    def mock_dispatch_order(order_num: int):
        pass

    test_kitchen = Kitchen(orders, config)
    test_kitchen.dispatch_order = mock_dispatch_order

    # empty before
    assert test_kitchen.dispatch_queue.empty()
    assert test_kitchen.orders_state == {}

    order = Order(id="xxx", name="taco", temp="hot", shelfLife=1, decayRate=1)
    test_kitchen.fulfill_order(25, order)

    # not empty
    assert not test_kitchen.dispatch_queue.empty()
    assert 25 in test_kitchen.orders_state
    assert isinstance(test_kitchen.orders_state[25], OrderState)


def test_dispatch_order_ok():
    """Test the delay was in given range & state was removed (order picked up)"""
    
    test_kitchen = Kitchen(orders, config)

    test_kitchen.config.pickup_min_sec = 0.2
    test_kitchen.config.pickup_max_sec = 0.4

    accuracy = 0.1

    # empty before
    assert test_kitchen.orders_state == {}
    assert test_kitchen.stats_waste == []

    order = Order(id="xxx", name="taco", temp="hot", shelfLife=1, decayRate=1)
    state = OrderState(order, order_recieved=time.time(), wasted=False)
    test_kitchen.orders_state[25] = state

    start = time.time()
    test_kitchen.dispatch_order(25)
    elapsed = time.time() - start

    # empty after
    assert test_kitchen.orders_state == {}
    assert test_kitchen.stats_waste == []

    # delay within given range
    assert elapsed < test_kitchen.config.pickup_max_sec + accuracy
    assert elapsed > test_kitchen.config.pickup_min_sec - accuracy


def test_dispatch_order_wasted():
    """Make sure we collected statistics about wasted order"""

    test_kitchen = Kitchen(orders, config)

    test_kitchen.config.pickup_min_sec = 0.01
    test_kitchen.config.pickup_max_sec = 0.02

    # empty before
    assert test_kitchen.orders_state == {}
    assert test_kitchen.stats_waste == []

    now = time.time()
    state = OrderState(object, order_recieved=now, wasted=True)
    test_kitchen.orders_state[33] = state

    test_kitchen.dispatch_order(33)

    # after: orders are empty, but stats_waste is not
    assert test_kitchen.orders_state == {}
    assert len(test_kitchen.stats_waste) == 1

    # Checking that actual data stored in stats_waste
    assert test_kitchen.stats_waste[0].order_num == 33
    assert test_kitchen.stats_waste[0].order_state == state

    # execution speed as expected (courier dispatched -> delay -> courier arrived)
    arrived_at = test_kitchen.stats_waste[0].courier_arrived_at
    assert abs(arrived_at - now) < 2 * test_kitchen.config.pickup_max_sec


def test_run():
    """Make sure all orders were processed/dispatched"""

    test_kitchen = Kitchen(orders, config)
    test_kitchen.run()

    assert len(orders) > 0
    assert len(test_kitchen.orders_state) == 0
