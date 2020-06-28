import pytest
import sys
import pathlib
import time
import logging

from unittest.mock import Mock, patch
from threading import Lock
from collections import Counter

from orders_simulation.kitchen import Kitchen, set_logger
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


def test_accept_orders():
    """Testing two things: input rate(exec time) & total number of orders"""

    test_kitchen = Kitchen(orders, config)

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


def test_fulfill_order():
    """Test two things: new Order state created; new dispatch_order issued"""

    def mock_dispatch_order(order_num: int):
        pass

    test_kitchen = Kitchen(orders, config)
    test_kitchen.dispatch_order = mock_dispatch_order

    # empty before
    assert test_kitchen.dispatch_queue.empty()
    assert test_kitchen.orders_state == {}

    test_kitchen.fulfill_order(25, object)

    # not empty
    assert not test_kitchen.dispatch_queue.empty()
    assert 25 in test_kitchen.orders_state
    assert isinstance(test_kitchen.orders_state[25], OrderState)

def test_dispatch_order():
    """Test the delay was in given range & state was removed (order picked up)"""
    
    accuracy = 0.1
    min_sec = 0.2
    max_sec = min_sec + 2*accuracy

    test_kitchen = Kitchen(orders, config)

    test_kitchen.config.pickup_min_sec = min_sec
    test_kitchen.config.pickup_max_sec = max_sec

    # empty before
    assert test_kitchen.orders_state == {}

    test_kitchen.orders_state[25] = object

    start = time.time()
    test_kitchen.dispatch_order(25)
    elapsed = time.time() - start

    # empty after
    assert test_kitchen.orders_state == {}

    # delay within given range
    assert elapsed < test_kitchen.config.pickup_max_sec + accuracy
    assert elapsed > test_kitchen.config.pickup_min_sec - accuracy

def test_run():
    """Make sure all orders were processed/dispatched"""
    
    test_kitchen = Kitchen(orders, config)
    test_kitchen.run()

    assert len(orders) > 0
    assert len(test_kitchen.orders_state) == 0
