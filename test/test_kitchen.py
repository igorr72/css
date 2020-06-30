import pytest
import sys
import pathlib
import time
import logging
import math

from unittest.mock import Mock, patch
from threading import Lock
from collections import Counter

from orders_simulation.kitchen import Kitchen, set_logger, min_value
from orders_simulation.kitchendata import load_orders, load_config, Order, OVERFLOW, WASTE
from orders_simulation.orderstate import OrderState, ShelfHistory

cur_dir = pathlib.Path(__file__).parent

orders_path = cur_dir.joinpath("fixture", "orders.json")
config_path = cur_dir.joinpath("fixture", "config.json")

orders = load_orders(orders_path, errors_sink=sys.stderr)
config = load_config(config_path, errors_sink=sys.stderr)


def test_input_delay():
    test_kitchen = Kitchen(orders, config)
    test_kitchen.config.intake_orders_per_sec = 5

    assert test_kitchen.input_delay() == 0.2  # 1.0 / 5


def test_min_value():
    d = {2: 0.1}
    assert min_value(d) == (2, 0.1)

    d.update({3: -0.1})
    assert min_value(d) == (3, -0.1)


def test_accept_orders():
    """Testing two things: input rate(exec time) & total number of orders"""

    test_kitchen = Kitchen(orders, config)

    lock = Lock()  # just in case when input rate is very high...
    called_count = 0
    delay_total = 0.0

    def mock_fulfill_order(order_num: int, order: Order):
        nonlocal called_count, lock
        with lock:
            called_count += 1

    def mock_delay(d):
        nonlocal delay_total
        delay_total += d

    test_kitchen.fulfill_order = mock_fulfill_order
    with patch("time.sleep", mock_delay):
        test_kitchen.accept_orders()

        assert called_count == len(orders)
        assert math.isclose(
            delay_total, test_kitchen.input_delay() * len(orders))


def test_shelf_orders():
    test_kitchen = Kitchen(orders, config)

    order = Order(id="xxx", name="taco", temp="hot", shelfLife=1, decayRate=1)
    state25 = OrderState(
        order, history=[ShelfHistory(shelf="hot", reason="new_order")])
    test_kitchen.orders_state[25] = state25

    order = Order(id="yyy", name="taco", temp="hot", shelfLife=1, decayRate=1)
    hist = [
        ShelfHistory(shelf="hot", added_at=0,
                     removed_at=1, reason="new_order"),
        ShelfHistory(shelf=WASTE, added_at=1, reason="overflow_full")
    ]
    state33 = OrderState(order, history=hist)
    test_kitchen.orders_state[33] = state33

    wasted = test_kitchen.shelf_orders(WASTE)
    assert len(wasted) == 1
    assert wasted[0] == (33, state33)

    hot = test_kitchen.shelf_orders("hot")
    assert len(hot) == 1
    assert hot[0] == (25, state25)


def test_active_orders():
    test_kitchen = Kitchen(orders, config)

    order = Order(id="xxx", name="taco", temp="hot", shelfLife=1, decayRate=1)
    state25 = OrderState(
        order, history=[ShelfHistory(shelf="hot", reason="new_order")])
    test_kitchen.orders_state[25] = state25

    # order 33 is technically not closed (removed_at==None for latest shelf)
    order = Order(id="yyy", name="taco", temp="hot", shelfLife=1, decayRate=1)
    hist = [
        ShelfHistory(shelf="hot", added_at=0,
                     removed_at=1, reason="new_order"),
        ShelfHistory(shelf=WASTE, added_at=1, reason="overflow_full")
    ]
    state33 = OrderState(order, history=hist)
    test_kitchen.orders_state[33] = state33

    active = test_kitchen.active_orders()

    assert len(active) == 2


def test_find_recoverable_orders():
    test_kitchen = Kitchen(orders, config)

    order = Order(id="xxx", name="taco", temp="hot",
                  shelfLife=1, decayRate=0.5)
    state25 = OrderState(
        order, history=[ShelfHistory(shelf=OVERFLOW, reason="new_order")])
    test_kitchen.orders_state[25] = state25

    order = Order(id="yyy", name="icecream",
                  temp="cold", shelfLife=1, decayRate=1)
    state33 = OrderState(
        order, history=[ShelfHistory(shelf=OVERFLOW, reason="new_order")])
    test_kitchen.orders_state[33] = state33

    test_kitchen.shelves["hot"] = test_kitchen.config.capacity["hot"]
    test_kitchen.shelves["cold"] = test_kitchen.config.capacity["cold"] - 1

    #import pdb; pdb.set_trace()
    d = test_kitchen.find_recoverable_orders([25, 33])
    assert list(d.keys()) == [33]


def test_recover_order():
    test_kitchen = Kitchen(orders, config)

    order = Order(id="yyy", name="icecream",
                  temp="cold", shelfLife=1, decayRate=1)
    state33 = OrderState(
        order, history=[ShelfHistory(shelf=OVERFLOW, reason="new_order")])
    test_kitchen.orders_state[33] = state33

    test_kitchen.shelves["cold"] = test_kitchen.config.capacity["cold"] - 1

    assert len(state33.history) == 1  # before

    test_kitchen.recover_order(33)

    assert len(state33.history) == 2  # after
    # increased
    assert test_kitchen.shelves["cold"] == test_kitchen.config.capacity["cold"]
    assert state33.history[-1].added_at == state33.history[-2].removed_at


def test_remove_from_overflow():
    test_kitchen = Kitchen(orders, config)

    order = Order(id="xxx", name="taco", temp="hot",
                  shelfLife=1, decayRate=0.5)
    state25 = OrderState(
        order, history=[ShelfHistory(shelf=OVERFLOW, reason="new_order")])
    test_kitchen.orders_state[25] = state25

    order = Order(id="yyy", name="taco", temp="hot", shelfLife=1, decayRate=1)
    state33 = OrderState(
        order, history=[ShelfHistory(shelf=OVERFLOW, reason="new_order")])
    test_kitchen.orders_state[33] = state33

    assert len(test_kitchen.shelf_orders(OVERFLOW)) == 2

    test_kitchen.shelves[OVERFLOW] = 2
    test_kitchen.config.capacity[OVERFLOW] = 2

    test_kitchen.remove_from_overflow({25: 1.0, 33: -1.0})

    assert test_kitchen.shelves[OVERFLOW] == 2  # does not change
    # it supposed to remove order with lowest TTL - with greater decay rate
    assert test_kitchen.orders_state[33].history[-1].shelf == WASTE
    # previous state is closed
    assert test_kitchen.orders_state[33].history[-2].removed_at != None


def test_make_room_raise():
    test_kitchen = Kitchen(orders, config)

    # simulate overflow of "hot" shelf
    test_kitchen.shelves["hot"] = test_kitchen.config.capacity["hot"]

    # simulate overflow of OVERFLOW shelf
    test_kitchen.shelves[OVERFLOW] = test_kitchen.config.capacity[OVERFLOW]

    # state is empty by default, including overflow shelf
    with pytest.raises(RuntimeError) as e:
        test_kitchen.make_room("hot")
        assert "INTERNAL ERROR" in str(e.value)


def test_make_room():
    test_kitchen = Kitchen(orders, config)

    assert test_kitchen.shelves["hot"] == 0  # before
    assert test_kitchen.make_room("hot") == "hot"
    assert test_kitchen.shelves["hot"] == 1  # after

    # simulate overflow of "hot" shelf
    test_kitchen.shelves["hot"] = test_kitchen.config.capacity["hot"]

    assert test_kitchen.shelves[OVERFLOW] == 0  # before
    assert test_kitchen.make_room("hot") == OVERFLOW
    assert test_kitchen.shelves[OVERFLOW] == 1  # after

    order = Order(id="yyy", name="ice", temp="cold", shelfLife=1, decayRate=1)
    state33 = OrderState(
        order, history=[ShelfHistory(shelf=OVERFLOW, reason="new_order")])
    test_kitchen.orders_state[33] = state33

    # simulate overflow of OVERFLOW shelf
    test_kitchen.shelves[OVERFLOW] = 1
    test_kitchen.config.capacity[OVERFLOW] = 1

    assert test_kitchen.shelves["cold"] == 0  # before

    new_shelf = test_kitchen.make_room("hot")

    # by default recoverable order should be found
    assert test_kitchen.shelves[OVERFLOW] == 1  # no changed
    assert new_shelf == OVERFLOW
    assert test_kitchen.shelves["cold"] == 1  # was recovered
    # recovered
    assert test_kitchen.orders_state[33].history[-1].shelf == "cold"

    order = Order(id="xxx", name="taco", temp="hot", shelfLife=1, decayRate=1)
    state25 = OrderState(
        order, history=[ShelfHistory(shelf=OVERFLOW, reason="new_order")])
    test_kitchen.orders_state[25] = state25

    new_shelf = test_kitchen.make_room("hot")

    assert test_kitchen.shelves[OVERFLOW] == 1  # no changed
    assert new_shelf == OVERFLOW
    assert test_kitchen.orders_state[25].history[-1].shelf == WASTE  # removed


def test_fulfill_order():
    """Test two things: new Order state created; new dispatch_order issued"""

    def mock_dispatch_order(order_num: int):
        pass

    test_kitchen = Kitchen(orders, config)
    test_kitchen.dispatch_order = mock_dispatch_order

    # empty before
    assert test_kitchen.dispatch_queue.empty()
    assert test_kitchen.orders_state == {}

    order25 = Order(id="xxx", name="taco", temp="hot",
                    shelfLife=1, decayRate=1)

    test_kitchen.make_room = Mock(return_value="FOO")
    test_kitchen.fulfill_order(25, order25)

    # not empty
    assert not test_kitchen.dispatch_queue.empty()
    assert 25 in test_kitchen.orders_state
    assert test_kitchen.orders_state[25].order == order25
    assert test_kitchen.orders_state[25].history[0].shelf == "FOO"


def test_dispatch_order_ok():
    """Test the delay was in given range & state updated (order picked up)"""

    test_kitchen = Kitchen(orders, config)
    delay = None

    def capture_delay(d):
        nonlocal delay
        delay = d

    # manually add a test order
    order = Order(id="xxx", name="taco", temp="hot", shelfLife=1, decayRate=1)
    state = OrderState(
        order, history=[ShelfHistory(shelf="hot", reason="new_order")])
    test_kitchen.orders_state[25] = state
    test_kitchen.shelves["hot"] = 1

    with patch("time.sleep", capture_delay):
        test_kitchen.dispatch_order(25)
        assert delay <= test_kitchen.config.pickup_max_sec
        assert delay >= test_kitchen.config.pickup_min_sec

        assert test_kitchen.orders_state[25].history[-1].added_at != None
        assert test_kitchen.orders_state[25].history[-1].removed_at != None
        assert test_kitchen.shelves["hot"] == 0  # descreased


def test_dispatch_order_wasted():
    """Make sure we collected statistics about wasted order"""

    test_kitchen = Kitchen(orders, config)

    # manually add a wasted test order
    order = Order(id="xxx", name="taco", temp="hot", shelfLife=1, decayRate=1)
    hist = [
        ShelfHistory(shelf="hot", added_at=0,
                     removed_at=1, reason="new_order"),
        ShelfHistory(shelf=WASTE, added_at=1, reason="overflow_full")
    ]
    state = OrderState(order, history=hist)
    test_kitchen.orders_state[33] = state

    with patch("time.sleep", Mock()):
        test_kitchen.dispatch_order(33)

        assert test_kitchen.orders_state[33].history[-1].added_at != None
        assert test_kitchen.orders_state[33].history[-1].removed_at != None


def test_run():
    """Make sure all orders were processed/dispatched"""

    test_kitchen = Kitchen(orders, config)
    test_kitchen.run()

    assert len(orders) > 0
    assert len(test_kitchen.orders_state) == len(orders)
