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
from orders_simulation.kitchendata import load_orders, load_config, Order, OVERFLOW, WASTE, shelf_types
from orders_simulation.orderstate import OrderState, ShelfHistory

cur_dir = pathlib.Path(__file__).parent

orders_path = cur_dir.joinpath("fixture", "orders.json")
config_path = cur_dir.joinpath("fixture", "config.json")

orders = load_orders(orders_path, errors_sink=sys.stderr)
config = load_config(config_path, errors_sink=sys.stderr)


def _order_hot(test_kitchen: Kitchen, life: int = 1):
    """Helper: generate test order #25 added to hot shelf"""

    order = Order(id="xxx", name="taco", temp="hot",
                  shelfLife=life, decayRate=1)
    state = OrderState(order, init_state=ShelfHistory(shelf="hot"))
    test_kitchen.orders_state[25] = state

    return state


def _order_cold(test_kitchen: Kitchen, life: int = 1):
    """Helper: generate test order #33 added to cold shelf"""

    order = Order(id="yyy", name="ice", temp="cold",
                  shelfLife=life, decayRate=1)
    state = OrderState(order, init_state=ShelfHistory(shelf="cold"))
    test_kitchen.orders_state[33] = state

    return state


def test_input_delay():
    """Make sure we calculate delay between orders correctly, based on rate"""

    test_kitchen = Kitchen(orders, config)
    test_kitchen.config.intake_orders_per_sec = 5

    assert test_kitchen.input_delay() == 0.2  # 1.0 / 5


def test_min_value():
    """Returns pair with lowest value"""

    d = {2: 0.1}
    assert min_value(d) == (2, 0.1)

    d.update({3: -0.1})
    assert min_value(d) == (3, -0.1)


def test_cleanup():
    """Should remove one order to waste but keep the other one"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)
    _order_cold(test_kitchen)

    count_before = test_kitchen.shelves_count()
    assert dict(count_before) == {"hot": 1, "cold": 1}

    # mock one order to return zero value
    state_hot.value = Mock(return_value=0.0)
    test_kitchen.cleanup()

    count_after = test_kitchen.shelves_count()
    assert dict(count_after) == {WASTE: 1, "cold": 1}


def test_accept_orders():
    """Testing two things: input rate & total number of orders"""

    test_kitchen = Kitchen(orders, config)

    lock = Lock()  # need a lock because of running multiple threads

    called_count = 0
    delay_total = 0.0

    def mock_fulfill_order(order_num: int, order: Order):
        nonlocal called_count, lock
        with lock:
            called_count += 1

    def mock_delay(d):
        nonlocal delay_total

        # do not need a lock because all calls are maid sequentially in main thread
        delay_total += d

    test_kitchen.fulfill_order = mock_fulfill_order

    with patch("time.sleep", mock_delay):
        test_kitchen.accept_orders()

        assert called_count == len(orders)
        assert called_count >= 5  # number of test cases in fixture
        assert math.isclose(
            delay_total, test_kitchen.input_delay() * len(orders))


def test_shelf_orders():
    """Test if returns orders from correct shelf"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)
    state_cold = _order_cold(test_kitchen)

    assert test_kitchen.shelf_orders("hot") == [(25, state_hot)]
    assert test_kitchen.shelf_orders("cold") == [(33, state_cold)]

    # now move cold order to waste
    state_cold.move(ShelfHistory(WASTE))

    assert test_kitchen.shelf_orders("cold") == []
    assert test_kitchen.shelf_orders(WASTE) == [(33, state_cold)]


def test_unfinished_orders():
    """Test if unfinished orders include waste shelf (they should)"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)
    state_cold = _order_cold(test_kitchen)

    assert (25, state_hot) in test_kitchen.unfinished_orders()
    assert (33, state_cold) in test_kitchen.unfinished_orders()

    # now move cold order to waste
    state_cold.move(ShelfHistory(WASTE))
    assert (33, state_cold) in test_kitchen.unfinished_orders()

    # now close the state
    state_cold.close()
    assert (33, state_cold) not in test_kitchen.unfinished_orders()


def test_active_orders():
    """Test if active orders include waste shelf (they should NOT)"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)
    state_cold = _order_cold(test_kitchen)

    assert (25, state_hot) in test_kitchen.active_orders()
    assert (33, state_cold) in test_kitchen.active_orders()

    # now move cold order to waste
    state_cold.move(ShelfHistory(WASTE))
    assert (33, state_cold) not in test_kitchen.active_orders()

    # now close hot order
    state_hot.close()
    assert test_kitchen.active_orders() == []


def test_shelves_count():
    """Test all active orders are included as well as all waste orders"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)
    state_cold = _order_cold(test_kitchen)

    assert test_kitchen.shelves_count() == {"hot": 1, "cold": 1}

    # now move cold order to waste
    state_cold.move(ShelfHistory(WASTE))
    assert test_kitchen.shelves_count() == {"hot": 1, "waste": 1}

    # now close hot order
    state_hot.close()
    assert test_kitchen.shelves_count() == {"waste": 1}

    # finally, close waste order ==> counters still there!
    state_cold.close()
    assert test_kitchen.shelves_count() == {"waste": 1}


def test_find_recoverable_orders():
    """Test all active orders are included as well as all waste orders"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)
    state_cold = _order_cold(test_kitchen)

    # moving both to overflow to make them recoverable
    state_hot.move(ShelfHistory(OVERFLOW))
    state_cold.move(ShelfHistory(OVERFLOW))

    # build a proper countero bject
    counter = test_kitchen.shelves_count()

    # simulate load
    counter["hot"] = 1
    counter["cold"] = 2

    # simulate capacity
    test_kitchen.config.capacity["hot"] = 5
    test_kitchen.config.capacity["cold"] = 4

    assert test_kitchen.find_recoverable_orders(
        counter, [25, 33]) == {25: 0.2, 33: 0.5}

    # now adjust hot counter to simulate hot shelf reaching capacity
    counter["hot"] = test_kitchen.config.capacity["hot"]

    assert test_kitchen.find_recoverable_orders(
        counter, [25, 33]) == {33: 0.5}


def test_make_room_ok():
    """Simple cases: all shelves have room"""

    test_kitchen = Kitchen(orders, config)

    counter = test_kitchen.shelves_count()

    for shelf in shelf_types:
        assert test_kitchen.make_room(counter, shelf) == shelf


def test_make_room_overflow():
    """Simulate full capacity for each normal shelf to be redirected to overflow shelf"""

    test_kitchen = Kitchen(orders, config)

    counter = test_kitchen.shelves_count()

    for shelf in shelf_types:
        # simulate reaching capacity
        counter[shelf] = test_kitchen.config.capacity[shelf]
        assert test_kitchen.make_room(counter, shelf) == OVERFLOW


def test_make_room_recovery():
    """Test recovery from overflow shelf back to desired shelf"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)
    state_cold = _order_cold(test_kitchen)

    # moving both to overflow to make them recoverable
    state_hot.move(ShelfHistory(OVERFLOW))
    state_cold.move(ShelfHistory(OVERFLOW))

    # build a proper counter object
    counter = test_kitchen.shelves_count()

    # simulate load
    counter["hot"] = 1
    counter["cold"] = 2

    # simulate capacity
    test_kitchen.config.capacity["hot"] = 5
    test_kitchen.config.capacity["cold"] = 4

    assert test_kitchen.find_recoverable_orders(
        counter, [25, 33]) == {25: 0.2, 33: 0.5}

    # simulate frozen & OVERFLOW are full
    counter["frozen"] = test_kitchen.config.capacity["frozen"]
    counter[OVERFLOW] = test_kitchen.config.capacity[OVERFLOW]

    avail_shelf = test_kitchen.make_room(counter, "frozen")

    assert avail_shelf == OVERFLOW

    # make_room_ supposed to recover order with lowest utilization score 0.2 (hot)
    assert state_hot.history[-1].shelf == "hot"
    assert state_cold.history[-1].shelf == OVERFLOW


def test_make_room_removal():
    """Test removal from overflow shelf"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen, life=100)
    state_cold = _order_cold(test_kitchen, life=200)

    # moving both to overflow to make them recoverable
    state_hot.move(ShelfHistory(OVERFLOW))
    state_cold.move(ShelfHistory(OVERFLOW))

    # build a proper counter object
    counter = test_kitchen.shelves_count()

    # simulate OVERFLOW  and other shelves are full
    counter[OVERFLOW] = test_kitchen.config.capacity[OVERFLOW]
    counter["hot"] = test_kitchen.config.capacity["hot"]
    counter["cold"] = test_kitchen.config.capacity["cold"]

    # making sure there is nothing to recover...
    assert test_kitchen.find_recoverable_orders(counter, [25, 33]) == {}

    avail_shelf = test_kitchen.make_room(counter, "cold")

    assert avail_shelf == OVERFLOW

    # make_room_ supposed to remove order with lowest pickup_ttl which
    # which is proportional to shelfLife, i.e. shorter life dies first
    assert state_hot.history[-1].shelf == WASTE
    assert state_cold.history[-1].shelf == OVERFLOW


def test_fulfill_order():
    """Test two things: new Order state created; new dispatch_order issued"""

    delay: int = None

    def mock_dispatch_order(order_num: int, d: int):
        nonlocal delay
        delay = d

    test_kitchen = Kitchen(orders, config)
    test_kitchen.dispatch_order = mock_dispatch_order

    # empty before
    assert test_kitchen.dispatch_queue.empty()
    assert test_kitchen.orders_state == {}

    order25 = Order(id="xxx", name="taco", temp="hot",
                    shelfLife=1, decayRate=1)

    test_kitchen.make_room = Mock(return_value=OVERFLOW)
    test_kitchen.fulfill_order(25, order25)

    assert delay <= test_kitchen.config.pickup_max_sec
    assert delay >= test_kitchen.config.pickup_min_sec

    # not empty
    assert not test_kitchen.dispatch_queue.empty()
    assert 25 in test_kitchen.orders_state
    assert test_kitchen.orders_state[25].order == order25
    assert test_kitchen.orders_state[25].history[0].shelf == OVERFLOW
    assert test_kitchen.orders_state[25].history[0].removed_at == None


def test_dispatch_order_ok():
    """Pickup normal order"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)

    assert test_kitchen.shelves_count()["hot"] == 1  # before

    with patch("time.sleep", Mock()):
        test_kitchen.dispatch_order(25, 300)

        assert state_hot.history[-1].added_at != None
        assert state_hot.history[-1].removed_at != None
        assert state_hot.history[-1].shelf == "hot"
        assert state_hot.pickup_sec == 300

        assert test_kitchen.shelves_count()["hot"] == 0  # after


def test_dispatch_order_wasted():
    """Make sure we do not pickup wasted order"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)
    state_hot.move(ShelfHistory(WASTE))

    with patch("time.sleep", Mock()):
        test_kitchen.dispatch_order(25, 300)

        assert state_hot.history[-1].added_at != None
        assert state_hot.history[-1].removed_at != None
        assert state_hot.history[-1].shelf == WASTE
        assert state_hot.pickup_sec == 300


def test_run():
    """Make sure all orders were processed/dispatched"""

    test_kitchen = Kitchen(orders, config)
    test_kitchen.run()

    assert len(orders) > 0
    assert len(test_kitchen.orders_state) == len(orders)
