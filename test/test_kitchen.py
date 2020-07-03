import pytest
import sys
import pathlib
import time
import logging
import math

from unittest.mock import Mock, patch
from threading import Lock
from collections import Counter

from threading import Thread, Lock

from orders_simulation.kitchen import Kitchen, set_logger, min_value
from orders_simulation.kitchendata import load_orders, load_config, Order, OVERFLOW, WASTE, shelf_types
from orders_simulation.orderstate import OrderState, ShelfHistory

cur_dir = pathlib.Path(__file__).parent

orders_path = cur_dir.joinpath("fixture", "orders.json")
config_path = cur_dir.joinpath("fixture", "config.json")

orders = load_orders(orders_path, errors_sink=sys.stderr)
config = load_config(config_path, errors_sink=sys.stderr)


def _order_hot(test_kitchen: Kitchen, life: int = 1, order_num: int = 25):
    """Helper: generate test order #25 added to hot shelf"""

    order = Order(id="xxx", name="taco", temp="hot",
                  shelfLife=life, decayRate=1)
    state = OrderState(order, ShelfHistory("hot"), pickup_sec=10)
    test_kitchen.orders_state[order_num] = state

    return state


def _order_cold(test_kitchen: Kitchen, life: int = 1, order_num: int = 33):
    """Helper: generate test order #33 added to cold shelf"""

    order = Order(id="yyy", name="ice", temp="cold",
                  shelfLife=life, decayRate=1)
    state = OrderState(order, ShelfHistory("cold"), pickup_sec=10)
    test_kitchen.orders_state[order_num] = state

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


def test_terminate_delivery():
    """Should call Event.set() for a partucular order"""

    test_kitchen = Kitchen(orders, config)

    test_kitchen.dispatch_events[55] = Mock()

    test_kitchen.terminate_delivery(55)

    test_kitchen.dispatch_events[55].set.assert_called()


def test_active_orders():
    """Test if active orders include waste shelf (they should NOT)"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)
    state_cold = _order_cold(test_kitchen)

    assert test_kitchen.active_orders() == {25: state_hot, 33: state_cold}

    # now move cold order to waste
    state_cold.move_to_waste()
    assert test_kitchen.active_orders() == {25: state_hot}

    # now close hot order
    state_hot.close()
    assert test_kitchen.active_orders() == {}


def test_waste_orders():
    """Test if active orders include waste shelf (they should NOT)"""

    test_kitchen = Kitchen(orders, config)
    state_cold = _order_cold(test_kitchen)

    assert test_kitchen.waste_orders() == {}

    # now move cold order to waste
    state_cold.move_to_waste()

    assert test_kitchen.waste_orders() == {33: state_cold}


def test_shelf_orders():
    """Test orders placed on particular shelf"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)
    state_cold = _order_cold(test_kitchen)

    assert test_kitchen.shelf_orders("hot") == {25: state_hot}
    assert test_kitchen.shelf_orders("cold") == {33: state_cold}

    # now move cold order to waste
    state_cold.move_to_waste()
    assert test_kitchen.shelf_orders("cold") == {}

    # now close hot order
    state_hot.close()
    assert test_kitchen.shelf_orders("hot") == {}


def test_count_shelves():
    """Test all active orders are included as well as all waste orders"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)
    state_cold = _order_cold(test_kitchen)

    assert test_kitchen.count_shelves() == {"hot": 1, "cold": 1}

    # now move cold order to waste
    state_cold.move_to_waste()
    assert test_kitchen.count_shelves() == {"hot": 1, "waste": 1}

    # now close hot order
    state_hot.close()
    assert test_kitchen.count_shelves() == {"waste": 1}


def test_remove_unhealty():
    """Should remove orders with value <=0"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)
    _order_cold(test_kitchen)

    assert test_kitchen.count_shelves() == {"hot": 1, "cold": 1}

    # mock one order to return zero value
    state_hot.value = Mock(return_value=-1.0)
    test_kitchen.terminate_delivery = Mock()

    active, expired = test_kitchen.remove_unhealty()

    assert active == 2
    assert expired == 1

    test_kitchen.terminate_delivery.assert_called_with(25)
    assert test_kitchen.count_shelves() == {WASTE: 1, "cold": 1}


def test_find_recoverable_orders():
    """Find orders in overflow shelf that could be moved"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)
    state_cold = _order_cold(test_kitchen)

    # moving both to overflow to make them recoverable
    state_hot.move(ShelfHistory(OVERFLOW))
    state_cold.move(ShelfHistory(OVERFLOW))

    # add more orders so the active counters will be > 0
    _order_hot(test_kitchen, order_num=55)
    _order_cold(test_kitchen, order_num=66)

    # simulate capacity
    test_kitchen.config.capacity["hot"] = 5
    test_kitchen.config.capacity["cold"] = 4

    assert math.isclose(test_kitchen.find_recoverable_orders()[25], 1/5)
    assert math.isclose(test_kitchen.find_recoverable_orders()[33], 1/4)


def test_recover_from_overflow():
    """Check if order with lowest utilization is recovered"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)
    state_cold = _order_cold(test_kitchen)

    # moving both to overflow to make them recoverable
    state_hot.move(ShelfHistory(OVERFLOW))
    state_cold.move(ShelfHistory(OVERFLOW))

    assert state_hot.current_shelf() == OVERFLOW
    assert state_cold.current_shelf() == OVERFLOW

    # Mock the return
    test_kitchen.find_recoverable_orders = Mock(
        return_value={25: 1/5, 33: 1/4}
    )

    recovered = test_kitchen.recover_from_overflow()

    assert recovered == 1
    assert state_hot.current_shelf() == "hot"
    assert state_cold.current_shelf() == OVERFLOW


def test_cleanup():
    """Should remove unhealthy and recover from overflow"""

    test_kitchen = Kitchen(orders, config)

    # mocking delay value
    test_kitchen.config.cleanup_delay = 55

    # mock two main methods
    test_kitchen.remove_unhealty = Mock(return_value=(2, 1))
    test_kitchen.recover_from_overflow = Mock(return_value=1)

    # check the delay
    test_kitchen.cleanup_event = Mock()
    test_kitchen.cleanup_event.wait = Mock()

    # Execute main method
    test_kitchen.cleanup()

    test_kitchen.remove_unhealty.assert_called()
    test_kitchen.recover_from_overflow.assert_called()
    test_kitchen.cleanup_event.wait.assert_called_with(55)


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


def test_make_room_ok():
    """Simple cases: all shelves have room"""

    test_kitchen = Kitchen(orders, config)

    for shelf in shelf_types:
        assert test_kitchen.make_room(shelf) == shelf


def test_make_room_overflow():
    """Simulate full capacity for each normal shelf to be redirected to overflow shelf"""

    test_kitchen = Kitchen(orders, config)

    for shelf in shelf_types:
        # simulate reaching capacity
        test_kitchen.config.capacity[shelf] = 0

        assert test_kitchen.make_room(shelf) == OVERFLOW


def test_make_room_recovery():
    """Test recovery from overflow shelf back to desired shelf"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)
    state_cold = _order_cold(test_kitchen)

    # moving both to overflow to make them recoverable
    state_hot.move(ShelfHistory(OVERFLOW))
    state_cold.move(ShelfHistory(OVERFLOW))

    assert state_hot.current_shelf() == OVERFLOW
    assert state_cold.current_shelf() == OVERFLOW

    # Mock the return
    test_kitchen.find_recoverable_orders = Mock(
        return_value={25: 1/5, 33: 1/4}
    )

    # Mock the full capacity
    test_kitchen.config.capacity["frozen"] = 0
    test_kitchen.config.capacity[OVERFLOW] = 2

    # Run the main test command
    avail_shelf = test_kitchen.make_room("frozen")

    assert avail_shelf == OVERFLOW
    assert state_hot.current_shelf() == "hot"


def test_make_room_removal():
    """Test removal from overflow shelf"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen, life=200)
    state_cold = _order_cold(test_kitchen, life=100)

    # moving both to overflow to make them recoverable
    state_hot.move(ShelfHistory(OVERFLOW))
    state_cold.move(ShelfHistory(OVERFLOW))

    # Mock the full capacity
    test_kitchen.config.capacity["frozen"] = 0
    test_kitchen.config.capacity["hot"] = 0
    test_kitchen.config.capacity["cold"] = 0
    test_kitchen.config.capacity[OVERFLOW] = 2

    # disable terminate_delivery method
    test_kitchen.terminate_delivery = Mock()

    # Run the main test command
    avail_shelf = test_kitchen.make_room("frozen")

    assert avail_shelf == OVERFLOW

    # should remove order with smallest TTL (shortest life -> cold)
    assert state_cold.current_shelf() == WASTE
    assert state_cold.closed() == True


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

    # making limits explicit and very close
    test_kitchen.config.pickup_max_sec = 101
    test_kitchen.config.pickup_max_sec = 102

    test_kitchen.make_room = Mock(return_value=OVERFLOW)
    test_kitchen.fulfill_order(25, order25)

    assert delay <= test_kitchen.config.pickup_max_sec
    assert delay >= test_kitchen.config.pickup_min_sec

    # not empty
    assert not test_kitchen.dispatch_queue.empty()
    assert 25 in test_kitchen.orders_state
    assert test_kitchen.orders_state[25].order == order25
    assert test_kitchen.orders_state[25].current_shelf() == OVERFLOW
    assert test_kitchen.orders_state[25].closed() == False
    assert test_kitchen.orders_state[25].pickup_sec == delay


def test_dispatch_order_ok():
    """Pickup normal order"""

    test_kitchen = Kitchen(orders, config)
    state_hot = _order_hot(test_kitchen)

    assert test_kitchen.count_shelves()["hot"] == 1  # before

    # Making a fake Event
    test_kitchen.dispatch_events[25] = Mock()
    test_kitchen.dispatch_events[25].wait = Mock()

    test_kitchen.dispatch_order(25, 999)

    test_kitchen.dispatch_events[25].wait.assert_called_with(999)

    assert state_hot.history[-1].added_at != None
    assert state_hot.history[-1].removed_at != None
    assert state_hot.history[-1].shelf == "hot"

    assert test_kitchen.count_shelves()["hot"] == 0  # after


def test_dispatch_order_wasted():
    """Make sure we do not pickup wasted order"""

    test_kitchen = Kitchen(orders, config)

    state_hot = _order_hot(test_kitchen)
    state_hot.move_to_waste()

    # Making a fake Event
    test_kitchen.dispatch_events[25] = Mock()
    test_kitchen.dispatch_events[25].wait = Mock()

    test_kitchen.dispatch_order(25, 999)

    test_kitchen.dispatch_events[25].wait.assert_called_with(999)

    assert state_hot.history[-1].added_at != None
    assert state_hot.history[-1].removed_at != None
    assert state_hot.history[-1].shelf == WASTE
