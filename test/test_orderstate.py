import pytest
import pathlib
import time
import math

from unittest.mock import Mock, patch

from orders_simulation.kitchendata import Order, OVERFLOW
from orders_simulation.orderstate import OrderState, ShelfHistory

cur_dir = pathlib.Path(__file__).parent


def _order_hot(life: int = 1, decay: float = 1.0, pickup_sec: int = 1):
    """Helper: generate test order #25 added to hot shelf"""

    order = Order(id="xxx", name="taco", temp="hot",
                  shelfLife=life, decayRate=decay)
    state = OrderState(order, ShelfHistory("hot"), pickup_sec=pickup_sec)

    return state


def test_decay_modifiers():
    """Decay modifier should be different for OVERFLOW and normal shelves"""

    state = _order_hot()
    state.move(ShelfHistory(OVERFLOW))

    assert state.decay_modifiers() == [1, 2]


def test_decay_rates():
    """decay rate is proportional to decay modifier"""

    state = _order_hot(decay=0.6)
    state.move(ShelfHistory(OVERFLOW))

    assert state.decay_rates() == [0.6, 1.2]


def test_ages_fixed():
    """For closed orders it uses existing time stamps"""

    state = _order_hot()
    state.move(ShelfHistory(OVERFLOW))

    state.history[0].added_at = 1.1
    state.history[0].removed_at = 3.1

    state.history[1].added_at = 3.1
    state.history[1].removed_at = 4.1

    ages = state.ages()

    assert math.isclose(ages[0], 2.0)
    assert math.isclose(ages[1], 1.0)


def test_ages_not_removed_yet():
    """For active orders it calls time.time() to estimate removed_at"""

    state = _order_hot()
    state.history[0].added_at = 1.1  # fixing time stamp

    with patch("time.time", Mock(return_value=2.1)):
        ages = state.ages()
        assert math.isclose(ages[0], 1.0)


def test_value():
    """Calculate value for two stages"""

    state = _order_hot(life=3, decay=0.5)
    state.move(ShelfHistory(OVERFLOW))

    state.history[0].added_at = 1.1
    state.history[0].removed_at = 3.1

    state.history[1].added_at = 3.1
    state.history[1].removed_at = 4.1

    # value = 1 - (2s*0.5*1 + 1s*0.5*2) / 3 => 1 - (1+1)/3
    assert math.isclose(state.value(), 1.0/3)


def test_ttl():
    """Calculate TTL for two stages"""

    state = _order_hot(life=4, decay=0.5)
    state.move(ShelfHistory(OVERFLOW))

    state.history[0].added_at = 1.1
    state.history[0].removed_at = 4.1

    state.history[1].added_at = 4.1

    # ttl = (shelfLife - sum(prior_decays)) / last_decay_rate
    # prior_decays = a1 * d1 = 3s * 0.5rate * 1modif = 1.5
    # last_decay_rate = 0.5 * 2modif = 1
    # ttl = (4 - 1.5) / 1

    assert math.isclose(state.ttl(), 2.5)


def test_pickup_ttl():
    """Calculate pickup TTL based on delivery time"""

    state = _order_hot(life=4, decay=0.5, pickup_sec=6)
    state.move(ShelfHistory(OVERFLOW))

    state.history[0].added_at = 1.1
    state.history[0].removed_at = 4.1

    state.history[1].added_at = 4.1
    # we know from previous test that TTL for such order will be 2.5

    with patch("time.time", Mock(return_value=5.1)):  # 1 sec spent on overflow shelf
        # pickup_ttl = ttl() - time_to_pickup
        # ttl = 2.5 from previous test
        # time_to_pickup = pickup_sec - total_age = 6 - (3+1) = 2
        # pickup_ttl = 2.5 - 2

        assert math.isclose(state.pickup_ttl(), 0.5)
