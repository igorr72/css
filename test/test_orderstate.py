import pytest
import pathlib
import time
import math

from unittest.mock import Mock, patch

from orders_simulation.kitchendata import Order, OVERFLOW
from orders_simulation.orderstate import OrderState, ShelfHistory

cur_dir = pathlib.Path(__file__).parent


def test_decay_modifiers():
    order = Order(id="xxx", name="taco", temp="hot",
                  shelfLife=3, decayRate=5.0)

    state = OrderState(order, init_state=ShelfHistory(shelf="hot"))
    state.history.append(ShelfHistory(shelf=OVERFLOW))

    assert state.decay_modifiers() == [1, 2]


def test_decay_rates():
    order = Order(id="xxx", name="taco", temp="hot",
                  shelfLife=3, decayRate=0.5)

    state = OrderState(order, init_state=ShelfHistory(shelf="hot"))
    state.history.append(ShelfHistory(shelf=OVERFLOW))

    assert state.decay_rates() == [0.5, 1.0]


def test_ages_fixed():
    order = Order(id="xxx", name="taco", temp="hot",
                  shelfLife=3, decayRate=0.5)

    state = OrderState(order, init_state=ShelfHistory(
        shelf="hot", added_at=1.1, removed_at=3.1))
    state.history.append(ShelfHistory(
        shelf="overflow", added_at=3.1, removed_at=4.1))

    ages = state.ages()

    assert math.isclose(ages[0], 2.0)
    assert math.isclose(ages[1], 1.0)


def test_ages_not_removed_yet():
    order = Order(id="xxx", name="taco", temp="hot",
                  shelfLife=3, decayRate=0.5)

    state = OrderState(order, init_state=ShelfHistory(
        shelf="hot", added_at=1.1))

    with patch("time.time", Mock(return_value=2.1)):
        ages = state.ages()
        assert math.isclose(ages[0], 1.0)


def test_value():
    order = Order(id="xxx", name="taco", temp="hot",
                  shelfLife=3, decayRate=0.5)

    state = OrderState(order, init_state=ShelfHistory(
        shelf="hot", added_at=1.1, removed_at=3.1))
    state.history.append(ShelfHistory(
        shelf="overflow", added_at=3.1, removed_at=4.1))

    # value = 1 - (2*0.5 + 1*1) / 3 => 1 - 2/3
    assert math.isclose(state.value(), 1.0/3)


def test_ttl():
    order = Order(id="xxx", name="taco", temp="hot",
                  shelfLife=3, decayRate=0.5)

    state = OrderState(order, init_state=ShelfHistory(
        shelf="hot", added_at=1.1, removed_at=4.1))
    # removed_at is calculated as ttl
    state.history.append(ShelfHistory(shelf="overflow", added_at=4.1))

    # ttl = (shelfLife - sum(prior_decays)) / last_decay_rate
    # prior_decays = a1 * d1 = 3age * 0.5rate * 1modif = 1.5
    # last_decay_rate = 0.5 * 2modif = 1
    # ttl = (3 - 1.5) / 1

    assert math.isclose(state.ttl(), 1.5)


def test_pickup_ttl():
    order = Order(id="xxx", name="taco", temp="hot",
                  shelfLife=3, decayRate=0.5)

    state = OrderState(order, init_state=ShelfHistory(
        shelf="hot", added_at=1.1, removed_at=4.1), pickup_sec=5)
    # removed_at is calculated as ttl
    state.history.append(ShelfHistory(shelf="overflow", added_at=4.1))

    # ttl = (shelfLife - sum(prior_decays)) / last_decay_rate
    # prior_decays = a1 * d1 = 3age * 0.5rate * 1modif = 1.5
    # last_decay_rate = 0.5 * 2modif = 1
    # ttl = (3 - 1.5) / 1

    with patch("time.time", Mock(return_value=5.1)):  # 1 sec spent on overflow shelf
        # pickup_ttl = ttl() - time_to_pickup
        # ttl = 1.5
        # time_to_pickup = pickup_sec - total_age = 5 - (3+1) = 1
        # pickup_ttl = 1.5 - 1

        assert math.isclose(state.pickup_ttl(), 0.5)
