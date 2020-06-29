import pytest
import pathlib
import time
import math

from unittest.mock import Mock, patch

from orders_simulation.kitchendata import Order
from orders_simulation.orderstate import OrderState

cur_dir = pathlib.Path(__file__).parent


def test_decay_modifier():
    order = Order(id="xxx", name="taco", temp="hot", shelfLife=1, decayRate=1)
    state1 = OrderState(order, order_recieved=0, shelf="hot")
    state2 = OrderState(order, order_recieved=0, shelf="overflow")

    assert state1.decay_modifier() == 1
    assert state2.decay_modifier() == 2


def test_decay_rate():
    order = Order(id="xxx", name="taco", temp="hot", shelfLife=1, decayRate=2)
    state1 = OrderState(order, order_recieved=0, shelf="hot")
    state2 = OrderState(order, order_recieved=0, shelf="overflow")

    assert state1.decay_rate() == 2
    assert state2.decay_rate() == 4


def test_age():
    now = time.time()
    age = 3
    state = OrderState(object, order_recieved=now, shelf="hot")
    with patch("time.time", Mock(return_value = now + age)):
        assert math.isclose(state.age(), age)


def test_value_base():
    """Base case when all variables are equal to 'one'."""

    now = time.time()
    life = 1
    age = 1

    order = Order(id="xxx", name="taco", temp="hot", shelfLife=life, decayRate=1.0)
    state1 = OrderState(order, order_recieved=now, shelf="hot")
    state2 = OrderState(order, order_recieved=now, shelf="overflow")

    with patch("time.time", Mock(return_value = now + age)):
        assert math.isclose(state1.value(), 0.0)
        assert math.isclose(state2.value(), -1.0)

    with patch("time.time", Mock(return_value = now)):
        assert math.isclose(state1.value(), 1.0)
        # age is the same (zero), so decay_modifier does not matter
        assert math.isclose(state2.value(), 1.0)


def test_value_mutate_decay():
    """With decay less than one, value should be (1-decay)"""

    now = time.time()
    life = 1
    age = 1

    order = Order(id="xxx", name="taco", temp="hot", shelfLife=life, decayRate=0.4)
    state1 = OrderState(order, order_recieved=now, shelf="hot")
    state2 = OrderState(order, order_recieved=now, shelf="overflow")

    with patch("time.time", Mock(return_value = now + age)):
        assert math.isclose(state1.value(), 0.6) # 1 - 0.4 * 1 * 1
        assert math.isclose(state2.value(), 0.2) # 1 - 0.4 * 1 * 2


def test_value_mutate_age():
    """With decay=0.5 allows for shelf life to double the age on proper shelf"""

    now = time.time()
    life = 1
    age = 2

    order = Order(id="xxx", name="taco", temp="hot", shelfLife=life, decayRate=0.5)
    state1 = OrderState(order, order_recieved=now, shelf="hot")
    state2 = OrderState(order, order_recieved=now, shelf="overflow")

    with patch("time.time", Mock(return_value = now + age)):
        assert math.isclose(state1.value(), 0)
        assert math.isclose(state2.value(), -1) # 1 - 2 * 0.5 * 2
