import pytest
import pathlib

from unittest.mock import Mock

import orders_simulation.kitchendata as kitchendata

cur_dir = pathlib.Path(__file__).parent

# ========== load_json ==========
def test_load_json_garbage():
    output = Mock()
    orders_path = cur_dir.joinpath("fixture", "garbage.json")
    data = kitchendata.load_json(orders_path, output)

    assert data == None
    output.write.assert_called()
    assert "JSONDecodeError" in str(output.mock_calls[0])

def test_load_json_nofile():
    output = Mock()
    orders_path = cur_dir.joinpath("fixture", "non-existent")
    data = kitchendata.load_json(orders_path, output)

    assert data == None
    output.write.assert_called()
    assert "file does not exist" in str(output.mock_calls[0])

def test_load_json_orders():
    output = Mock()
    orders_path = cur_dir.joinpath("fixture", "orders.json")
    data = kitchendata.load_json(orders_path, output)

    assert len(data) == 5
    output.write.assert_not_called()

# ========== load_orders ==========
def test_load_orders_ok():
    output = Mock()
    orders_path = cur_dir.joinpath("fixture", "orders.json")
    data = kitchendata.load_orders(orders_path, output)

    assert len(data) == 5
    output.write.assert_not_called()

def test_load_orders_not_list():
    output = Mock()
    orders_path = cur_dir.joinpath("fixture", "wrong_dict.json")
    data = kitchendata.load_orders(orders_path, output)

    assert data == None
    output.write.assert_called()
    assert "Expected a list of orders" in str(output.mock_calls[0])

def test_load_orders_wrong_list():
    output = Mock()
    orders_path = cur_dir.joinpath("fixture", "wrong_list.json")
    data = kitchendata.load_orders(orders_path, output)

    assert data == None
    output.write.assert_called()
    assert "Can't convert json data into" in str(output.mock_calls[0])

# ========== load_config ==========
def test_load_config_ok():
    output = Mock()
    config_path = cur_dir.joinpath("fixture", "config.json")
    data = kitchendata.load_config(config_path, output)

    assert isinstance(data, kitchendata.Config)
    output.write.assert_not_called()

def test_load_config_wrong_config():
    output = Mock()
    config_path = cur_dir.joinpath("fixture", "wrong_dict.json")
    data = kitchendata.load_config(config_path, output)

    assert data == None
    output.write.assert_called()
    assert "Can't convert json data into" in str(output.mock_calls[0])
