import pytest
import pathlib
import argparse
import logging

from unittest.mock import patch, Mock
from collections import Counter

from main import main

cur_dir = pathlib.Path(__file__).parent


def test_run_main_failed(capsys):
    orders_path = cur_dir.joinpath("fixture", "NON-EXISTENT")
    config_path = cur_dir.joinpath("fixture", "NON-EXISTENT")

    args = Mock(return_value=argparse.Namespace(
        orders=orders_path,
        config=config_path,
        debug_level=0))

    with patch("main.parse_cli_args", args):
        with pytest.raises(SystemExit, match="1"):
            main()
            captured = capsys.readouterr()
            assert captured.out == ""
            assert captured.err != ""


def test_run_main_debug(caplog):
    orders_path = cur_dir.joinpath("fixture", "orders.json")
    config_path = cur_dir.joinpath("fixture", "config.json")

    args = Mock(return_value=argparse.Namespace(
        orders=orders_path,
        config=config_path,
        debug_level=2))

    with patch("main.parse_cli_args", args):
        try:
            caplog.set_level(logging.DEBUG)
            main()
            c = Counter([r.levelname for r in caplog.records])
            assert c["WARNING"] >= 3  # start, finish, stats messages
            assert c["INFO"] > 0
            assert c["DEBUG"] > 0
            assert c["ERROR"] > 0

        except SystemExit as e:
            assert e == None  # will fail if exception was raised by sys.exit()
