#!/usr/bin/env python3

import argparse
import json
import sys
import logging

from pathlib import Path

from typing import Union, List, Dict, Optional

from kitchen import Kitchen


ListOrDict = Union[List, Dict]


def parse_cli_args():
    parser = argparse.ArgumentParser(description="Simulate kitchen orders")

    parser.add_argument("-d", "--debug", help=f"Debug level (default: 0), 1-verbose, 2-debug", type=int, default=0)

    parser.add_argument("-o", "--orders", help=f"Specify input file with orders (json)", required=True)
    parser.add_argument("-c", "--config", help=f"Specify custom config file (json)", required=True)

    return parser.parse_args()


def load_json(filepath: str, expected_type: ListOrDict) -> Optional[ListOrDict]:
    if not Path(filepath).is_file():
        print(f"INPUT ERROR: required file does not exist: '{filepath}'", file=sys.stderr)
        return None

    try:
        with open(filepath, "r") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"INPUT ERROR: could not load json file '{filepath}'; {e}", file=sys.stderr)
        return None

    if not isinstance(data, expected_type):
        print(f"DATA ERROR: json file '{filepath}' contains {data.__class__.__name__}, expected {expected_type}", file=sys.stderr)
        return None

    return data


def new_logger(debug: bool) -> logging.Logger:
    logger = logging.getLogger(__name__)

    if debug == 1:
        level = logging.INFO
    elif debug == 2:
        level = logging.DEBUG
    else:
        level = logging.WARNING

    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(filename)s/%(funcName)s: %(message)s',
        level = level)

    return logger


def main():
    args = parse_cli_args()
    # print(f"CLI args: {args}")

    orders = load_json(args.orders, list)
    config = load_json(args.config, dict)

    if orders == None or config == None:
        sys.exit(1) # exit code passed to shell

    Kitchen(orders, config, new_logger(args.debug)).run()

if __name__ == "__main__":
    main()
