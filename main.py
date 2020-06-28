#!/usr/bin/env python3

import argparse
import sys

from orders_simulation.kitchendata import load_orders, load_config
from orders_simulation.kitchen import Kitchen


def parse_cli_args():
    parser = argparse.ArgumentParser(description="Simulate kitchen orders")

    parser.add_argument("-d", "--debug_level", help=f"Debug level (default: 0), 1-verbose, 2-debug", type=int, default=0)

    parser.add_argument("-o", "--orders", help=f"Specify input file with orders (json)", required=True)
    parser.add_argument("-c", "--config", help=f"Specify custom config file (json)", required=True)

    return parser.parse_args()


def main():
    args = parse_cli_args()

    orders = load_orders(args.orders, errors_sink = sys.stderr)
    config = load_config(args.config, errors_sink = sys.stderr)

    if orders == None or config == None:
        sys.exit(1) # exit code passed to shell

    Kitchen(orders, config).run(args.debug_level)


if __name__ == "__main__":
    main()
