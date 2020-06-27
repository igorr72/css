
from logging import Logger

from typing import List, Dict

class Kitchen:
    def __init__(self, orders: List[Dict], config: Dict, logger: Logger) -> None:
        self.logger = logger
        self.orders = orders
        self.config = config

    def run(self) -> None:
        self.logger.debug(f"Orders: {self.orders}")
        self.logger.debug(f"Config: {self.config}")

        self.logger.info(f"Number of Orders: {len(self.orders)}")
        self.logger.info(f"Config size: {len(self.config)}")


