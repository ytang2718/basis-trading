"""
This is the abstract class for quoting modules.

Yuanming Tang, 2023
"""

from abc import ABC, abstractmethod


class AbstractQuotingModule(ABC):
    @abstractmethod
    def startup(self):
        pass

    @abstractmethod
    def shutdown(self):
        pass

    @abstractmethod
    def on_order_book_tick(self, *args, **kwargs):
        pass
