

import abc
import asyncio
from collections import deque
import time

from typing import Any, Deque, Dict, List, Tuple, Union, Optional


class VWAP(abc.ABC):
    
    def __init__(self, period, get_time_nanos) -> None:
        self.period_ns = period * 1_000_000_000
        self.get_time_nanos = get_time_nanos
        self._total_pv = 0.0
        self._total_volume = 0.0

    @abc.abstractmethod
    def add_trade(self, trade: Dict[str, Any]) -> None:
        pass
    
    def add_trades(self, trades: List[Dict[str, Any]]) -> None:
        for trade in trades:
            self.add_trade(trade)

    @abc.abstractmethod
    def get_vwap(self) -> Optional[float]:
        pass
        
    @abc.abstractmethod
    async def run(self) -> None:
        pass


class SimpleVWAP(VWAP):
    
    def __init__(self, period=1800, get_time_nanos=time.time_ns) -> None:
        super().__init__(period, get_time_nanos)
        self._trades: Deque[Dict[str, Any]] = deque()

    def add_trade(self, trade: Dict[str, Any]) -> None:
        self._trades.append(trade)
        self._total_pv += trade["price"] * trade["amount"]
        self._total_volume += trade["amount"]
        self._prune_old_trades()

    def get_vwap(self) -> Optional[float]:
        if self._total_volume == 0:
            return None
        self._prune_old_trades()
        return self._total_pv / self._total_volume

    def _prune_old_trades(self) -> None:
        cutoff_time = self.get_time_nanos() - self.period_ns
        while self._trades and (self._trades[0]["timestamp"] * 1_000_000) < cutoff_time:
            old_trade = self._trades.popleft()
            self._total_pv -= old_trade["price"] * old_trade["amount"]
            self._total_volume -= old_trade["amount"]

    async def run(self) -> None:
        pass


class BucketedVWAP(VWAP):
    
    def __init__(self, period=1800, time_resolution_ms=1, get_time_nanos=time.time_ns) -> None:
        super().__init__(period, get_time_nanos)
        self.time_resolution_ns = time_resolution_ms * 1_000_000
        self.buckets: Deque[Tuple[int, float, float]] = deque()
        self._current_bucket = None
        self.start_time_ns = None

    def add_trade(self, trade: Dict[str, Any]) -> None:
        if self._current_bucket is None:
            return
        self._current_bucket[1] += trade["price"] * trade["amount"]
        self._current_bucket[2] += trade["amount"]
        self._total_pv += trade["price"] * trade["amount"]
        self._total_volume += trade["amount"]

    def get_vwap(self) -> Optional[float]:
        if self._total_volume == 0:
            return None
        return self._total_pv / self._total_volume

    async def run(self):
        self.start_time_ns = self.get_time_nanos()
        self._current_bucket = [self.get_time_nanos(), 0.0, 0.0]

        # Handle the initial period
        while True:
            await asyncio.sleep(self.time_resolution_ns / 1_000_000_000)
            current_time = self.get_time_nanos()
            if current_time - self.start_time_ns >= self.period_ns:
                break
            self._current_bucket = [current_time, 0.0, 0.0]

        # Continue with bucket management
        while True:
            await asyncio.sleep(self.time_resolution_ns / 1_000_000_000)
            current_time = self.get_time_nanos()
            self.buckets.append(self._current_bucket)
            self._current_bucket = [current_time, 0.0, 0.0]
            while self.buckets and self.buckets[0][0] < current_time - self.period_ns:
                old_bucket = self.buckets.popleft()
                self._total_pv -= old_bucket[1]
                self._total_volume -= old_bucket[2]