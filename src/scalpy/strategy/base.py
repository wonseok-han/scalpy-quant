import time
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any

from scalpy.core.models import Signal

_DEFAULT_COOLDOWN = 30


class BaseStrategy(ABC):
    name: str
    display_name: str
    description: str

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

    def _init_base(self) -> None:
        self.enabled: bool = True
        self.cooldown_seconds: int = _DEFAULT_COOLDOWN
        self._last_signal_at: dict[str, float] = {}
        self._tick_count: dict[str, int] = {}
        self._last_signal_tick: dict[str, int] = {}
        self._backtest_mode: bool = False
        self.cooldown_ticks: int = 1
        self.stop_loss_ratio: float | None = None

    def reset(self) -> None:
        self._last_signal_at.clear()
        self._tick_count.clear()
        self._last_signal_tick.clear()

    def _advance_tick(self, symbol: str) -> None:
        self._tick_count[symbol] = self._tick_count.get(symbol, 0) + 1

    def _check_cooldown(self, symbol: str, side: str) -> bool:
        key = f"{symbol}:{side}"
        if self._backtest_mode:
            tick = self._tick_count.get(symbol, 0)
            last = self._last_signal_tick.get(key, -999)
            if tick - last < self.cooldown_ticks:
                return False
            self._last_signal_tick[key] = tick
            return True
        now = time.monotonic()
        last = self._last_signal_at.get(key, 0)
        if now - last < self.cooldown_seconds:
            return False
        self._last_signal_at[key] = now
        return True

    @abstractmethod
    async def on_tick(self, symbol: str, price: Decimal, volume: int) -> Signal | None:
        """Called on each trade tick. Return a Signal or None."""

    @abstractmethod
    async def on_orderbook(
        self,
        symbol: str,
        asks: list[tuple[Decimal, int]],
        bids: list[tuple[Decimal, int]],
    ) -> Signal | None:
        """Called on orderbook update. Return a Signal or None."""

    def prefill(self, symbol: str, candles: list[dict]) -> None:
        """Override in subclasses to pre-fill buffers with historical OHLCV data."""

    def configure(self, params: dict[str, Any]) -> None:
        for key, value in params.items():
            if hasattr(self, key):
                setattr(self, key, value)
