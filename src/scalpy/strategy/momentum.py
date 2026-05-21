from collections import deque
from datetime import datetime
from decimal import Decimal

import structlog

from scalpy.core.enums import Side
from scalpy.core.models import Signal
from scalpy.strategy.base import BaseStrategy

logger = structlog.get_logger()


class MomentumStrategy(BaseStrategy):
    name = "momentum"
    display_name = "모멘텀 돌파"
    description = "Volume surge + price breakout above recent high"

    def __init__(self) -> None:
        self._init_base()
        self.lookback: int = 30
        self.volume_multiplier: float = 2.0
        self.breakout_pct: float = 0.005
        self.cooldown_seconds: int = 1800
        self.stop_loss_ratio: float | None = 0.03
        self._prices: dict[str, deque[Decimal]] = {}
        self._volumes: dict[str, deque[int]] = {}

    def reset(self) -> None:
        super().reset()
        self._prices.clear()
        self._volumes.clear()

    def _get_prices(self, symbol: str) -> deque[Decimal]:
        if symbol not in self._prices:
            self._prices[symbol] = deque(maxlen=self.lookback)
        return self._prices[symbol]

    def _get_volumes(self, symbol: str) -> deque[int]:
        if symbol not in self._volumes:
            self._volumes[symbol] = deque(maxlen=self.lookback)
        return self._volumes[symbol]

    async def on_tick(self, symbol: str, price: Decimal, volume: int) -> Signal | None:
        self._advance_tick(symbol)
        prices = self._get_prices(symbol)
        volumes = self._get_volumes(symbol)
        prices.append(price)
        volumes.append(volume)

        if len(prices) < self.lookback:
            return None

        avg_vol = sum(volumes) / len(volumes)
        if avg_vol <= 0 or volume < avg_vol * self.volume_multiplier:
            return None

        recent_high = max(list(prices)[:-1])
        threshold = recent_high * (1 + Decimal(str(self.breakout_pct)))

        if price >= threshold:
            if not self._check_cooldown(symbol, "BUY"):
                logger.debug("momentum.skip", symbol=symbol, reason="cooldown", side="BUY")
                return None
            confidence = min(0.9, 0.6 + (volume / avg_vol - self.volume_multiplier) * 0.1)
            return Signal(symbol, Side.BUY, self.name, price, 0, confidence, datetime.now())

        return None

    def prefill(self, symbol: str, candles: list[dict]) -> None:
        prices = self._get_prices(symbol)
        volumes = self._get_volumes(symbol)
        for c in candles[-self.lookback :]:
            prices.append(Decimal(str(c["close"])))
            volumes.append(c["volume"])

    async def on_orderbook(
        self,
        symbol: str,
        asks: list[tuple[Decimal, int]],
        bids: list[tuple[Decimal, int]],
    ) -> Signal | None:
        return None
