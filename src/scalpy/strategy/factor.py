from collections import deque
from datetime import datetime
from decimal import Decimal

import structlog

from scalpy.core.enums import Side
from scalpy.core.models import Signal
from scalpy.strategy.base import BaseStrategy

logger = structlog.get_logger()


class FactorStrategy(BaseStrategy):
    name = "factor"
    display_name = "팩터 기반"
    description = "Multi-factor scoring: price momentum + volume strength + orderbook imbalance"

    def __init__(self) -> None:
        self._init_base()
        self.lookback: int = 60
        self.buy_threshold: float = 0.65
        self.sell_threshold: float = 0.70
        self.weight_momentum: float = 0.4
        self.weight_volume: float = 0.3
        self.weight_orderbook: float = 0.3
        self.cooldown_seconds: int = 1800
        self.stop_loss_ratio: float | None = 0.025
        self.min_tick_volume: int = 5
        self._prices: dict[str, deque[Decimal]] = {}
        self._volumes: dict[str, deque[int]] = {}
        self._ob_imbalance: dict[str, float] = {}

    def reset(self) -> None:
        super().reset()
        self._prices.clear()
        self._volumes.clear()
        self._ob_imbalance.clear()

    def _get_prices(self, symbol: str) -> deque[Decimal]:
        if symbol not in self._prices:
            self._prices[symbol] = deque(maxlen=self.lookback)
        return self._prices[symbol]

    def _get_volumes(self, symbol: str) -> deque[int]:
        if symbol not in self._volumes:
            self._volumes[symbol] = deque(maxlen=self.lookback)
        return self._volumes[symbol]

    def _momentum_score(self, prices: deque[Decimal]) -> float:
        if len(prices) < 2:
            return 0.5
        start = float(prices[0])
        end = float(prices[-1])
        if start == 0:
            return 0.5
        ret = (end - start) / start
        return max(0.0, min(1.0, 0.5 + ret * 50))

    def _is_near_peak(self, prices: deque[Decimal], window: int = 10) -> bool:
        if len(prices) < window:
            return False
        recent = list(prices)[-window:]
        peak = max(float(p) for p in recent)
        current = float(recent[-1])
        if peak == 0:
            return False
        return (peak - current) / peak < 0.001

    def _is_short_term_declining(self, prices: deque[Decimal], window: int = 5) -> bool:
        if len(prices) < window + 1:
            return False
        recent = list(prices)[-window:]
        base = float(recent[0])
        if base == 0:
            return False
        ret = (float(recent[-1]) - base) / base
        return ret < -0.001

    def _volume_score(self, volumes: deque[int], current: int) -> float:
        if len(volumes) < 2:
            return 0.5
        avg = sum(volumes) / len(volumes)
        if avg == 0:
            return 0.5
        ratio = current / avg
        return max(0.0, min(1.0, ratio / 4))

    def _orderbook_score(self, symbol: str) -> float:
        return self._ob_imbalance.get(symbol, 0.5)

    def _is_below_sma(self, prices: deque[Decimal]) -> bool:
        if len(prices) < self.lookback:
            return False
        sma = sum(float(p) for p in prices) / len(prices)
        return float(prices[-1]) < sma

    async def on_tick(self, symbol: str, price: Decimal, volume: int) -> Signal | None:
        if volume < self.min_tick_volume:
            return None
        self._advance_tick(symbol)
        prices = self._get_prices(symbol)
        volumes = self._get_volumes(symbol)
        prices.append(price)
        volumes.append(volume)

        if len(prices) < self.lookback:
            return None

        m = self._momentum_score(prices)
        v = self._volume_score(volumes, volume)
        o = self._orderbook_score(symbol)

        buy_score = (
            m * self.weight_momentum
            + v * self.weight_volume
            + o * self.weight_orderbook
        )
        sell_score = (
            (1 - m) * self.weight_momentum
            + (1 - v) * self.weight_volume
            + (1 - o) * self.weight_orderbook
        )

        if buy_score >= self.buy_threshold and m >= 0.5:
            if not self._check_cooldown(symbol, "BUY"):
                logger.debug("factor.skip", symbol=symbol, reason="cooldown", side="BUY", score=round(buy_score, 3))
                return None
            if self._is_short_term_declining(prices):
                logger.debug("factor.skip", symbol=symbol, reason="declining", score=round(buy_score, 3))
                return None
            if self._is_near_peak(prices):
                logger.debug("factor.skip", symbol=symbol, reason="near_peak", score=round(buy_score, 3))
                return None
            if self._is_below_sma(prices):
                logger.debug("factor.skip", symbol=symbol, reason="below_sma", score=round(buy_score, 3))
                return None
            confidence = min(0.9, buy_score)
            return Signal(symbol, Side.BUY, self.name, price, 0, confidence, datetime.now())

        if sell_score >= self.sell_threshold and m <= 0.5:
            if not self._check_cooldown(symbol, "SELL"):
                logger.debug("factor.skip", symbol=symbol, reason="cooldown", side="SELL", score=round(sell_score, 3))
                return None
            confidence = min(0.9, sell_score)
            return Signal(symbol, Side.SELL, self.name, price, 0, confidence, datetime.now())

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
        if not asks or not bids:
            return None
        total_ask = sum(qty for _, qty in asks)
        total_bid = sum(qty for _, qty in bids)
        total = total_ask + total_bid
        if total == 0:
            return None
        self._ob_imbalance[symbol] = total_bid / total
        return None
