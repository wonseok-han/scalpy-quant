import math
from collections import deque
from datetime import datetime
from decimal import Decimal

import structlog

from scalpy.core.enums import Side
from scalpy.core.models import Signal
from scalpy.strategy.base import BaseStrategy

logger = structlog.get_logger()


class _Candle:
    __slots__ = ("high", "low", "close", "volume")

    def __init__(self, price: Decimal, volume: int = 0) -> None:
        self.high = price
        self.low = price
        self.close = price
        self.volume = volume

    def update(self, price: Decimal, volume: int = 0) -> None:
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        self.close = price
        self.volume += volume


class MeanReversionStrategy(BaseStrategy):
    name = "mean_reversion"
    display_name = "평균회귀"
    description = "Buy when price bounces back from Bollinger lower band (minute-candle based)"

    def __init__(self) -> None:
        self._init_base()
        self.window: int = 20
        self.num_std: float = 2.0
        self.candle_minutes: int = 3
        self.cooldown_seconds: int = 1800
        self.stop_loss_ratio: float | None = 0.025
        self.min_tick_volume: int = 5
        self.max_candle_range: float = 0.03
        self.open_grace_candles: int = 5
        self._candles: dict[str, deque[_Candle]] = {}
        self._current_candle: dict[str, _Candle] = {}
        self._candle_start: dict[str, datetime] = {}
        self._was_below: dict[str, bool] = {}
        self._realtime_candle_count: dict[str, int] = {}

    def reset(self) -> None:
        super().reset()
        self._candles.clear()
        self._current_candle.clear()
        self._candle_start.clear()
        self._was_below.clear()
        self._realtime_candle_count.clear()

    def _get_candles(self, symbol: str) -> deque[_Candle]:
        if symbol not in self._candles:
            self._candles[symbol] = deque(maxlen=self.window + 5)
        return self._candles[symbol]

    def _rotate_candle(self, symbol: str, price: Decimal, volume: int, now: datetime) -> bool:
        """봉 갱신. 새 봉이 열렸으면 True 반환."""
        candles = self._get_candles(symbol)
        start = self._candle_start.get(symbol)
        interval = self.candle_minutes * 60

        if start is None:
            self._candle_start[symbol] = now
            self._current_candle[symbol] = _Candle(price, volume)
            return False

        elapsed = (now - start).total_seconds()
        if elapsed >= interval:
            if symbol in self._current_candle:
                candles.append(self._current_candle[symbol])
                self._realtime_candle_count[symbol] = self._realtime_candle_count.get(symbol, 0) + 1
            self._candle_start[symbol] = now
            self._current_candle[symbol] = _Candle(price, volume)
            return True
        else:
            if symbol in self._current_candle:
                self._current_candle[symbol].update(price, volume)
            else:
                self._current_candle[symbol] = _Candle(price, volume)
            return False

    def _bands(self, symbol: str) -> tuple[Decimal, Decimal, Decimal] | None:
        candles = list(self._get_candles(symbol))
        if len(candles) < self.window:
            return None
        recent = [float(c.close) for c in candles[-self.window:]]
        mean = sum(recent) / len(recent)
        variance = sum((x - mean) ** 2 for x in recent) / len(recent)
        std = math.sqrt(variance)
        if std == 0:
            return None
        mid = Decimal(str(mean))
        upper = Decimal(str(mean + self.num_std * std))
        lower = Decimal(str(mean - self.num_std * std))
        return lower, mid, upper

    async def on_tick(self, symbol: str, price: Decimal, volume: int) -> Signal | None:
        if volume < self.min_tick_volume:
            return None
        self._advance_tick(symbol)
        now = datetime.now()
        rotated = self._rotate_candle(symbol, price, volume, now)

        if not rotated:
            return None

        candles = list(self._get_candles(symbol))
        if not candles:
            return None
        last = candles[-1]

        candle_range = float(last.high - last.low) / float(last.close) if last.close > 0 else 0
        if candle_range > self.max_candle_range:
            logger.debug("mean_rev.skip", symbol=symbol, reason="candle_range", value=round(candle_range, 4), limit=self.max_candle_range)
            return None

        last_close = last.close

        bands = self._bands(symbol)
        if bands is None:
            return None

        lower, mid, upper = bands
        was_below = self._was_below.get(symbol, False)

        if last_close < lower:
            self._was_below[symbol] = True
            logger.debug("mean_rev.below_band", symbol=symbol, price=str(last_close), lower=str(lower))
            return None

        if was_below and last_close >= lower:
            self._was_below[symbol] = False
            rt = self._realtime_candle_count.get(symbol, 0)
            if rt < self.open_grace_candles:
                logger.debug("mean_rev.skip", symbol=symbol, reason="grace_period", candles=rt, need=self.open_grace_candles)
                return None
            if not self._check_cooldown(symbol, "BUY"):
                logger.debug("mean_rev.skip", symbol=symbol, reason="cooldown", side="BUY")
                return None
            dist = float(mid - last_close) / float(mid) if mid > 0 else 0
            confidence = min(0.85, 0.6 + dist * 5)
            return Signal(symbol, Side.BUY, self.name, last_close, 0, confidence, now)

        if last_close > upper:
            if not self._check_cooldown(symbol, "SELL"):
                logger.debug("mean_rev.skip", symbol=symbol, reason="cooldown", side="SELL")
                return None
            return Signal(symbol, Side.SELL, self.name, last_close, 0, 0.7, now)

        return None

    def prefill(self, symbol: str, candles_data: list[dict]) -> None:
        if not candles_data:
            return
        aggregated = self._aggregate_candles(candles_data)
        candles = self._get_candles(symbol)
        for c in aggregated[-(self.window + 5):]:
            candles.append(c)

    def _aggregate_candles(self, minute_candles: list[dict]) -> list[_Candle]:
        if self.candle_minutes <= 1:
            result = []
            for c in minute_candles:
                candle = _Candle(Decimal(str(c["close"])))
                if "high" in c:
                    candle.high = Decimal(str(c["high"]))
                if "low" in c:
                    candle.low = Decimal(str(c["low"]))
                if "volume" in c:
                    candle.volume = c["volume"]
                result.append(candle)
            return result
        result = []
        group: _Candle | None = None
        for i, c in enumerate(minute_candles):
            price = Decimal(str(c["close"]))
            high = Decimal(str(c.get("high", c["close"])))
            low = Decimal(str(c.get("low", c["close"])))
            vol = c.get("volume", 0)
            if group is None:
                group = _Candle(price, vol)
                group.high = high
                group.low = low
            else:
                if high > group.high:
                    group.high = high
                if low < group.low:
                    group.low = low
                group.close = price
                group.volume += vol
            if (i + 1) % self.candle_minutes == 0:
                result.append(group)
                group = None
        if group is not None:
            result.append(group)
        return result

    async def on_orderbook(
        self,
        symbol: str,
        asks: list[tuple[Decimal, int]],
        bids: list[tuple[Decimal, int]],
    ) -> Signal | None:
        return None
