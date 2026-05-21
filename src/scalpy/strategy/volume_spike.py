from collections import deque
from datetime import datetime
from decimal import Decimal

import structlog

from scalpy.core.enums import Side
from scalpy.core.models import Signal
from scalpy.strategy.base import BaseStrategy

logger = structlog.get_logger()


class _Candle:
    __slots__ = ("open", "high", "low", "close", "volume")

    def __init__(self, price: Decimal, volume: int = 0) -> None:
        self.open = price
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

    @property
    def is_bullish(self) -> bool:
        return self.close >= self.open


class VolumeSpikeStrategy(BaseStrategy):
    name = "volume_spike"
    display_name = "거래량폭발"
    description = "Volume surge + momentum breakout: ride the spike"

    def __init__(self) -> None:
        self._init_base()
        self.baseline_window: int = 10
        self.spike_ratio: float = 1.0
        self.candle_minutes: int = 1
        self.lookback: int = 5
        self.cooldown_seconds: int = 120
        self.stop_loss_ratio: float | None = 0.015
        self.min_tick_volume: int = 1
        self._candles: dict[str, deque[_Candle]] = {}
        self._current_candle: dict[str, _Candle] = {}
        self._candle_start: dict[str, datetime] = {}

    def reset(self) -> None:
        super().reset()
        self._candles.clear()
        self._current_candle.clear()
        self._candle_start.clear()

    def _get_candles(self, symbol: str) -> deque[_Candle]:
        if symbol not in self._candles:
            self._candles[symbol] = deque(maxlen=self.baseline_window + self.lookback + 5)
        return self._candles[symbol]

    def _rotate_candle(self, symbol: str, price: Decimal, volume: int, now: datetime) -> None:
        candles = self._get_candles(symbol)
        start = self._candle_start.get(symbol)
        interval = self.candle_minutes * 60

        if start is None:
            self._candle_start[symbol] = now
            self._current_candle[symbol] = _Candle(price, volume)
            return

        elapsed = (now - start).total_seconds()
        if elapsed >= interval:
            if symbol in self._current_candle:
                candles.append(self._current_candle[symbol])
            self._candle_start[symbol] = now
            self._current_candle[symbol] = _Candle(price, volume)
        else:
            if symbol in self._current_candle:
                self._current_candle[symbol].update(price, volume)
            else:
                self._current_candle[symbol] = _Candle(price, volume)

    async def on_tick(self, symbol: str, price: Decimal, volume: int) -> Signal | None:
        if volume < self.min_tick_volume:
            return None
        self._advance_tick(symbol)
        now = datetime.now()
        self._rotate_candle(symbol, price, volume, now)

        candles = list(self._get_candles(symbol))
        if len(candles) < self.baseline_window:
            return None

        cur = self._current_candle.get(symbol)
        if cur is None or cur.volume == 0:
            return None

        recent = candles[-self.lookback:] if len(candles) >= self.lookback else candles
        baseline = candles[:self.baseline_window]

        # 1) 거래량 활발: 최근 평균 거래량 >= baseline 평균
        avg_vol = sum(c.volume for c in baseline) / len(baseline)
        if avg_vol <= 0:
            return None
        recent_avg_vol = sum(c.volume for c in recent) / len(recent)
        vol_ratio = recent_avg_vol / avg_vol
        if vol_ratio < self.spike_ratio:
            return None

        if not cur.is_bullish:
            logger.debug("vol_spike.skip", symbol=symbol, reason="bearish",
                         open=str(cur.open), close=str(cur.close), vol_ratio=round(vol_ratio, 2))
            return None

        prev = candles[-1] if candles else None
        if prev and cur.close < prev.close:
            logger.debug("vol_spike.skip", symbol=symbol, reason="prev_declining",
                         cur_close=str(cur.close), prev_close=str(prev.close))
            return None

        if cur.volume <= 0:
            return None

        if not self._check_cooldown(symbol, "BUY"):
            logger.debug("vol_spike.skip", symbol=symbol, reason="cooldown", side="BUY")
            return None

        confidence = min(0.9, 0.6 + vol_ratio * 0.05)
        logger.info("volume_spike.signal", symbol=symbol, price=str(price),
                     vol_ratio=round(vol_ratio, 2), cur_vol=cur.volume)
        return Signal(symbol, Side.BUY, self.name, price, 0, confidence, now)

    def prefill(self, symbol: str, candles_data: list[dict]) -> None:
        if not candles_data:
            return
        candles = self._get_candles(symbol)
        max_candles = self.baseline_window + self.lookback + 5
        for c in candles_data[-max_candles:]:
            candle = _Candle(Decimal(str(c["close"])))
            if "open" in c:
                candle.open = Decimal(str(c["open"]))
            if "high" in c:
                candle.high = Decimal(str(c["high"]))
            if "low" in c:
                candle.low = Decimal(str(c["low"]))
            if "volume" in c:
                candle.volume = c["volume"]
            candles.append(candle)

    async def on_orderbook(
        self,
        symbol: str,
        asks: list[tuple[Decimal, int]],
        bids: list[tuple[Decimal, int]],
    ) -> Signal | None:
        return None
