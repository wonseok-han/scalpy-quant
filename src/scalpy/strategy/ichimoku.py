from collections import deque
from datetime import datetime
from decimal import Decimal

import structlog

from scalpy.core.enums import Side
from scalpy.core.models import Signal
from scalpy.strategy.base import BaseStrategy

logger = structlog.get_logger()


class _Candle:
    __slots__ = ("high", "low", "close")

    def __init__(self, price: Decimal) -> None:
        self.high = price
        self.low = price
        self.close = price

    def update(self, price: Decimal) -> None:
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        self.close = price


class IchimokuStrategy(BaseStrategy):
    name = "ichimoku"
    display_name = "일목균형"
    description = "Ichimoku Cloud: Tenkan/Kijun cross + cloud position (minute-candle based)"

    def __init__(self) -> None:
        self._init_base()
        self.tenkan_period: int = 9
        self.kijun_period: int = 26
        self.senkou_b_period: int = 52
        self.candle_minutes: int = 1
        self.cooldown_seconds: int = 300
        self.stop_loss_ratio: float | None = 0.025

        self.min_tick_volume: int = 5
        self.max_cloud_distance: float = 0.01
        self.max_candle_range: float = 0.03
        self.open_grace_candles: int = 10
        self._candles: dict[str, deque[_Candle]] = {}
        self._current_candle: dict[str, _Candle] = {}
        self._candle_start: dict[str, datetime] = {}
        self._live_candle_count: dict[str, int] = {}
        self._realtime_candle_count: dict[str, int] = {}

    def reset(self) -> None:
        super().reset()
        self._candles.clear()
        self._current_candle.clear()
        self._candle_start.clear()
        self._live_candle_count.clear()
        self._realtime_candle_count.clear()

    def _get_candles(self, symbol: str) -> deque[_Candle]:
        if symbol not in self._candles:
            self._candles[symbol] = deque(maxlen=self.senkou_b_period + 5)
        return self._candles[symbol]

    def _rotate_candle(self, symbol: str, price: Decimal, now: datetime) -> bool:
        """봉 갱신. 새 봉이 열렸으면 True 반환."""
        candles = self._get_candles(symbol)
        start = self._candle_start.get(symbol)
        interval = self.candle_minutes * 60

        if start is None:
            self._candle_start[symbol] = now
            self._current_candle[symbol] = _Candle(price)
            return False

        elapsed = (now - start).total_seconds()
        if elapsed >= interval:
            if symbol in self._current_candle:
                candles.append(self._current_candle[symbol])
                count = self._live_candle_count.get(symbol, 0) + 1
                self._live_candle_count[symbol] = count
                rt = self._realtime_candle_count.get(symbol, 0) + 1
                self._realtime_candle_count[symbol] = rt
                if count <= self.senkou_b_period and count % 10 == 0:
                    logger.info("ichimoku.warmup", symbol=symbol, candles=count, need=self.senkou_b_period)
                if rt == self.open_grace_candles:
                    logger.info("ichimoku.grace_done", symbol=symbol, realtime_candles=rt)
            self._candle_start[symbol] = now
            self._current_candle[symbol] = _Candle(price)
            return True
        else:
            if symbol in self._current_candle:
                self._current_candle[symbol].update(price)
            else:
                self._current_candle[symbol] = _Candle(price)
            return False

    def _midpoint(self, candles: list[_Candle], period: int) -> Decimal | None:
        if len(candles) < period:
            return None
        window = candles[-period:]
        high = max(c.high for c in window)
        low = min(c.low for c in window)
        return (high + low) / 2

    def _compute(self, symbol: str) -> dict | None:
        candles = list(self._get_candles(symbol))

        tenkan = self._midpoint(candles, self.tenkan_period)
        kijun = self._midpoint(candles, self.kijun_period)
        senkou_a = (tenkan + kijun) / 2 if tenkan and kijun else None
        senkou_b = self._midpoint(candles, self.senkou_b_period)
        if not all([tenkan, kijun, senkou_a, senkou_b]):
            return None
        return {
            "tenkan": tenkan,
            "kijun": kijun,
            "senkou_a": senkou_a,
            "senkou_b": senkou_b,
        }

    async def on_tick(self, symbol: str, price: Decimal, volume: int) -> Signal | None:
        if volume < self.min_tick_volume:
            return None
        self._advance_tick(symbol)
        now = datetime.now()
        rotated = self._rotate_candle(symbol, price, now)

        if not rotated:
            return None

        candles = list(self._get_candles(symbol))
        if not candles:
            return None
        last = candles[-1]

        candle_range = float(last.high - last.low) / float(last.close) if last.close > 0 else 0
        if candle_range > self.max_candle_range:
            logger.debug("ichimoku.skip", symbol=symbol, reason="candle_range", value=round(candle_range, 4), limit=self.max_candle_range)
            return None

        candle_count = self._live_candle_count.get(symbol, 0)
        if candle_count < self.senkou_b_period:
            return None

        ichi = self._compute(symbol)
        if ichi is None:
            return None

        last_close = last.close

        tenkan = ichi["tenkan"]
        kijun = ichi["kijun"]
        cloud_top = max(ichi["senkou_a"], ichi["senkou_b"])
        cloud_bottom = min(ichi["senkou_a"], ichi["senkou_b"])

        above_cloud = last_close > cloud_top
        below_cloud = last_close < cloud_bottom
        tk_bull = tenkan > kijun
        tk_bear = tenkan < kijun

        if above_cloud and tk_bull:
            rt = self._realtime_candle_count.get(symbol, 0)
            if rt < self.open_grace_candles:
                logger.debug("ichimoku.skip", symbol=symbol, reason="grace_period", candles=rt, need=self.open_grace_candles)
                return None
            spread = float(last_close - cloud_top) / float(cloud_top) if cloud_top else 0
            if spread > self.max_cloud_distance:
                logger.debug("ichimoku.skip", symbol=symbol, reason="overheated", spread=round(spread, 4), limit=self.max_cloud_distance)
                return None
            if not self._check_cooldown(symbol, "BUY"):
                logger.debug("ichimoku.skip", symbol=symbol, reason="cooldown", side="BUY")
                return None
            confidence = min(0.85, 0.5 + spread * 20)
            return Signal(symbol, Side.BUY, self.name, last_close, 0, confidence, now)

        if below_cloud and tk_bear and self._check_cooldown(symbol, "SELL"):
            spread = float(cloud_bottom - last_close) / float(cloud_bottom) if cloud_bottom else 0
            confidence = min(0.85, 0.5 + spread * 20)
            return Signal(symbol, Side.SELL, self.name, last_close, 0, confidence, now)

        if not below_cloud and last_close < kijun and self._check_cooldown(symbol, "SELL"):
            return Signal(symbol, Side.SELL, self.name, last_close, 0, 0.7, now)

        if above_cloud and tk_bear and self._check_cooldown(symbol, "SELL"):
            return Signal(symbol, Side.SELL, self.name, last_close, 0, 0.6, now)

        return None

    def prefill(self, symbol: str, candles_data: list[dict]) -> None:
        if not candles_data:
            return
        has_minute = "time" in candles_data[0]
        if not has_minute:
            return
        candles = self._get_candles(symbol)
        limit = self.senkou_b_period + 5
        for c in candles_data[-limit:]:
            candle = _Candle(Decimal(str(c["close"])))
            if "high" in c:
                candle.high = Decimal(str(c["high"]))
            if "low" in c:
                candle.low = Decimal(str(c["low"]))
            candles.append(candle)
        self._live_candle_count[symbol] = len(candles)

    async def on_orderbook(
        self,
        symbol: str,
        asks: list[tuple[Decimal, int]],
        bids: list[tuple[Decimal, int]],
    ) -> Signal | None:
        return None
