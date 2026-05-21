from datetime import datetime, time as dt_time
from decimal import Decimal
from unittest.mock import patch

from scalpy.broker.mock import MockBroker
from scalpy.core.enums import Side
from scalpy.core.models import Signal
from scalpy.strategy.base import BaseStrategy
from scalpy.strategy.registry import StrategyRegistry
from scalpy.trading.engine import TradingEngine
from scalpy.trading.risk import RiskManager

_MOCK_MARKET_OPEN = dt_time(10, 0)


class _AlwaysBuyStrategy(BaseStrategy):
    name = "test_buy"
    display_name = "Test Buy"
    description = "Test"

    def __init__(self):
        self._init_base()
        self._count = 0

    async def on_tick(self, symbol, price, volume):
        self._count += 1
        if self._count == 5:
            return Signal(
                symbol=symbol, side=Side.BUY, price=price,
                strategy=self.name, quantity=1, confidence=1.0,
                timestamp=datetime.now(),
            )
        return None

    async def on_orderbook(self, symbol, asks, bids):
        return None


class TestTradingEngine:
    def setup_method(self) -> None:
        self.broker = MockBroker(initial_balance=Decimal("10000000"))
        self.registry = StrategyRegistry()
        self.registry.register(_AlwaysBuyStrategy())
        self.risk = RiskManager(stop_loss_ratio=0.02)
        self.engine = TradingEngine(self.broker, self.registry, self.risk)
        self.engine._is_market_hours = lambda: True
        self.engine._market = type("MockMarket", (), {
            "is_buy_cutoff": lambda self, now=None: False,
            "is_close_window": lambda self, now=None: False,
            "timezone": __import__("zoneinfo").ZoneInfo("Asia/Seoul"),
        })()
        self._time_patch = patch(
            "scalpy.trading.engine.datetime",
            wraps=datetime,
        )
        mock_dt = self._time_patch.start()
        mock_dt.now = lambda tz=None: datetime(2026, 1, 5, 10, 0, 0, tzinfo=tz)

    def teardown_method(self) -> None:
        self._time_patch.stop()

    async def test_signal_triggers_order(self) -> None:
        self.engine._running = True

        for i in range(6):
            await self.engine.on_tick("005930", Decimal("72000"), 100)

        orders = self.engine.orders.get_history()
        buy_orders = [o for o in orders if o.side == Side.BUY]
        assert len(buy_orders) > 0

    async def test_engine_ignores_ticks_when_stopped(self) -> None:
        await self.engine.on_tick("005930", Decimal("100"), 100)
        assert len(self.engine.orders.get_history()) == 0
