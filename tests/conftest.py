from decimal import Decimal

import pytest

from scalpy.broker.mock import MockBroker
from scalpy.core.enums import OrderType, Side
from scalpy.core.models import Order
from scalpy.strategy.factor import FactorStrategy
from scalpy.strategy.registry import StrategyRegistry
from scalpy.trading.risk import RiskManager


@pytest.fixture
def mock_broker() -> MockBroker:
    return MockBroker(initial_balance=Decimal("10000000"))


@pytest.fixture
def strategy_registry() -> StrategyRegistry:
    registry = StrategyRegistry()
    registry.register(FactorStrategy())
    return registry


@pytest.fixture
def risk_manager() -> RiskManager:
    return RiskManager(stop_loss_ratio=0.02)


@pytest.fixture
def sample_buy_order() -> Order:
    return Order(
        symbol="005930",
        side=Side.BUY,
        order_type=OrderType.MARKET,
        price=Decimal("72000"),
        quantity=10,
        strategy="factor",
    )
