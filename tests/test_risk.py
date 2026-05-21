from decimal import Decimal

from scalpy.core.enums import OrderType, Side
from scalpy.core.models import Order, Position
from scalpy.trading.risk import RiskManager


class TestRiskManager:
    def setup_method(self) -> None:
        self.risk = RiskManager(
            stop_loss_ratio=0.02,
            max_position_size=100,
        )

    def test_stop_loss_triggered(self) -> None:
        pos = Position(
            symbol="005930",
            side=Side.BUY,
            quantity=10,
            avg_price=Decimal("72000"),
            current_price=Decimal("70500"),  # -2.08%
            strategy="rsi",
        )
        assert self.risk.check_stop_loss(pos) is True

    def test_stop_loss_not_triggered(self) -> None:
        pos = Position(
            symbol="005930",
            side=Side.BUY,
            quantity=10,
            avg_price=Decimal("72000"),
            current_price=Decimal("71500"),  # -0.69%
            strategy="rsi",
        )
        assert self.risk.check_stop_loss(pos) is False

    def test_validate_order_exceeds_max_size(self) -> None:
        order = Order(
            symbol="005930",
            side=Side.BUY,
            order_type=OrderType.MARKET,
            price=Decimal("72000"),
            quantity=200,
            strategy="rsi",
        )
        assert self.risk.validate_order(order, Decimal("20000000")) is False

    def test_validate_order_insufficient_balance(self) -> None:
        order = Order(
            symbol="005930",
            side=Side.BUY,
            order_type=OrderType.MARKET,
            price=Decimal("72000"),
            quantity=50,
            strategy="rsi",
        )
        assert self.risk.validate_order(order, Decimal("1000000")) is False

    def test_validate_order_ok(self) -> None:
        order = Order(
            symbol="005930",
            side=Side.BUY,
            order_type=OrderType.MARKET,
            price=Decimal("72000"),
            quantity=10,
            strategy="rsi",
        )
        assert self.risk.validate_order(order, Decimal("10000000")) is True
