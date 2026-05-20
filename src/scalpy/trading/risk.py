from datetime import datetime
from decimal import Decimal

import structlog

from scalpy.core.models import Order, Position

logger = structlog.get_logger()


class RiskManager:
    def __init__(
        self,
        stop_loss_ratio: float = 0.02,
        max_position_size: int = 100,
        max_open_positions: int = 3,
        max_position_ratio: float = 0.3,
        stagnation_hours: float = 0,
        stagnation_threshold: float = 0.005,
        trailing_activate_ratio: float = 0.01,
        trailing_stop_ratio: float = 0.01,
        profit_protect_activate: float = 0.008,
        profit_protect_ratio: float = 0.005,
    ) -> None:
        self.stop_loss_ratio = Decimal(str(stop_loss_ratio))
        self.trailing_activate_ratio = Decimal(str(trailing_activate_ratio))
        self.trailing_stop_ratio = Decimal(str(trailing_stop_ratio))
        self.profit_protect_activate = Decimal(str(profit_protect_activate))
        self.profit_protect_ratio = Decimal(str(profit_protect_ratio))
        self.max_position_size = max_position_size
        self.max_open_positions = max_open_positions
        self.max_position_ratio = max_position_ratio
        self.stagnation_hours = stagnation_hours
        self.stagnation_threshold = Decimal(str(stagnation_threshold))

    def check_stop_loss(self, position: Position, override_ratio: Decimal | None = None) -> bool:
        if position.quantity == 0:
            return False
        ratio = override_ratio if override_ratio is not None else self.stop_loss_ratio
        loss_ratio = (position.avg_price - position.current_price) / position.avg_price
        triggered = loss_ratio >= ratio
        if triggered:
            logger.warning(
                "risk.stop_loss_triggered",
                symbol=position.symbol,
                loss_ratio=str(loss_ratio),
            )
        return triggered

    def is_trailing_active(self, position: Position) -> bool:
        if position.quantity == 0 or position.avg_price <= 0:
            return False
        gain = (position.current_price - position.avg_price) / position.avg_price
        return gain >= self.trailing_activate_ratio

    def check_trailing_stop(self, position: Position) -> bool:
        if position.quantity == 0 or position.peak_price <= 0:
            return False
        if not self.is_trailing_active(position):
            return False
        drop = (position.peak_price - position.current_price) / position.peak_price
        triggered = drop >= self.trailing_stop_ratio
        if triggered:
            logger.info(
                "risk.trailing_stop_triggered",
                symbol=position.symbol,
                peak=str(position.peak_price),
                current=str(position.current_price),
                drop=str(drop),
            )
        return triggered

    def check_profit_protect(self, position: Position) -> bool:
        if position.quantity == 0 or position.avg_price <= 0 or position.peak_price <= 0:
            return False
        if self.profit_protect_activate <= 0:
            return False
        gain = (position.current_price - position.avg_price) / position.avg_price
        if gain < Decimal("0"):
            return False
        peak_gain = (position.peak_price - position.avg_price) / position.avg_price
        if peak_gain < self.profit_protect_activate:
            return False
        if gain >= self.trailing_activate_ratio:
            return False
        drop = (position.peak_price - position.current_price) / position.peak_price
        triggered = drop >= self.profit_protect_ratio
        if triggered:
            logger.info(
                "risk.profit_protect_triggered",
                symbol=position.symbol,
                peak_gain=str(round(peak_gain, 4)),
                current_gain=str(round(gain, 4)),
                drop=str(round(drop, 4)),
            )
        return triggered

    def check_stagnation(self, position: Position) -> bool:
        if self.stagnation_hours <= 0 or position.quantity == 0:
            return False
        now = datetime.now(position.opened_at.tzinfo)
        elapsed = (now - position.opened_at).total_seconds() / 3600
        if elapsed < self.stagnation_hours:
            return False
        change = abs(position.current_price - position.avg_price) / position.avg_price
        triggered = change <= self.stagnation_threshold
        if triggered:
            logger.info(
                "risk.stagnation_triggered",
                symbol=position.symbol,
                hours=round(elapsed, 1),
                change=str(change),
            )
        return triggered

    def validate_order(self, order: Order, balance: Decimal) -> bool:
        if order.quantity > self.max_position_size:
            logger.warning(
                "risk.order_rejected",
                reason="exceeds_max_position_size",
                qty=order.quantity,
                max=self.max_position_size,
            )
            return False

        cost = order.price * order.quantity
        if cost > balance:
            logger.warning(
                "risk.order_rejected",
                reason="insufficient_balance",
                cost=str(cost),
                balance=str(balance),
            )
            return False

        return True

    def get_max_position_size(
        self,
        symbol: str,
        balance: Decimal,
        price: Decimal,
        total_asset: Decimal | None = None,
    ) -> int:
        if price <= 0:
            return 0

        if total_asset and total_asset > 0:
            per_slot = total_asset / Decimal(str(self.max_open_positions))
            max_cost = min(per_slot, balance)
        else:
            max_cost = balance * Decimal(str(self.max_position_ratio))

        cap = balance * Decimal(str(self.max_position_ratio))
        max_cost = min(max_cost, cap)

        by_cost = int(max_cost / price)
        affordable = int(balance / price)
        return min(affordable, by_cost, self.max_position_size)
