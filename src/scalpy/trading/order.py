from collections import deque

import structlog

from scalpy.broker.base import BaseBroker
from scalpy.core.enums import OrderStatus, OrderType
from scalpy.core.models import Order, Signal

logger = structlog.get_logger()


class OrderManager:
    def __init__(self, broker: BaseBroker) -> None:
        self._broker = broker
        self._pending: deque[Order] = deque()
        self._submitted: dict[str, Order] = {}  # order_id -> Order (접수 후 미체결)
        self._history: list[Order] = []

    def signal_to_order(self, signal: Signal, quantity: int) -> Order:
        return Order(
            symbol=signal.symbol,
            side=signal.side,
            order_type=OrderType.MARKET,
            price=signal.price,
            quantity=quantity,
            strategy=signal.strategy,
        )

    async def submit(self, order: Order) -> Order:
        self._pending.append(order)
        logger.info(
            "order.submitted",
            id=order.id,
            symbol=order.symbol,
            side=order.side.value,
            qty=order.quantity,
            price=str(order.price),
        )
        result = await self._broker.place_order(order)
        self._pending.popleft()

        if result.status == OrderStatus.PENDING and result.order_id:
            self._submitted[result.order_id] = result
        else:
            self._history.append(result)

        if result.status == OrderStatus.FILLED:
            logger.info("order.filled", id=result.id)
        elif result.status == OrderStatus.PENDING:
            logger.info("order.pending", id=result.id, order_id=result.order_id)
        else:
            logger.warning("order.not_filled", id=result.id, status=result.status.value)

        return result

    def mark_filled(self, order_id: str) -> Order | None:
        order = self._submitted.pop(order_id, None)
        if order:
            order.status = OrderStatus.FILLED
            self._history.append(order)
        return order

    def mark_cancelled(self, order_id: str) -> Order | None:
        order = self._submitted.pop(order_id, None)
        if order:
            order.status = OrderStatus.CANCELLED
            self._history.append(order)
        return order

    def get_pending(self) -> list[Order]:
        return list(self._pending) + list(self._submitted.values())

    def get_history(self) -> list[Order]:
        return list(self._history)

    def has_pending_for(self, symbol: str) -> bool:
        if any(o.symbol == symbol for o in self._pending):
            return True
        return any(o.symbol == symbol for o in self._submitted.values())
