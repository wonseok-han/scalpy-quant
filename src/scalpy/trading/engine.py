import asyncio
import time
from datetime import datetime
from decimal import Decimal
from typing import Any

import structlog

from scalpy.broker.base import BaseBroker
from scalpy.config import settings
from scalpy.core.enums import OrderStatus, Side
from scalpy.core.market import KR_MARKET, MarketConfig
from scalpy.core.models import Position, Signal
from scalpy.events.bus import EventBus
from scalpy.strategy.registry import StrategyRegistry
from scalpy.trading.order import OrderManager
from scalpy.trading.performance import PerformanceTracker
from scalpy.trading.position import PositionManager
from scalpy.trading.risk import RiskManager

logger = structlog.get_logger()

_BALANCE_CACHE_TTL = 10
_SYNC_INTERVAL = 10
_RISK_CHECK_INTERVAL = 10
_MIN_CONFIDENCE = 0.5
_REJECTED_COOLDOWN = 60
_SPLIT_SELL_DELAY = 1.0
_UNFILLED_CANCEL_INTERVAL = 60


class TradingEngine:
    def __init__(
        self,
        broker: BaseBroker,
        registry: StrategyRegistry,
        risk: RiskManager,
        market_config: MarketConfig | None = None,
    ) -> None:
        self._broker = broker
        self._registry = registry
        self._risk = risk
        self._market = market_config or KR_MARKET
        self._orders = OrderManager(broker)
        self._running = False
        self._bus: EventBus | None = None
        self._cached_balance: Decimal = Decimal("0")
        self._cached_available_cash: Decimal = Decimal("0")
        self._balance_fetched_at: float = 0
        self._market_close_done = False
        self._last_sync_at: float = 0
        self._rejected_symbols: dict[str, float] = {}
        self._closed_symbols: dict[str, float] = {}
        self._untradeable: set[str] = set()
        self._risk_loop_task: asyncio.Task[None] | None = None
        self._sync_loop_task: asyncio.Task[None] | None = None
        self._sync_running = False
        self._active_symbols: set[str] = set()
        self._performance = PerformanceTracker()
        self._trade_repo: Any = None
        self._pending_buy_cost: Decimal = Decimal("0")
        self._last_unfilled_cancel: float = 0
        self._last_daily_init: str = ""
        self._trade_reasons: dict[str, str] = {}
        self._position_pnl: dict[str, Decimal] = {}
        self._position_strategy: dict[str, str] = {}

    def set_trade_repo(self, repo: Any) -> None:
        self._trade_repo = repo

    def set_event_bus(self, bus: EventBus) -> None:
        self._bus = bus

    @property
    def positions(self) -> PositionManager:
        return self._broker.positions

    @property
    def orders(self) -> OrderManager:
        return self._orders

    async def get_cached_balance(self) -> Decimal:
        now = time.monotonic()
        if now - self._balance_fetched_at > _BALANCE_CACHE_TTL:
            try:
                self._cached_balance = await self._broker.get_balance()
                self._cached_available_cash = await self._broker.get_available_cash()
                self._balance_fetched_at = now
            except Exception as e:
                logger.warning("engine.balance_fetch_failed", error=str(e))
        return self._cached_balance

    async def sync_positions(self) -> int:
        try:
            count = await self._broker.sync_positions()
        except Exception as e:
            logger.warning("engine.sync_failed", error=str(e))
            return 0

        now = time.monotonic()
        skip = self._untradeable | {
            s for s, t in self._rejected_symbols.items() if now - t < _REJECTED_COOLDOWN
        }
        for sym in skip:
            self.positions.remove(sym)

        self._last_sync_at = time.monotonic()

        if self._trade_repo:
            try:
                db_times = self._trade_repo.get_open_position_times()
                strat_map = self._trade_repo.get_position_strategies()
                for pos in self.positions.all():
                    if pos.symbol in db_times:
                        pos.opened_at = db_times[pos.symbol]
                    if pos.strategy == "synced" and pos.symbol in strat_map:
                        pos.strategy = strat_map[pos.symbol]
            except Exception:
                pass

        if self._running:
            for pos in self.positions.all():
                await self._check_risk(pos.symbol)

        return count

    def prefill_strategies(self, symbol: str, candles: list[dict]) -> None:
        if not candles:
            return
        for strategy in self._registry.all():
            strategy.prefill(symbol, candles)
        logger.info("engine.prefilled", symbol=symbol, candles=len(candles))

    async def prefill_minute_candles(self, symbols: list[str]) -> None:
        candle_strategies = []
        need = 0
        for s in self._registry.all():
            if not s.enabled:
                continue
            if hasattr(s, '_candles') and hasattr(s, 'candle_minutes'):
                candle_strategies.append(s)
                strat_need = getattr(s, 'senkou_b_period', 0) or getattr(s, 'baseline_window', 0) or getattr(s, 'window', 0)
                candle_min = getattr(s, 'candle_minutes', 1)
                need = max(need, (strat_need + 5) * candle_min)
        if not candle_strategies:
            logger.debug("engine.candle_prefill_skip", reason="no_candle_strategies")
            return
        names = [s.name for s in candle_strategies]
        logger.info("engine.candle_prefill_start", symbols=len(symbols), need=need, strategies=names)
        for sym in symbols:
            try:
                candles = await self._broker.get_minute_candles(sym, need)
                if candles:
                    for s in candle_strategies:
                        s.prefill(sym, candles)
                    logger.info("engine.candle_prefilled", symbol=sym, candles=len(candles))
                else:
                    logger.info("engine.candle_prefill_empty", symbol=sym)
            except Exception as e:
                logger.warning("engine.candle_prefill_failed", symbol=sym, error=str(e))

    async def update_symbols(self, symbols: list[str]) -> None:
        held = {p.symbol for p in self.positions.all()}
        self._active_symbols = set(symbols) | held
        logger.info("engine.symbols_updated", symbols=list(self._active_symbols))

    async def start(self) -> None:
        await self._broker.connect()
        self._running = True
        self.start_background_loops()
        logger.info("engine.started")
        if self._bus:
            await self._bus.emit("engine.started")

    async def stop(self) -> None:
        self._running = False
        self.stop_background_loops()
        self.stop_sync_loop()
        await self._broker.disconnect()
        logger.info("engine.stopped")
        if self._bus:
            await self._bus.emit("engine.stopped")

    def start_sync_loop(self) -> None:
        self._sync_running = True
        if not self._sync_loop_task or self._sync_loop_task.done():
            self._sync_loop_task = asyncio.create_task(self._sync_loop())

    def stop_sync_loop(self) -> None:
        self._sync_running = False
        if self._sync_loop_task and not self._sync_loop_task.done():
            self._sync_loop_task.cancel()
        self._sync_loop_task = None

    def start_background_loops(self) -> None:
        if not self._risk_loop_task or self._risk_loop_task.done():
            self._risk_loop_task = asyncio.create_task(self._risk_check_loop())
        self.start_sync_loop()

    def stop_background_loops(self) -> None:
        if self._risk_loop_task and not self._risk_loop_task.done():
            self._risk_loop_task.cancel()
        self._risk_loop_task = None

    async def _risk_check_loop(self) -> None:
        while self._running:
            await asyncio.sleep(_RISK_CHECK_INTERVAL)
            if not self._running:
                break
            for pos in self.positions.all():
                await self._check_risk(pos.symbol)

    async def _sync_loop(self) -> None:
        while self._sync_running:
            await asyncio.sleep(_SYNC_INTERVAL)
            if not self._sync_running:
                break
            try:
                await self._daily_init()
                await self.sync_positions()
                now = time.monotonic()
                if now - self._last_unfilled_cancel >= _UNFILLED_CANCEL_INTERVAL:
                    cancelled = await self._broker.cancel_all_orders()
                    if cancelled > 0:
                        logger.info("engine.unfilled_cancelled", count=cancelled)
                    self._last_unfilled_cancel = now
                if self._bus:
                    await self._bus.emit("position.updated", {})
            except Exception as e:
                logger.warning("engine.sync_loop_failed", error=str(e))

    async def _daily_init(self) -> None:
        now = datetime.now(self._market.timezone)
        today = now.strftime("%Y%m%d")
        if self._last_daily_init == today:
            return
        if not self._market.should_daily_init(now):
            return

        logger.info("engine.daily_init_start", date=today)

        cancelled = await self._broker.cancel_all_orders()
        if cancelled > 0:
            logger.info("engine.daily_init.cancelled_orders", count=cancelled)

        await self.sync_positions()

        self._cached_balance = Decimal("0")
        self._balance_fetched_at = 0
        await self.get_cached_balance()

        self._rejected_symbols.clear()
        self._closed_symbols.clear()
        self._pending_buy_cost = Decimal("0")
        self._market_close_done = False

        self._last_daily_init = today
        logger.info("engine.daily_init_done", date=today)

        if self._bus:
            await self._bus.emit("engine.daily_init", {"date": today})

    def _is_market_hours(self) -> bool:
        return self._market.is_market_hours()

    async def on_tick(self, symbol: str, price: Decimal, volume: int) -> None:
        if not self._running:
            return

        self.positions.update_price(symbol, price)
        if self._bus:
            await self._bus.emit(
                "tick.received",
                {"symbol": symbol, "price": str(price), "volume": volume},
            )
            pos = self.positions.get(symbol)
            if pos:
                await self._bus.emit(
                    "position.updated", {"symbol": symbol, "current_price": str(price)}
                )

        if not self._is_market_hours():
            return

        await self._check_risk(symbol)
        await self._check_market_close()

        for strategy in self._registry.enabled():
            signal = await strategy.on_tick(symbol, price, volume)
            if signal is not None:
                logger.info(
                    "engine.signal_raw",
                    symbol=signal.symbol,
                    side=signal.side.value,
                    strategy=signal.strategy,
                    confidence=signal.confidence,
                    price=str(signal.price),
                )
                await self._process_signal(signal)

    async def on_orderbook(
        self,
        symbol: str,
        asks: list[tuple[Decimal, int]],
        bids: list[tuple[Decimal, int]],
    ) -> None:
        if not self._running or not self._is_market_hours():
            return

        for strategy in self._registry.enabled():
            signal = await strategy.on_orderbook(symbol, asks, bids)
            if signal is not None:
                await self._process_signal(signal)

    async def on_fill_notice(self, data: dict[str, Any]) -> None:
        symbol = data["symbol"]
        order_no = data.get("order_no", "")

        if data.get("is_rejected"):
            self._rejected_symbols[symbol] = time.monotonic()
            if order_no:
                self._orders.mark_cancelled(order_no)
            logger.warning(
                "engine.ws_order_rejected", symbol=symbol, order_no=order_no
            )
            return
        if not data.get("is_fill"):
            return

        if order_no:
            self._orders.mark_filled(order_no)

        side = data["side"]
        qty = data["quantity"]
        price = Decimal(str(data["price"]))
        strategy = "ws_fill"

        if side == "buy":
            pos = self.positions.get(symbol)
            if pos:
                strategy = pos.strategy
                total_qty = pos.quantity + qty
                pos.avg_price = (pos.avg_price * pos.quantity + price * qty) / total_qty
                pos.quantity = total_qty
                pos.current_price = price
            else:
                self.positions._positions[symbol] = Position(
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=qty,
                    avg_price=price,
                    current_price=price,
                    strategy="ws_fill",
                )
            logger.info("engine.ws_fill_buy", symbol=symbol, qty=qty, price=str(price))
        else:
            pos = self.positions.get(symbol)
            if pos:
                strategy = pos.strategy
                remaining = pos.quantity - qty
                if remaining <= 0:
                    self.positions.remove(symbol)
                    self._closed_symbols[symbol] = time.monotonic()
                else:
                    pos.quantity = remaining
                    pos.current_price = price
            logger.info("engine.ws_fill_sell", symbol=symbol, qty=qty, price=str(price))

        if self._bus:
            await self._bus.emit(
                "order.filled",
                {
                    "symbol": symbol,
                    "side": side,
                    "price": str(price),
                    "qty": qty,
                    "strategy": strategy,
                },
            )

    async def on_vi_event(self, symbol: str, triggered: bool) -> None:
        if triggered:
            self._untradeable.add(symbol)
            pos = self.positions.get(symbol)
            if pos:
                logger.warning("engine.vi_hold", symbol=symbol)
        else:
            self._untradeable.discard(symbol)

    async def _process_signal(self, signal: Signal) -> None:
        if not self._running:
            return
        if signal.confidence < _MIN_CONFIDENCE:
            logger.debug("engine.signal_blocked", symbol=signal.symbol, reason="low_confidence", confidence=signal.confidence)
            return
        if self._orders.has_pending_for(signal.symbol):
            logger.debug("engine.signal_blocked", symbol=signal.symbol, reason="pending_order")
            return

        if signal.symbol in self._untradeable:
            logger.debug("engine.signal_blocked", symbol=signal.symbol, reason="untradeable")
            return

        if signal.side == Side.BUY:
            closed_at = self._closed_symbols.get(signal.symbol)
            close_cooldown = settings.get("trading.close_cooldown_seconds", 300)
            if closed_at and time.monotonic() - closed_at < close_cooldown:
                logger.debug("engine.signal_blocked", symbol=signal.symbol, reason="close_cooldown")
                return
            rejected_at = self._rejected_symbols.get(signal.symbol)
            if rejected_at and time.monotonic() - rejected_at < _REJECTED_COOLDOWN:
                logger.debug("engine.signal_blocked", symbol=signal.symbol, reason="rejected_cooldown")
                return
            if self._market.is_buy_cutoff():
                logger.info("engine.buy_blocked_market_closing", symbol=signal.symbol)
                return
            if self.positions.get(signal.symbol) is not None or self._orders.has_pending_for(signal.symbol):
                logger.debug("engine.signal_blocked", symbol=signal.symbol, reason="already_holding")
                return
            pending_buy_count = sum(1 for o in self._orders.get_pending() if o.side == Side.BUY)
            if len(self.positions.all()) + pending_buy_count >= self._risk.max_open_positions:
                logger.debug("engine.signal_blocked", symbol=signal.symbol, reason="max_positions")
                return

        qty = 0
        sell_pos: Position | None = None

        if signal.side == Side.BUY:
            try:
                broker_qty = await self._broker.get_buyable_qty(signal.symbol, signal.price)
            except Exception as e:
                logger.warning("engine.buyable_qty_failed", symbol=signal.symbol, error=str(e))
                return
            if broker_qty <= 0:
                logger.info("engine.signal_blocked", symbol=signal.symbol, reason="zero_buyable_qty")
                return
            qty = min(broker_qty, self._risk.max_position_size)
            await self.get_cached_balance()
            total_asset = self._cached_balance
            if total_asset > 0:
                cap = int(total_asset * Decimal(str(self._risk.max_position_ratio)) / signal.price)
                qty = min(qty, cap)
        else:
            pos = self.positions.get(signal.symbol)
            if pos is None or pos.quantity == 0:
                logger.debug("engine.sell_blocked", symbol=signal.symbol, reason="no_position", strategy=signal.strategy)
                return
            if pos.strategy != "synced" and pos.strategy != signal.strategy:
                logger.debug("engine.sell_blocked", symbol=signal.symbol, reason="strategy_mismatch",
                             pos_strategy=pos.strategy, signal_strategy=signal.strategy)
                return
            min_hold = settings.get("trading.min_hold_seconds", 60)
            held = (datetime.now(pos.opened_at.tzinfo) - pos.opened_at).total_seconds()
            if held < min_hold:
                logger.debug("engine.sell_blocked", symbol=signal.symbol, reason="min_hold",
                             held_sec=round(held), min_sec=min_hold)
                return
            gain = (pos.current_price - pos.avg_price) / pos.avg_price if pos.avg_price > 0 else Decimal("0")
            if gain >= Decimal("0") and self._risk.is_trailing_active(pos):
                if signal.confidence < 0.7:
                    logger.debug("engine.sell_blocked", symbol=signal.symbol, reason="trailing_active_low_conf",
                                 gain=str(round(gain, 4)), confidence=signal.confidence)
                    return
            sell_pos = pos
            qty = pos.quantity

        if qty <= 0:
            return

        order = self._orders.signal_to_order(signal, qty)

        if self._bus:
            await self._bus.emit(
                "signal.generated",
                {
                    "symbol": signal.symbol,
                    "side": signal.side.value,
                    "strategy": signal.strategy,
                    "price": str(signal.price),
                    "confidence": signal.confidence,
                },
            )

        buy_cost = order.price * order.quantity if signal.side == Side.BUY else Decimal("0")
        self._pending_buy_cost += buy_cost
        result = await self._orders.submit(order)
        self._pending_buy_cost -= buy_cost
        if result.status == OrderStatus.REJECTED:
            if "매매불가" in result.reject_reason or "잔고" in result.reject_reason:
                self._untradeable.add(signal.symbol)
                logger.warning("engine.untradeable", symbol=signal.symbol)
            else:
                self._rejected_symbols[signal.symbol] = time.monotonic()
            if signal.side == Side.SELL:
                self.positions.remove(signal.symbol)
                logger.info("engine.stale_position_removed", symbol=signal.symbol)
            return
        if result.status == OrderStatus.PENDING:
            logger.info("engine.order_pending", symbol=result.symbol,
                        side=result.side.value, order_id=result.order_id)
            return
        if result.status == OrderStatus.FILLED:
            self.positions.update_on_fill(result)
            if self._bus:
                await self._bus.emit(
                    "order.filled",
                    {
                        "symbol": result.symbol,
                        "side": result.side.value,
                        "price": str(result.price),
                        "qty": result.quantity,
                        "strategy": result.strategy,
                    },
                )
                if result.side == Side.BUY:
                    self._trade_reasons[result.symbol] = "signal"
                    self._position_pnl[result.symbol] = Decimal("0")
                    self._position_strategy[result.symbol] = result.strategy
                    if self._trade_repo:
                        try:
                            self._trade_repo.save_position_open(result.symbol, result.strategy)
                        except Exception:
                            pass
                    await self._bus.emit(
                        "position.opened",
                        {
                            "symbol": result.symbol,
                            "qty": result.quantity,
                            "avg_price": str(result.price),
                            "strategy": result.strategy,
                        },
                    )
                elif result.side == Side.SELL:
                    if sell_pos:
                        pnl = (result.price - sell_pos.avg_price) * result.quantity
                        self._position_pnl[result.symbol] = self._position_pnl.get(result.symbol, Decimal("0")) + pnl
                    remaining = self.positions.get(result.symbol)
                    if remaining is None or remaining.quantity == 0:
                        strat = self._position_strategy.pop(result.symbol, result.strategy)
                        total = self._position_pnl.pop(result.symbol, Decimal("0"))
                        self._performance.record_trade(strat, total, symbol=result.symbol)
                        self._closed_symbols[result.symbol] = time.monotonic()
                        if self._trade_repo:
                            try:
                                self._trade_repo.close_position(result.symbol)
                            except Exception:
                                pass
                    self._trade_reasons[result.symbol] = "signal"
                    await self._bus.emit(
                        "position.closed",
                        {
                            "symbol": result.symbol,
                            "qty": result.quantity,
                            "reason": "signal",
                        },
                    )

    async def _check_market_close(self) -> None:
        if not self._market.is_close_window():
            self._market_close_done = False
            return
        if self._market_close_done:
            return

        if not settings.get("trading.market_close_liquidate", True):
            self._market_close_done = True
            logger.info("engine.market_close_hold", count=len(self.positions.all()))
            return

        positions = list(self.positions.all())
        if not positions:
            return
        logger.warning("engine.market_close_liquidation", count=len(positions))
        for pos in positions:
            await self._force_close(pos, reason="market_close")
        self._running = False
        self.stop_background_loops()
        self._market_close_done = True
        logger.info("engine.auto_stopped_market_close")
        if self._bus:
            await self._bus.emit("engine.stopped")

    def _get_strategy_sl(self, strategy_name: str) -> Decimal | None:
        strategy = self._registry.get(strategy_name)
        if strategy is None:
            return None
        return Decimal(str(strategy.stop_loss_ratio)) if strategy.stop_loss_ratio is not None else None

    async def _check_risk(self, symbol: str) -> None:
        pos = self.positions.get(symbol)
        if pos is None:
            return

        sl_ratio = self._get_strategy_sl(pos.strategy)

        if self._risk.check_stop_loss(pos, sl_ratio):
            await self._force_close(pos, reason="stop_loss")
        elif self._risk.check_trailing_stop(pos):
            await self._force_close(pos, reason="trailing_stop")
        elif self._risk.check_profit_protect(pos):
            await self._force_close(pos, reason="profit_protect")
        elif self._risk.check_stagnation(pos):
            await self._force_close(pos, reason="stagnation")

    async def _force_close(self, pos: Position, reason: str = "") -> None:
        if reason == "stop_loss" or not self._trade_repo:
            splits = 1
        else:
            splits = max(1, settings.get("trading.force_close_splits", 1))
        remaining = pos.quantity
        total_pnl = Decimal("0")
        total_sold = 0

        for i in range(splits):
            if remaining <= 0:
                break
            chunk = remaining // (splits - i)
            if chunk <= 0:
                continue

            signal = Signal(
                symbol=pos.symbol,
                side=Side.SELL,
                strategy=pos.strategy,
                price=pos.current_price,
                quantity=chunk,
                confidence=1.0,
                timestamp=datetime.now(),
            )
            order = self._orders.signal_to_order(signal, chunk)
            result = await self._orders.submit(order)

            if result.status == OrderStatus.REJECTED:
                if total_sold == 0:
                    self.positions.remove(pos.symbol)
                    if self._trade_repo:
                        try:
                            self._trade_repo.close_position(pos.symbol)
                        except Exception:
                            pass
                if "잔고" in result.reject_reason or "매매불가" in result.reject_reason:
                    self._untradeable.add(pos.symbol)
                    logger.warning(
                        "engine.ghost_position_blocked", symbol=pos.symbol, reason=reason
                    )
                else:
                    self._rejected_symbols[pos.symbol] = time.monotonic()
                    logger.warning(
                        "engine.force_close_rejected", symbol=pos.symbol, reason=reason
                    )
                break

            if result.status == OrderStatus.FILLED:
                self.positions.update_on_fill(result)
                total_pnl += (result.price - pos.avg_price) * result.quantity
                total_sold += result.quantity
                remaining -= result.quantity
                if self._bus:
                    await self._bus.emit(
                        "order.filled",
                        {
                            "symbol": result.symbol,
                            "side": result.side.value,
                            "price": str(result.price),
                            "qty": result.quantity,
                            "strategy": reason or result.strategy,
                        },
                    )

            if remaining > 0 and i < splits - 1:
                await asyncio.sleep(_SPLIT_SELL_DELAY)

        if total_sold > 0:
            self._position_pnl[pos.symbol] = self._position_pnl.get(pos.symbol, Decimal("0")) + total_pnl
            self._trade_reasons[pos.symbol] = reason
            remaining_pos = self.positions.get(pos.symbol)
            if remaining_pos is None or remaining_pos.quantity == 0:
                strat = self._position_strategy.pop(pos.symbol, pos.strategy)
                total = self._position_pnl.pop(pos.symbol, Decimal("0"))
                self._performance.record_trade(strat, total, symbol=pos.symbol)
                self._closed_symbols[pos.symbol] = time.monotonic()
                if self._trade_repo:
                    try:
                        self._trade_repo.close_position(pos.symbol)
                    except Exception:
                        pass
            logger.info(
                "engine.position_force_closed",
                symbol=pos.symbol, reason=reason,
                splits=splits, sold=total_sold,
            )
            if self._bus:
                await self._bus.emit(
                    "signal.generated",
                    {
                        "symbol": pos.symbol,
                        "side": "sell",
                        "strategy": reason or pos.strategy,
                        "price": str(pos.current_price),
                        "confidence": 1.0,
                    },
                )
                await self._bus.emit(
                    "position.closed",
                    {
                        "symbol": pos.symbol,
                        "qty": total_sold,
                        "reason": reason,
                    },
                )
