import asyncio
import contextlib
import signal

import structlog

from scalpy.broker.base import BaseBroker
from scalpy.broker.mock import MockBroker
from scalpy.config import settings
from scalpy.core.market import KR_MARKET, US_MARKET
from scalpy.data.stream import MarketDataStream
from scalpy.events import EventBus
from scalpy.strategy.factor import FactorStrategy
from scalpy.strategy.ichimoku import IchimokuStrategy
from scalpy.strategy.mean_reversion import MeanReversionStrategy
from scalpy.strategy.momentum import MomentumStrategy
from scalpy.strategy.registry import StrategyRegistry
from scalpy.strategy.volume_spike import VolumeSpikeStrategy
from scalpy.trading.engine import TradingEngine
from scalpy.trading.risk import RiskManager

logger = structlog.get_logger()


def build_registry() -> StrategyRegistry:
    registry = StrategyRegistry()
    all_strategies = [
        MomentumStrategy(),
        MeanReversionStrategy(),
        FactorStrategy(),
        IchimokuStrategy(),
        VolumeSpikeStrategy(),
    ]
    quant_enabled = set(settings.get("strategies.quant_enabled", ["momentum"]))
    for s in all_strategies:
        registry.register(s)
        if s.name not in quant_enabled:
            s.enabled = False

    strategy_config = {}
    for s in all_strategies:
        params = settings.get(f"strategies.{s.name}", {})
        if params:
            strategy_config[s.name] = dict(params)
    if strategy_config:
        registry.configure_all(strategy_config)

    return registry


def build_broker() -> BaseBroker:
    mock = settings.get("mock", True)
    app_key = settings.get("kis_app_key", "")
    market = settings.get("market", "kr")

    if not app_key:
        return MockBroker()

    if market == "us":
        from scalpy.broker.kis_overseas import KISOverseasBroker

        exchange = settings.get("us_trading.exchange", "NASD")
        return KISOverseasBroker(
            app_key=app_key,
            app_secret=settings.get("kis_app_secret", ""),
            account_no=settings.get("kis_account_no", ""),
            mock=mock,
            exchange=exchange,
            summer_time=settings.get("us_trading.summer_time", True),
        )

    from scalpy.broker.kis import KISBroker

    return KISBroker(
        app_key=app_key,
        app_secret=settings.get("kis_app_secret", ""),
        account_no=settings.get("kis_account_no", ""),
        mock=mock,
    )


def build_engine(registry: StrategyRegistry) -> tuple[TradingEngine, BaseBroker]:
    broker = build_broker()
    market = settings.get("market", "kr")
    trading_key = "us_trading" if market == "us" else "trading"
    trading = settings.get(trading_key, {})
    risk = RiskManager(
        stop_loss_ratio=trading.get("stop_loss_ratio", 0.02),
        max_position_size=trading.get("max_position_size", 100),
        max_open_positions=trading.get("max_open_positions", 3),
        max_position_ratio=trading.get("max_position_ratio", 0.3),
        stagnation_hours=trading.get("stagnation_hours", 0),
        stagnation_threshold=trading.get("stagnation_threshold", 0.005),
        trailing_activate_ratio=trading.get("trailing_activate_ratio", 0.01),
        trailing_stop_ratio=trading.get("trailing_stop_ratio", 0.01),
        profit_protect_activate=trading.get("profit_protect_activate", 0.008),
        profit_protect_ratio=trading.get("profit_protect_ratio", 0.005),
    )
    market_config = US_MARKET if market == "us" else KR_MARKET
    return TradingEngine(broker, registry, risk, market_config=market_config), broker


_TRADE_SYNC_INTERVAL = 60
_TRADE_SYNC_FILL_DELAY = 3


_current_sync_broker: "BaseBroker | None" = None
_current_sync_engine: "TradingEngine | None" = None
_trade_sync_nudge: asyncio.Event | None = None


def update_trade_sync_broker(broker: "BaseBroker", engine: "TradingEngine | None" = None) -> None:
    global _current_sync_broker, _current_sync_engine
    _current_sync_broker = broker
    _current_sync_engine = engine


def nudge_trade_sync() -> None:
    if _trade_sync_nudge:
        _trade_sync_nudge.set()


async def _trade_sync_loop(
    broker: "BaseBroker",
    trade_repo: "TradeRepository",
    stop_event: asyncio.Event,
    engine: "TradingEngine | None" = None,
    bus: "EventBus | None" = None,
) -> None:
    global _current_sync_broker, _current_sync_engine, _trade_sync_nudge
    _current_sync_broker = broker
    _current_sync_engine = engine
    _trade_sync_nudge = asyncio.Event()
    mock = settings.get("mock", True)
    while not stop_event.is_set():
        try:
            b = _current_sync_broker or broker
            e = _current_sync_engine or engine
            market = settings.get("market", "kr")
            trades = await b.get_trade_history()
            logger.debug("trade_sync.fetched", count=len(trades), market=market)
            if trades:
                reasons = getattr(e, "_trade_reasons", {}) if e else {}
                strats = getattr(e, "_symbol_strategy", {}) if e else {}
                count = trade_repo.sync_trades(trades, reason_map=reasons, market=market, strategy_map=strats)
                if count:
                    logger.info("trade_sync.new_trades", count=count)
                    if bus:
                        await bus.emit("trade_sync.updated", {"count": count, "market": market})
            if not mock:
                profit_records = await b.get_period_pnl()
                if profit_records:
                    trade_repo.correct_pnl_from_api(profit_records)
        except Exception as e:
            logger.error("trade_sync.error", error=str(e))

        _trade_sync_nudge.clear()
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=_TRADE_SYNC_INTERVAL)
            break
        except asyncio.TimeoutError:
            pass
        if _trade_sync_nudge.is_set():
            await asyncio.sleep(_TRADE_SYNC_FILL_DELAY)


_QUANT_STRATEGIES = {"momentum", "mean_reversion", "factor", "ichimoku", "volume_spike"}


def _apply_strategies(registry: StrategyRegistry) -> None:
    quant_on = set(settings.get("strategies.quant_enabled", []))
    for s in registry.all():
        s.enabled = s.name in quant_on
    logger.info("scalpy.strategies_applied", enabled=[s.name for s in registry.enabled()])


def _prefill_from_ohlcv(engine: TradingEngine, symbols: list[str]) -> None:
    db_url = settings.get("database_url", "")
    if not db_url:
        return
    try:
        from scalpy.data.ohlcv import OhlcvRepository

        repo = OhlcvRepository(db_url)
        repo.create_tables()
        for sym in symbols:
            candles = repo.get_candles(sym, interval="1d", limit=60)
            if candles:
                engine.prefill_strategies(sym, candles)
            else:
                logger.info("prefill.no_data", symbol=sym)
    except Exception as e:
        logger.warning("prefill.failed", error=str(e))


async def _start_trading(
    engine: TradingEngine,
    broker: BaseBroker,
    stream: MarketDataStream,
    bus: EventBus,
    stop_event: asyncio.Event,
) -> None:
    engine._running = True
    if bus:
        await bus.emit("engine.started")

    _apply_strategies(engine._registry)
    quant_on = settings.get("strategies.quant_enabled", [])

    symbols: list[str] = []

    if quant_on:
        quant_symbols = await _quant_scan(engine, stream, stop_event)
        symbols.extend(quant_symbols)

    if not symbols:
        market = settings.get("market", "kr")
        default_sym = ["AAPL"] if market == "us" else ["005930"]
        trading_key = "us_trading" if market == "us" else "trading"
        symbols = settings.get(f"{trading_key}.symbols", default_sym)

    if hasattr(stream, 'set_symbol_exchanges') and hasattr(engine._broker, '_symbol_exchange'):
        stream.set_symbol_exchanges(engine._broker._symbol_exchange)
    await stream.start(symbols)
    _prefill_from_ohlcv(engine, symbols)
    await engine.prefill_minute_candles(symbols)
    logger.info("scalpy.trading_started", symbols=symbols)


async def _quant_scan(
    engine: TradingEngine,
    stream: "MarketDataStream",
    stop_event: asyncio.Event,
) -> list[str]:
    db_url = settings.get("database_url", "")
    if not db_url:
        return []

    from scalpy.data.ohlcv import OhlcvRepository
    from scalpy.screening.quant_screener import QuantScreener

    quant_cfg = settings.get("quant", {})
    ohlcv_repo = OhlcvRepository(db_url)
    ohlcv_repo.create_tables()

    max_price = 0
    try:
        bal = await engine._broker.get_balance()
        max_price = int(bal)
    except Exception:
        pass

    universe = quant_cfg.get("universe", [])
    live_data: dict[str, dict] = {}
    if not universe:
        try:
            from scalpy.screening.quant_screener import scan_market_universe
            min_cr = quant_cfg.get("min_change_rate", -2.0)
            max_cr = quant_cfg.get("max_change_rate", 15.0)
            min_vol = quant_cfg.get("min_avg_volume", 500_000)
            top_n = quant_cfg.get("universe_size", 100)
            stocks = scan_market_universe(min_vol, min_cr, max_cr, 0, top_n, max_price)
            universe = [s["symbol"] for s in stocks]
            live_data = {
                s["symbol"]: {"change_rate": s["change_rate"], "volume": s["volume"], "amount": s["amount"]}
                for s in stocks
            }
            logger.info("quant.market_universe", candidates=len(universe))
        except Exception as e:
            logger.warning("quant.market_universe_failed", error=str(e))
    if not universe:
        try:
            from scalpy.screening.quant_screener import get_warn_symbols
            broker = engine._broker
            top_stocks = await broker.get_top_volume_stocks(30)
            warned = get_warn_symbols()
            universe = [s["symbol"] for s in top_stocks if s.get("symbol", "") not in warned]
        except Exception as e:
            logger.warning("quant.universe_fallback", error=str(e))
            universe = settings.get("trading.symbols", ["005930"])

    ohlcv_repo.bulk_fetch(universe, interval="1d", period="3mo")

    ichi_on = "ichimoku" in settings.get("strategies.quant_enabled", [])
    screener = QuantScreener(
        ohlcv_repo=ohlcv_repo,
        max_stocks=quant_cfg.get("max_stocks", 10),
        momentum_days=quant_cfg.get("momentum_days", 20),
        min_avg_volume=quant_cfg.get("min_avg_volume", 500_000),
        min_momentum=quant_cfg.get("min_momentum", 0.0),
        ichimoku_filter=ichi_on,
    )
    held = [p.symbol for p in engine.positions.all()]
    symbols = screener.scan(universe, held_symbols=held, live_data=live_data or None)

    scan_results = screener.get_last_scan()
    if scan_results:
        logger.info(
            "quant.top_picks",
            picks=[
                {"sym": s["symbol"], "mom": s["momentum"], "score": s["score"]}
                for s in scan_results[:5]
            ],
        )

    refresh = quant_cfg.get("ohlcv_refresh_minutes", 60)
    if refresh > 0:
        asyncio.create_task(_ohlcv_refresh_loop(ohlcv_repo, universe, refresh, stop_event))

    rescan_min = quant_cfg.get("rescan_interval_minutes", 30)
    if rescan_min > 0:
        asyncio.create_task(
            _quant_rescan_loop(engine, ohlcv_repo, stream, rescan_min, stop_event)
        )

    return symbols


async def _quant_rescan_loop(
    engine: TradingEngine,
    ohlcv_repo: "OhlcvRepository",
    stream: "MarketDataStream",
    interval_minutes: int,
    stop_event: asyncio.Event,
) -> None:
    from scalpy.screening.quant_screener import QuantScreener

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_minutes * 60)
            break
        except asyncio.TimeoutError:
            pass
        try:
            quant_cfg = settings.get("quant", {})

            max_price = 0
            try:
                bal = await engine._broker.get_balance()
                max_price = int(bal)
            except Exception:
                pass

            universe = list(quant_cfg.get("universe", []))
            live_data: dict[str, dict] = {}
            if not universe:
                try:
                    from scalpy.screening.quant_screener import scan_market_universe
                    stocks = scan_market_universe(
                        quant_cfg.get("min_avg_volume", 500_000),
                        quant_cfg.get("min_change_rate", -2.0),
                        quant_cfg.get("max_change_rate", 15.0),
                        0,
                        quant_cfg.get("universe_size", 100),
                        max_price,
                    )
                    universe = [s["symbol"] for s in stocks]
                    live_data = {
                        s["symbol"]: {"change_rate": s["change_rate"], "volume": s["volume"], "amount": s["amount"]}
                        for s in stocks
                    }
                except Exception:
                    universe = list(engine._active_symbols)

            if not universe:
                continue

            ohlcv_repo.bulk_fetch(universe, interval="1d")

            ichi_on = "ichimoku" in settings.get("strategies.quant_enabled", [])
            screener = QuantScreener(
                ohlcv_repo=ohlcv_repo,
                max_stocks=quant_cfg.get("max_stocks", 10),
                momentum_days=quant_cfg.get("momentum_days", 20),
                min_avg_volume=quant_cfg.get("min_avg_volume", 500_000),
                min_momentum=quant_cfg.get("min_momentum", 0.0),
                ichimoku_filter=ichi_on,
            )
            held = [p.symbol for p in engine.positions.all()]
            new_symbols = screener.scan(universe, held_symbols=held, live_data=live_data or None)
            if new_symbols:
                await stream.update_subscriptions(new_symbols)
                await engine.update_symbols(new_symbols)
                for sym in new_symbols:
                    candles = ohlcv_repo.get_candles(sym, interval="1d", limit=60)
                    if candles:
                        engine.prefill_strategies(sym, candles)
                await engine.prefill_minute_candles(new_symbols)
                logger.info("quant_rescan.updated", symbols=new_symbols)
        except Exception as e:
            logger.warning("quant_rescan.failed", error=str(e))


async def _ohlcv_refresh_loop(
    repo: "OhlcvRepository",
    symbols: list[str],
    interval_minutes: int,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_minutes * 60)
            break
        except asyncio.TimeoutError:
            pass
        try:
            count = repo.bulk_fetch(symbols, interval="1d")
            if count:
                logger.info("ohlcv_refresh.updated", rows=count)
        except Exception as e:
            logger.warning("ohlcv_refresh.failed", error=str(e))


async def run() -> None:
    registry = build_registry()
    engine, broker = build_engine(registry)
    mock = settings.get("mock", True)
    market = settings.get("market", "kr")
    hts_id = settings.get("kis_hts_id", "")

    if market == "us":
        from scalpy.data.us_stream import USMarketDataStream

        exchange = settings.get("us_trading.exchange", "NASD")
        stream = USMarketDataStream(
            app_key=settings.get("kis_app_key", ""),
            app_secret=settings.get("kis_app_secret", ""),
            mock=mock,
            hts_id=hts_id,
            exchange=exchange,
        )
    else:
        stream = MarketDataStream(
            app_key=settings.get("kis_app_key", ""),
            app_secret=settings.get("kis_app_secret", ""),
            mock=mock,
            hts_id=hts_id,
        )

    stream.on_tick(engine.on_tick)
    stream.on_orderbook(engine.on_orderbook)
    stream.on_fill(engine.on_fill_notice)
    stream.on_vi(engine.on_vi_event)

    bus = EventBus()
    engine.set_event_bus(bus)

    db_url = settings.get("database_url", "")
    if db_url:
        from scalpy.data.repository import TradeRepository
        trade_repo = TradeRepository(db_url, mock=mock)
        trade_repo.create_tables()
    else:
        trade_repo = None

    await broker.connect()
    cancelled = await broker.cancel_all_orders()
    if cancelled > 0:
        logger.info("scalpy.cleared_unfilled_orders", count=cancelled)
    await engine.sync_positions()
    await engine.get_cached_balance()
    engine.start_sync_loop()

    stop_event = asyncio.Event()
    dashboard_task = None

    trading_cfg = settings.get("trading", {})
    auto_start = trading_cfg.get("auto_start", True)

    # Dashboard
    dashboard_cfg = settings.get("dashboard", {})
    if dashboard_cfg.get("enabled", False):
        from scalpy.dashboard.server import start_dashboard_server
        from scalpy.dashboard.state import DashboardState

        dash_state = DashboardState()
        dash_state.register_handlers(bus)
        synced_names = getattr(broker, "_position_names", {})
        if synced_names:
            dash_state.symbol_names.update(synced_names)
        dashboard_task = start_dashboard_server(
            dash_state, bus, engine,
            host=dashboard_cfg.get("host", "0.0.0.0"),
            port=dashboard_cfg.get("port", 8080),
            stream=stream,
            registry=registry,
            trade_repo=trade_repo,
        )

    # Telegram
    telegram_cfg = settings.get("telegram", {})
    if telegram_cfg.get("enabled", False) and telegram_cfg.get("bot_token"):
        from scalpy.notification import TelegramNotifier

        notifier = TelegramNotifier(
            bot_token=telegram_cfg["bot_token"],
            chat_id=telegram_cfg.get("chat_id", ""),
        )
        notifier.register_handlers(bus)
        logger.info("scalpy.telegram_enabled")

    if trade_repo:
        asyncio.create_task(_trade_sync_loop(broker, trade_repo, stop_event, engine, bus=bus))
        logger.info("scalpy.trade_sync_started", interval=_TRADE_SYNC_INTERVAL)

    if auto_start:
        await _start_trading(engine, broker, stream, bus, stop_event)
    else:
        logger.info("scalpy.waiting_for_manual_start")

    logger.info(
        "scalpy.started",
        mock=mock,
        auto_start=auto_start,
        strategies=[s.name for s in registry.all()],
        dashboard=dashboard_cfg.get("enabled", False),
    )

    def _signal_handler() -> None:
        logger.info("scalpy.shutdown_requested")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    await stop_event.wait()

    async def _shutdown() -> None:
        if dashboard_task is not None:
            sse = getattr(dashboard_task, '_sse', None)
            if sse:
                sse.stop()
            server = getattr(dashboard_task, '_uvicorn_server', None)
            if server:
                server.should_exit = True
                try:
                    await asyncio.wait_for(dashboard_task, timeout=3)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    dashboard_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await dashboard_task

        await stream.stop()
        await engine.stop()

    try:
        await asyncio.wait_for(_shutdown(), timeout=5)
    except asyncio.TimeoutError:
        logger.warning("scalpy.shutdown_timeout")

    for task in asyncio.all_tasks():
        if task is not asyncio.current_task() and not task.done():
            task.cancel()

    logger.info("scalpy.stopped")


def main() -> None:
    from scalpy.logging import setup_logging

    log_level = settings.get("log_level", "DEBUG")
    setup_logging(log_dir="logs", level=log_level)

    logger.info("scalpy.initializing", version="0.1.0")
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run())


if __name__ == "__main__":
    main()
