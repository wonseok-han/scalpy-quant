import asyncio
from typing import Any

import structlog
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from scalpy.config import settings
from scalpy.dashboard.sse import SSEManager
from scalpy.dashboard.state import DashboardState
from scalpy.events.bus import EventBus
from scalpy.strategy.registry import StrategyRegistry

logger = structlog.get_logger()

_ETF_PREFIXES = ("KODEX", "TIGER", "KBSTAR", "RISE", "ARIRANG", "SOL", "ACE", "HANARO", "KOSEF", "PLUS")

_STRAT_INTERNAL_ATTRS = frozenset({
    "name", "display_name", "enabled",
    "cooldown_ticks",
})


def _is_etf(symbol: str, name: str) -> bool:
    if symbol and not symbol[0].isdigit():
        return True
    if symbol.startswith("9"):
        return True
    if any(name.startswith(p) for p in _ETF_PREFIXES):
        return True
    return False

router = APIRouter(prefix="/api")

_state: DashboardState | None = None
_sse: SSEManager | None = None
_bus: EventBus | None = None
_engine_ref: Any = None
_stream_ref: Any = None
_registry_ref: StrategyRegistry | None = None
_trade_repo_ref: Any = None
_trading_started: bool = False
_initial_start_done: bool = False
_quant_rescan_task: asyncio.Task[None] | None = None
_perf_cache: dict[str, dict] = {}
_perf_sync_task: asyncio.Task[None] | None = None
_PERF_SYNC_INTERVAL = 120
_bus_subscriptions: list[tuple[str, Any]] = []

_SSE_EVENTS = [
    "tick.received", "order.filled", "signal.generated",
    "position.opened", "position.closed", "position.updated",
    "screening.completed", "engine.started", "engine.stopped",
    "engine.daily_init",
]


def init_routes(
    state: DashboardState,
    sse: SSEManager,
    engine: Any,
    bus: EventBus | None = None,
    stream: Any = None,
    registry: StrategyRegistry | None = None,
    trade_repo: Any = None,
) -> None:
    global _state, _sse, _bus, _engine_ref, _stream_ref, _registry_ref, _trade_repo_ref
    global _bus_subscriptions, _perf_sync_task, _trading_started, _perf_cache

    old_bus = _bus
    if old_bus:
        for event, handler in _bus_subscriptions:
            try:
                old_bus.unsubscribe(event, handler)
            except ValueError:
                pass
    _bus_subscriptions = []

    _state = state
    _sse = sse
    _bus = bus
    _engine_ref = engine
    _stream_ref = stream
    _registry_ref = registry
    _trade_repo_ref = trade_repo
    _trading_started = False
    _perf_cache = {}

    if engine and trade_repo:
        engine.set_trade_repo(trade_repo)
        engine._performance.set_repo(trade_repo)

    if trade_repo and engine and _perf_sync_task is None:
        _perf_sync_task = asyncio.create_task(_perf_sync_loop())

    if bus:
        for event in _SSE_EVENTS:
            bus.subscribe(event, _on_state_change)
            _bus_subscriptions.append((event, _on_state_change))
        bus.subscribe("order.filled", _on_order_filled)
        _bus_subscriptions.append(("order.filled", _on_order_filled))
        bus.subscribe("engine.daily_init", _on_daily_init)
        _bus_subscriptions.append(("engine.daily_init", _on_daily_init))

    logger.info("routes.initialized", engine=engine is not None, stream=stream is not None)


def _on_state_change(data: dict[str, Any]) -> None:
    if _sse is None:
        return
    _sse.broadcast("state", _build_sse_state())


def _on_order_filled(data: dict[str, Any]) -> None:
    if not _trade_repo_ref or not _engine_ref:
        return
    asyncio.create_task(_sync_trades_now())


def _on_daily_init(data: dict[str, Any]) -> None:
    if not _trading_started:
        return
    asyncio.create_task(_daily_rescan(data.get("date", "")))


async def _sync_trades_now() -> None:
    global _perf_cache
    try:
        broker = _engine_ref._broker
        broker._last_ccld_first_page = -1
        trades = await broker.get_trade_history()
        if trades:
            reasons = getattr(_engine_ref, "_trade_reasons", {})
            _trade_repo_ref.sync_trades(trades, reason_map=reasons, market="kr")
        if not settings.get("mock", True):
            profit_records = await broker.get_period_pnl()
            if profit_records and _trade_repo_ref:
                _trade_repo_ref.correct_pnl_from_api(profit_records)
        await _refresh_pnl_cache()
        if _trade_repo_ref:
            _perf_cache = _trade_repo_ref.get_strategy_performance(market="kr")
        if _sse:
            _sse.broadcast("state", _build_sse_state())
            if _perf_cache:
                _sse.broadcast("performance", _perf_cache)
    except Exception as e:
        logger.error("routes.trade_sync_failed", error=str(e))


async def _daily_rescan(date: str) -> None:
    global _last_quant_scan, _initial_start_done
    if not _initial_start_done:
        _initial_start_done = True
        logger.debug("routes.daily_rescan_skip_initial", date=date)
        return
    logger.info("routes.daily_rescan_start", date=date)

    _last_quant_scan = []
    if _state:
        _state.last_daily_pnl = ""
        _state.last_daily_fees = ""

    quant_on = settings.get("strategies.quant_enabled", [])
    if quant_on:
        try:
            quant_symbols = await _quant_start()
            if quant_symbols and _stream_ref:
                held = [p.symbol for p in _engine_ref.positions.all()] if _engine_ref else []
                await _stream_ref.update_subscriptions(list(set(quant_symbols) | set(held)))
            logger.info("routes.daily_rescan.quant_done", symbols=quant_symbols)
        except Exception as e:
            logger.error("routes.daily_rescan.quant_failed", error=str(e))

    if _sse:
        _sse.broadcast("state", _build_sse_state())
    logger.info("routes.daily_rescan_done", date=date)


async def _perf_sync_loop() -> None:
    """주기적으로 전략 성과를 DB에서 계산하여 캐시."""
    global _perf_cache
    while True:
        await asyncio.sleep(_PERF_SYNC_INTERVAL)
        try:
            if _trade_repo_ref:
                _perf_cache = _trade_repo_ref.get_strategy_performance(market="kr")
                if _sse:
                    _sse.broadcast("performance", _perf_cache)
        except Exception as e:
            logger.warning("routes.perf_sync_failed", error=str(e))


async def _refresh_pnl_cache() -> None:
    """실거래: KIS 기간별손익일별합산(TTTC8708R)에서 당일 실현손익/수수료를 캐싱."""
    if not _state or not _engine_ref:
        return
    broker = _engine_ref._broker
    mock = settings.get("mock", True)
    if mock:
        return
    try:
        summary = await broker.get_daily_profit_summary()
        if not summary:
            return
        _state.last_daily_pnl = str(summary.get("tot_rlzt_pfls", 0))
        _state.last_daily_fees = str(
            summary.get("tot_fee", 0) + summary.get("tot_tltx", 0)
        )
    except Exception as e:
        logger.warning("routes.pnl_cache_failed", error=str(e))


def _build_sse_state() -> dict[str, Any]:
    """SSE 푸시용 — API 호출 없이 메모리 캐시만 사용."""
    if _engine_ref is None:
        return {
            "status": {
                "running": False, "balance": "-", "prev_balance": "",
                "invested": "0", "available_balance": "-", "pending_order_count": 0,
                "daily_pnl": "0", "total_fees": "0", "trade_count": 0,
                "last_tick_at": "", "position_count": 0, "screening_count": 0,
            },
            "positions": [],
            "screening": {"symbols": [], "names": {}},
            "market_condition": {},
        }

    names = _state.symbol_names if _state else {}

    positions: list[dict[str, Any]] = []
    for p in _engine_ref.positions.all():
        pnl_pct = getattr(p, '_pnl_pct', 0.0)
        positions.append({
            "symbol": p.symbol,
            "name": names.get(p.symbol, p.symbol),
            "quantity": p.quantity,
            "avg_price": str(p.avg_price),
            "current_price": str(p.current_price),
            "pnl": str(p.unrealized_pnl),
            "pnl_pct": round(pnl_pct, 2),
            "strategy": p.strategy,
        })

    daily_pnl = "0"
    total_fees = "0"
    trade_count = 0
    if _state and _state.last_daily_pnl:
        daily_pnl = _state.last_daily_pnl
        total_fees = _state.last_daily_fees or "0"
    if _trade_repo_ref:
        try:
            if not daily_pnl or daily_pnl == "0":
                daily_pnl = str(_trade_repo_ref.get_daily_pnl(market="kr"))
            if not total_fees or total_fees == "0":
                total_fees = str(_trade_repo_ref.get_daily_fees(market="kr"))
            trade_count = _trade_repo_ref.get_daily_trade_count(market="kr")
        except Exception:
            pass

    invested = 0
    available_balance = "-"
    pending_order_count = 0
    for p in _engine_ref.positions.all():
        invested += int(p.avg_price * p.quantity)
    try:
        cash = _engine_ref._cached_available_cash
        if cash is not None:
            available_balance = str(int(cash - _engine_ref._pending_buy_cost))
    except Exception:
        pass
    pending_order_count = len(_engine_ref.orders.get_pending())

    return {
        "status": {
            "running": _engine_ref._running,
            "balance": _state.last_api_balance if _state else "-",
            "prev_balance": _state.last_prev_balance if _state else "",
            "invested": str(invested),
            "available_balance": available_balance,
            "pending_order_count": pending_order_count,
            "daily_pnl": daily_pnl,
            "total_fees": total_fees,
            "trade_count": trade_count,
            "last_tick_at": _state.last_tick_at if _state else "",
            "position_count": len(positions),
            "screening_count": len(_state.screening_symbols) if _state else 0,
        },
        "positions": positions,
        "screening": {
            "symbols": _state.screening_symbols if _state else [],
            "names": _state.symbol_names if _state else {},
        },
        "market_condition": _state.market_condition if _state else {},
    }


@router.get("/health")
async def health() -> dict[str, Any]:
    return {
        "ok": True,
        "engine": _engine_ref is not None,
        "bus": _bus is not None,
        "stream": _stream_ref is not None,
        "trading_started": _trading_started,
    }


@router.get("/status")
async def get_status() -> dict[str, Any]:
    """엔진 상태 — 캐시 데이터만, 즉시 반환."""
    mock = settings.get("mock", True)
    if _engine_ref is None:
        return {
            "running": False, "balance": "-", "prev_balance": "",
            "daily_pnl": "0", "total_fees": "0", "trade_count": 0,
            "last_tick_at": "", "position_count": 0, "screening_count": 0,
            "mock": mock, "strategies": {}, "strategy_enabled": {},
            "trading_started": False, "market_condition": {},
        }

    strategy_names = {s.name: s.display_name for s in _registry_ref.all()} if _registry_ref else {}
    strategy_enabled = {s.name: s.enabled for s in _registry_ref.all()} if _registry_ref else {}
    pos_count = len(_engine_ref.positions.all())
    daily_pnl = "0"
    total_fees = "0"
    trade_count = 0
    if _state and _state.last_daily_pnl:
        daily_pnl = _state.last_daily_pnl
        total_fees = _state.last_daily_fees or "0"
    if _trade_repo_ref:
        try:
            if not daily_pnl or daily_pnl == "0":
                daily_pnl = str(_trade_repo_ref.get_daily_pnl(market="kr"))
            if not total_fees or total_fees == "0":
                total_fees = str(_trade_repo_ref.get_daily_fees(market="kr"))
            trade_count = _trade_repo_ref.get_daily_trade_count(market="kr")
        except Exception:
            pass

    return {
        "running": _engine_ref._running,
        "balance": _state.last_api_balance if _state else "-",
        "prev_balance": _state.last_prev_balance if _state else "",
        "daily_pnl": daily_pnl,
        "total_fees": total_fees,
        "trade_count": trade_count,
        "last_tick_at": _state.last_tick_at if _state else "",
        "position_count": pos_count,
        "screening_count": len(_state.screening_symbols) if _state else 0,
        "mock": mock,
        "strategies": strategy_names,
        "strategy_enabled": strategy_enabled,
        "trading_started": _trading_started,
        "market_condition": _state.market_condition if _state else {},
    }


@router.get("/positions")
async def get_positions() -> list[dict[str, Any]]:
    """보유 포지션 — 엔진 메모리에서 즉시 반환."""
    if _engine_ref is None:
        return []
    names = _state.symbol_names if _state else {}
    positions: list[dict[str, Any]] = []
    for p in _engine_ref.positions.all():
        pnl_pct = getattr(p, '_pnl_pct', 0.0)
        positions.append({
            "symbol": p.symbol,
            "name": names.get(p.symbol, p.symbol),
            "quantity": p.quantity,
            "avg_price": str(p.avg_price),
            "current_price": str(p.current_price),
            "pnl": str(p.unrealized_pnl),
            "pnl_pct": round(pnl_pct, 2),
            "strategy": p.strategy,
        })
    return positions


@router.get("/screening")
async def get_screening() -> dict[str, Any]:
    """스크리닝 결과 — 캐시 데이터만, 즉시 반환."""
    if not _state:
        return {"symbols": [], "names": {}, "next_scan_at": ""}
    return {
        "symbols": _state.screening_symbols,
        "names": _state.symbol_names,
        "next_scan_at": _state.next_scan_at,
    }


@router.get("/signals")
async def get_signals() -> list[dict[str, Any]]:
    """시그널 목록 — 캐시 데이터만, 즉시 반환."""
    return _state.signals_list() if _state else []


@router.get("/trades")
async def get_trades() -> list[dict[str, Any]]:
    """당일 거래내역 — DB에서 즉시 반환."""
    if not _trade_repo_ref:
        return []
    try:
        trades = _trade_repo_ref.get_trades_today(market="kr")
        if trades and _state:
            for t in trades:
                name = t.get("name", "")
                if name:
                    _state.symbol_names[t["symbol"]] = name
        return trades
    except Exception as e:
        logger.error("api.trades_failed", error=str(e))
        return []


@router.get("/period-pnl")
async def get_period_pnl() -> list[dict[str, Any]]:
    """기간손익현황 — KIS API (실거래만)."""
    broker = _engine_ref._broker if _engine_ref else None
    if not broker or not broker._connected:
        return []
    try:
        return await broker.get_period_pnl()
    except Exception as e:
        logger.error("api.period_pnl_failed", error=str(e))
        return []


@router.get("/balance")
async def get_balance() -> dict[str, Any]:
    """KIS API 잔고 조회 — 느림."""
    broker = _engine_ref._broker if _engine_ref else None
    if not broker or not broker._connected:
        return {}
    try:
        balance = await asyncio.to_thread(lambda: broker._api._get_kr_total_balance())
        summary = balance.outputs[1][0]
        api_balance = str(int(summary.get("tot_evlu_amt") or summary.get("dnca_tot_amt", "0")))
        prev_balance = str(int(summary.get("bfdy_tot_asst_evlu_amt", "0")))
        if _state:
            _state.last_api_balance = api_balance
            _state.last_prev_balance = prev_balance
        await _refresh_pnl_cache()
        return {"balance": api_balance, "prev_balance": prev_balance}
    except Exception as e:
        logger.error("api.balance_failed", error=str(e))
        return {}


@router.post("/actions/liquidate")
async def liquidate_all() -> dict[str, Any]:
    if _engine_ref is None:
        return {"success": False, "error": "engine not available"}
    try:
        results = []
        for pos in list(_engine_ref.positions.all()):
            try:
                await _engine_ref._force_close(pos)
                removed = _engine_ref.positions.get(pos.symbol) is None
                results.append({"symbol": pos.symbol, "status": "closed" if removed else "pending"})
            except Exception as e:
                results.append({"symbol": pos.symbol, "status": "failed", "error": str(e)})
        logger.info("dashboard.liquidate_all", results=results)
        return {"success": True, "results": results}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/actions/liquidate/{symbol}")
async def liquidate_one(symbol: str) -> dict[str, Any]:
    if _engine_ref is None:
        return {"success": False, "error": "engine not available"}
    pos = _engine_ref.positions.get(symbol)
    if pos is None:
        return {"success": False, "error": "position not found"}
    try:
        await _engine_ref._force_close(pos)
        logger.info("dashboard.liquidate_one", symbol=symbol)
        return {"success": True, "symbol": symbol}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/actions/cancel-all-orders")
async def cancel_all_orders() -> dict[str, Any]:
    if _engine_ref is None:
        return {"success": False, "error": "engine not available"}
    broker = _engine_ref._broker
    if not broker._connected:
        return {"success": False, "error": "broker not connected"}
    try:
        count = await broker.cancel_all_orders()
        if count > 0:
            await _engine_ref.sync_positions()
            if _state:
                _state.symbol_names.update(broker._position_names)
        logger.info("dashboard.cancel_all_orders", count=count)
        return {"success": True, "cancelled": count}
    except Exception as e:
        return {"success": False, "error": str(e)}


_QUANT_STRATEGIES = {"momentum", "mean_reversion", "factor", "ichimoku", "volume_spike"}


def _apply_strategies() -> None:
    if not _registry_ref:
        return
    quant_on = set(settings.get("strategies.quant_enabled", []))
    for s in _registry_ref.all():
        s.enabled = s.name in quant_on
    logger.info("dashboard.strategies_applied", enabled=[s.name for s in _registry_ref.enabled()])


_last_quant_scan: list[dict] = []


async def _build_universe(quant_cfg: dict, max_price: int = 0) -> tuple[list[str], dict[str, str], dict[str, dict]]:
    """전체 시장에서 1차 필터링된 universe, 종목명 맵, 장중 실시간 데이터 반환."""
    from scalpy.screening.quant_screener import scan_market_universe

    min_cr = quant_cfg.get("min_change_rate", -2.0)
    max_cr = quant_cfg.get("max_change_rate", 15.0)
    min_vol = quant_cfg.get("min_avg_volume", 500_000)
    top_n = quant_cfg.get("universe_size", 100)
    try:
        stocks = await asyncio.to_thread(
            scan_market_universe, min_vol, min_cr, max_cr, 0, top_n, max_price
        )
        symbols = [s["symbol"] for s in stocks]
        names = {s["symbol"]: s["name"] for s in stocks}
        live_data = {
            s["symbol"]: {"change_rate": s["change_rate"], "volume": s["volume"], "amount": s["amount"]}
            for s in stocks
        }
        logger.info("quant.market_universe", candidates=len(symbols))
        from scalpy.screening.quant_screener import get_market_condition
        if _state:
            _state.market_condition = get_market_condition()
        return symbols, names, live_data
    except Exception as e:
        logger.warning("quant.market_universe_failed", error=str(e))
        return [], {}, {}


async def _quant_start() -> list[str]:
    """Quant flow: market scan -> OHLCV fetch -> score -> prefill + start rescan loop."""
    global _last_quant_scan, _quant_rescan_task
    from scalpy.data.ohlcv import OhlcvRepository
    from scalpy.screening.quant_screener import QuantScreener

    db_url = settings.get("database_url", "")
    if not db_url:
        return []

    quant_cfg = settings.get("quant", {})

    max_price = 0
    if _engine_ref:
        try:
            bal = await _engine_ref._broker.get_balance()
            max_price = int(bal)
        except Exception:
            pass

    universe = list(quant_cfg.get("universe", []))
    names: dict[str, str] = {}
    live_data: dict[str, dict] = {}
    if not universe:
        universe, names, live_data = await _build_universe(quant_cfg, max_price=max_price)
    if not universe:
        broker = _engine_ref._broker if _engine_ref else None
        if broker:
            from scalpy.screening.quant_screener import get_warn_symbols
            top = await broker.get_top_volume_stocks(30)
            warned = get_warn_symbols()
            top = [s for s in top if not _is_etf(s.get("symbol", ""), s.get("name", "")) and s.get("symbol", "") not in warned]
            universe = [s["symbol"] for s in top]
            names = {s["symbol"]: s.get("name", "") for s in top}
    if not universe:
        return []

    if _state and names:
        _state.symbol_names.update({k: v for k, v in names.items() if v})

    ohlcv_repo = OhlcvRepository(db_url)
    ohlcv_repo.create_tables()
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
    held = [p.symbol for p in _engine_ref.positions.all()] if _engine_ref else []
    selected = screener.scan(universe, held_symbols=held, live_data=live_data or None)

    all_names = {**names, **(_state.symbol_names if _state else {})}
    results = screener.get_last_scan()
    for r in results:
        r["name"] = all_names.get(r["symbol"], r["symbol"])
    _last_quant_scan = results

    if selected and _engine_ref:
        for sym in selected:
            candles = ohlcv_repo.get_candles(sym, interval="1d", limit=60)
            if candles:
                _engine_ref.prefill_strategies(sym, candles)
        await _engine_ref.prefill_minute_candles(selected)

    rescan_min = quant_cfg.get("rescan_interval_minutes", 30)
    if rescan_min > 0 and (not _quant_rescan_task or _quant_rescan_task.done()):
        _quant_rescan_task = asyncio.create_task(
            _quant_rescan_loop(ohlcv_repo, rescan_min)
        )

    return selected


async def _quant_rescan_loop(
    ohlcv_repo: Any,
    interval_minutes: int,
) -> None:
    """주기적으로 퀀트 스크리닝을 재실행하여 종목 교체."""
    global _last_quant_scan
    from scalpy.screening.quant_screener import QuantScreener

    while _trading_started:
        await asyncio.sleep(interval_minutes * 60)
        if not _trading_started:
            break
        try:
            quant_cfg = settings.get("quant", {})

            max_price = 0
            if _engine_ref:
                try:
                    bal = await _engine_ref._broker.get_balance()
                    max_price = int(bal)
                except Exception:
                    pass

            universe = list(quant_cfg.get("universe", []))
            names: dict[str, str] = {}
            live_data: dict[str, dict] = {}
            if not universe:
                universe, names, live_data = await _build_universe(quant_cfg, max_price=max_price)
            if not universe and _engine_ref:
                universe = list(_engine_ref._active_symbols)
            if not universe:
                continue

            if _state and names:
                _state.symbol_names.update({k: v for k, v in names.items() if v})

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
            held = [p.symbol for p in _engine_ref.positions.all()] if _engine_ref else []
            new_symbols = screener.scan(universe, held_symbols=held, live_data=live_data or None)

            all_names = {**names, **(_state.symbol_names if _state else {})}
            results = screener.get_last_scan()
            for r in results:
                r["name"] = all_names.get(r["symbol"], r["symbol"])
            _last_quant_scan = results

            if new_symbols and _engine_ref:
                if _stream_ref:
                    await _stream_ref.update_subscriptions(new_symbols)
                await _engine_ref.update_symbols(new_symbols)
                for sym in new_symbols:
                    candles = ohlcv_repo.get_candles(sym, interval="1d", limit=60)
                    if candles:
                        _engine_ref.prefill_strategies(sym, candles)
                await _engine_ref.prefill_minute_candles(new_symbols)
                logger.info("quant_rescan.updated", symbols=new_symbols)
                if _bus:
                    await _bus.emit("screening.completed", {
                        "symbols": new_symbols,
                        "names": {s: all_names.get(s, s) for s in new_symbols},
                    })
        except Exception as e:
            logger.warning("quant_rescan.failed", error=str(e))


@router.post("/actions/start")
async def start_engine() -> dict[str, Any]:
    global _trading_started
    if _engine_ref is None:
        return {"success": False, "error": "engine not available"}
    if _trading_started:
        return {"success": True}
    if _stream_ref and _stream_ref._stopping:
        return {"success": False, "error": "stop in progress"}
    try:
        if not _engine_ref._broker._connected:
            await _engine_ref._broker.connect()
        cancelled = await _engine_ref._broker.cancel_all_orders()
        if cancelled > 0:
            logger.info("dashboard.cleared_unfilled_orders", count=cancelled)
        await _engine_ref.sync_positions()
        _engine_ref._running = True
        _engine_ref.start_background_loops()

        _trading_started = True
        _apply_strategies()
        quant_on = settings.get("strategies.quant_enabled", [])

        symbols: list[str] = []

        if quant_on:
            quant_symbols = await _quant_start()
            symbols.extend(quant_symbols)

        if not symbols:
            symbols = list(settings.get("trading.symbols", ["005930"]))

        if _stream_ref:
            held = [p.symbol for p in _engine_ref.positions.all()]
            start_symbols = list(set(symbols) | set(held))
            await _stream_ref.start(start_symbols)

        await _engine_ref.update_symbols(symbols)

        if _bus:
            await _bus.emit("engine.started")
        logger.info("dashboard.engine_started", symbols=symbols)
        resp: dict[str, Any] = {"success": True, "symbols": symbols}
        if _last_quant_scan:
            resp["scan"] = _last_quant_scan
        return resp
    except Exception as e:
        _trading_started = False
        logger.error("dashboard.start_failed", error=str(e))
        return {"success": False, "error": str(e)}


@router.post("/actions/stop")
async def stop_engine() -> dict[str, Any]:
    global _trading_started, _quant_rescan_task, _initial_start_done
    if _engine_ref is None:
        return {"success": False}
    if not _trading_started:
        return {"success": False, "error": "already stopped"}
    if _stream_ref and _stream_ref._stopping:
        return {"success": False, "error": "stop in progress"}
    _engine_ref._running = False
    _engine_ref.stop_background_loops()
    _trading_started = False
    _initial_start_done = False
    if _quant_rescan_task and not _quant_rescan_task.done():
        _quant_rescan_task.cancel()
    _quant_rescan_task = None

    if _stream_ref:
        await _stream_ref.stop()
    if _bus:
        await _bus.emit("engine.stopped")
    logger.info("dashboard.engine_stopped")
    return {"success": True}


@router.post("/actions/rescan")
async def rescan() -> dict[str, Any]:
    if not _trading_started:
        return {"success": False, "error": "trading not started"}
    try:
        quant_symbols = await _quant_start()
        if quant_symbols and _stream_ref:
            held = [p.symbol for p in _engine_ref.positions.all()] if _engine_ref else []
            await _stream_ref.update_subscriptions(list(set(quant_symbols) | set(held)))
        if _engine_ref and quant_symbols:
            await _engine_ref.update_symbols(quant_symbols)
        return {"success": True, "symbols": quant_symbols}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/market-condition")
async def get_market_condition_api() -> dict[str, Any]:
    """장세 판단 — 스크리닝 전에도 독립 호출 가능."""
    if _state and _state.market_condition:
        return _state.market_condition
    from scalpy.screening.quant_screener import get_market_condition, scan_market_universe
    quant_cfg = settings.get("quant", {})
    min_vol = quant_cfg.get("min_avg_volume", 500_000)
    min_cr = quant_cfg.get("min_change_rate", -2.0)
    max_cr = quant_cfg.get("max_change_rate", 15.0)
    top_n = quant_cfg.get("universe_size", 100)
    try:
        await asyncio.to_thread(scan_market_universe, min_vol, min_cr, max_cr, 0, top_n)
        mc = get_market_condition()
        if _state:
            _state.market_condition = mc
        return mc
    except Exception as e:
        return {"error": str(e)}


@router.post("/strategies/{name}/toggle")
async def toggle_strategy(name: str) -> dict[str, Any]:
    if _registry_ref is None:
        return {"success": False, "error": "registry not available"}

    result = _registry_ref.toggle(name)
    if result is None:
        return {"success": False, "error": "strategy not found"}
    quant_on = [s.name for s in _registry_ref.all() if s.enabled and s.name in _QUANT_STRATEGIES]
    settings.set("strategies.quant_enabled", quant_on)
    if _sse:
        _sse.broadcast("state", _build_sse_state())
    return {"success": True, "name": name, "enabled": result}


@router.post("/ohlcv/fetch")
async def fetch_ohlcv(body: dict[str, Any] | None = None) -> dict[str, Any]:
    db_url = settings.get("database_url", "")
    if not db_url:
        return {"success": False, "error": "database_url not configured"}

    from scalpy.data.ohlcv import OhlcvRepository

    repo = OhlcvRepository(db_url)
    repo.create_tables()

    params = body or {}
    symbols = params.get("symbols", [])
    interval = params.get("interval", "1d")
    period = params.get("period")
    if not symbols and _engine_ref:
        symbols = list(_engine_ref._active_symbols)
    if not symbols:
        symbols = settings.get("trading.symbols", ["005930"])

    total = repo.bulk_fetch(symbols, interval=interval, period=period)
    return {"success": True, "symbols": symbols, "interval": interval, "rows_added": total}


@router.get("/ohlcv/{symbol}")
async def get_ohlcv(symbol: str, interval: str = "1d", limit: int = 60) -> dict[str, Any]:
    db_url = settings.get("database_url", "")
    if not db_url:
        return {"data": []}

    from scalpy.data.ohlcv import OhlcvRepository

    repo = OhlcvRepository(db_url)
    data = repo.get_candles(symbol, interval=interval, limit=limit)
    return {"data": data}


@router.get("/quant/scan")
async def quant_scan(refresh: bool = False) -> dict[str, Any]:
    global _last_quant_scan
    if _last_quant_scan and not refresh:
        return {"data": _last_quant_scan, "cached": True}

    db_url = settings.get("database_url", "")
    if not db_url:
        return {"data": [], "error": "database_url not configured"}

    from scalpy.data.ohlcv import OhlcvRepository
    from scalpy.screening.quant_screener import QuantScreener

    quant_cfg = settings.get("quant", {})
    ohlcv_repo = OhlcvRepository(db_url)

    scan_max_price = 0
    if _engine_ref:
        try:
            bal = await _engine_ref._broker.get_balance()
            scan_max_price = int(bal)
        except Exception:
            pass

    universe = list(quant_cfg.get("universe", []))
    names: dict[str, str] = {}
    if not universe:
        universe, names, _live = await _build_universe(quant_cfg, max_price=scan_max_price)
    if not universe and _engine_ref:
        universe = list(_engine_ref._active_symbols)
    if not universe:
        universe = settings.get("trading.symbols", ["005930"])

    if _state and names:
        _state.symbol_names.update({k: v for k, v in names.items() if v})

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
    held = [p.symbol for p in _engine_ref.positions.all()] if _engine_ref else []
    screener.scan(universe, held_symbols=held)
    results = screener.get_last_scan()

    all_names = {**names, **(_state.symbol_names if _state else {})}
    for r in results:
        r["name"] = all_names.get(r["symbol"], r["symbol"])
    _last_quant_scan = results

    return {"data": results}


@router.get("/performance")
async def performance() -> dict[str, Any]:
    global _perf_cache
    if not _engine_ref:
        return {"data": {}}
    if _perf_cache:
        return {"data": _perf_cache}
    if _trade_repo_ref:
        try:
            _perf_cache = _trade_repo_ref.get_strategy_performance(market="kr")
            return {"data": _perf_cache}
        except Exception:
            pass
    return {"data": _engine_ref._performance.all_stats()}


@router.get("/performance/history")
async def performance_history() -> dict[str, Any]:
    if not _trade_repo_ref:
        return {"data": []}
    try:
        return {"data": _trade_repo_ref.get_daily_performance_history(market="kr")}
    except Exception:
        return {"data": []}


@router.get("/quant/config")
async def quant_config() -> dict[str, Any]:
    quant = settings.get("quant", {})
    strat_configs = settings.get("strategies", {})
    strats = {}
    for s in (_registry_ref.all() if _registry_ref else []):
        if s.name not in _QUANT_STRATEGIES:
            continue
        params = dict(strat_configs.get(s.name, {}))
        params["enabled"] = s.enabled
        params["display_name"] = s.display_name
        params["stop_loss_ratio"] = s.stop_loss_ratio
        strats[s.name] = params
    return {
        "quant": dict(quant),
        "strategies": strats,
    }


@router.get("/settings")
async def get_settings() -> dict[str, Any]:
    trading = settings.get("trading", {})
    quant = settings.get("quant", {})
    risk = {}
    if _engine_ref:
        r = _engine_ref._risk
        risk = {
            "stop_loss_ratio": float(r.stop_loss_ratio),
            "max_position_size": r.max_position_size,
            "max_open_positions": r.max_open_positions,
            "max_position_ratio": r.max_position_ratio,
            "trailing_activate_ratio": float(r.trailing_activate_ratio),
            "trailing_stop_ratio": float(r.trailing_stop_ratio),
            "profit_protect_activate": float(r.profit_protect_activate),
            "profit_protect_ratio": float(r.profit_protect_ratio),
        }
    strats = {}
    if _registry_ref:
        for s in _registry_ref.all():
            params = {}
            for k in vars(s):
                if k.startswith("_") or k in _STRAT_INTERNAL_ATTRS:
                    continue
                v = getattr(s, k)
                if isinstance(v, (int, float, str, bool)):
                    params[k] = v
            strats[s.name] = {
                "display_name": s.display_name,
                "enabled": s.enabled,
                "params": params,
            }
    return {
        "trading": dict(trading),
        "quant": dict(quant),
        "risk": risk,
        "strategies": strats,
    }


@router.post("/settings")
async def update_settings(body: dict[str, Any]) -> dict[str, Any]:
    from decimal import Decimal
    applied = []

    if "trading" in body:
        t = body["trading"]
        for k, v in t.items():
            settings.set(f"trading.{k}", v)
        applied.append("trading")

    if "quant" in body:
        q = body["quant"]
        for k, v in q.items():
            settings.set(f"quant.{k}", v)
        applied.append("quant")

    if "risk" in body and _engine_ref:
        r = body["risk"]
        rm = _engine_ref._risk
        if r.get("stop_loss_ratio") is not None:
            rm.stop_loss_ratio = Decimal(str(r["stop_loss_ratio"]))
        if r.get("max_position_size") is not None:
            rm.max_position_size = int(r["max_position_size"])
        if r.get("max_open_positions") is not None:
            rm.max_open_positions = int(r["max_open_positions"])
        if r.get("max_position_ratio") is not None:
            rm.max_position_ratio = float(r["max_position_ratio"])
        if r.get("trailing_activate_ratio") is not None:
            rm.trailing_activate_ratio = Decimal(str(r["trailing_activate_ratio"]))
        if r.get("trailing_stop_ratio") is not None:
            rm.trailing_stop_ratio = Decimal(str(r["trailing_stop_ratio"]))
        if r.get("profit_protect_activate") is not None:
            rm.profit_protect_activate = Decimal(str(r["profit_protect_activate"]))
        if r.get("profit_protect_ratio") is not None:
            rm.profit_protect_ratio = Decimal(str(r["profit_protect_ratio"]))
        applied.append("risk")

    if "strategies" in body and _registry_ref:
        for name, cfg in body["strategies"].items():
            s = _registry_ref.get(name)
            if not s:
                continue
            if "enabled" in cfg:
                s.enabled = bool(cfg["enabled"])
            params = cfg.get("params", {})
            if params:
                s.configure(params)
        quant_on = [s.name for s in _registry_ref.all() if s.enabled and s.name in _QUANT_STRATEGIES]
        settings.set("strategies.quant_enabled", quant_on)
        applied.append("strategies")

    if _sse:
        _sse.broadcast("state", _build_sse_state())
    return {"success": True, "applied": applied}


@router.post("/settings/persist")
async def persist_settings() -> dict[str, Any]:
    """현재 런타임 설정을 settings.toml에 기록한다 (주석 보존)."""
    import tomlkit
    from scalpy.config import _PROJECT_ROOT

    toml_path = _PROJECT_ROOT / "config" / "settings.toml"
    try:
        doc = tomlkit.parse(toml_path.read_text())
    except Exception as e:
        return {"success": False, "error": f"TOML 읽기 실패: {e}"}

    d = doc.setdefault("default", {})

    # trading
    trading = d.setdefault("trading", {})
    for k in ("auto_start", "symbols", "max_position_size",
              "max_position_ratio", "max_open_positions",
              "stop_loss_ratio",
              "trailing_activate_ratio", "trailing_stop_ratio",
              "profit_protect_activate", "profit_protect_ratio"):
        v = settings.get(f"trading.{k}")
        if v is not None:
            trading[k] = v

    # quant
    quant = d.setdefault("quant", {})
    for k in ("max_stocks", "momentum_days", "min_avg_volume",
              "min_momentum", "ohlcv_refresh_minutes", "universe"):
        v = settings.get(f"quant.{k}")
        if v is not None:
            quant[k] = v

    # risk (from RiskManager runtime values)
    if _engine_ref and _engine_ref._risk:
        rm = _engine_ref._risk
        trading["stop_loss_ratio"] = float(rm.stop_loss_ratio)
        trading["max_position_size"] = rm.max_position_size
        trading["max_open_positions"] = rm.max_open_positions
        trading["max_position_ratio"] = rm.max_position_ratio
        trading["trailing_activate_ratio"] = float(rm.trailing_activate_ratio)
        trading["trailing_stop_ratio"] = float(rm.trailing_stop_ratio)
        trading["profit_protect_activate"] = float(rm.profit_protect_activate)
        trading["profit_protect_ratio"] = float(rm.profit_protect_ratio)

    # strategy enabled lists + params
    if _registry_ref:
        strats = d.setdefault("strategies", {})
        quant_on = [s.name for s in _registry_ref.all() if s.enabled and s.name in _QUANT_STRATEGIES]
        strats["quant_enabled"] = quant_on
        if "enabled" in strats:
            del strats["enabled"]
        for s in _registry_ref.all():
            sc = strats.setdefault(s.name, {})
            for k in vars(s):
                if k.startswith("_") or k in _STRAT_INTERNAL_ATTRS:
                    continue
                v = getattr(s, k)
                if isinstance(v, (int, float, str, bool)):
                    sc[k] = v

    try:
        toml_path.write_text(tomlkit.dumps(doc))
    except Exception as e:
        return {"success": False, "error": f"TOML 쓰기 실패: {e}"}

    return {"success": True}


@router.get("/events")
async def sse_stream() -> StreamingResponse:
    if _sse is None:
        return StreamingResponse(iter([]), media_type="text/event-stream")
    return StreamingResponse(
        _sse.stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )
