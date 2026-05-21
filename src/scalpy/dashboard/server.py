import asyncio
from pathlib import Path
from typing import Any

import structlog
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from scalpy.config import settings
from scalpy.dashboard.sse import SSEManager
from scalpy.dashboard.state import DashboardState
from scalpy.events.bus import EventBus
from scalpy.strategy.registry import StrategyRegistry

logger = structlog.get_logger()

_STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
_MAIN_HTML = _STATIC_DIR / "quant.html"
_US_HTML = _STATIC_DIR / "us_quant.html"


def create_app(
    state: DashboardState,
    bus: EventBus,
    engine: Any,
    stream: Any = None,
    registry: StrategyRegistry | None = None,
    trade_repo: Any = None,
) -> FastAPI:
    app = FastAPI(title="Scalpy Quant", docs_url=None, redoc_url=None)

    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    sse = SSEManager(bus)
    app.state.sse = sse
    app.state.engine = engine
    app.state.stream = stream
    app.state.bus = bus
    app.state.registry = registry
    app.state.trade_repo = trade_repo
    app.state.current_market = settings.get("market", "kr")

    market = app.state.current_market
    other_state = DashboardState()
    if market == "us":
        app.state.us_state = state
        app.state.kr_state = other_state
    else:
        app.state.kr_state = state
        app.state.us_state = other_state

    from scalpy.dashboard.routes import init_routes, router
    from scalpy.dashboard.us_routes import init_us_routes, us_router

    if market == "us":
        init_us_routes(app.state.us_state, sse, engine, bus=bus, stream=stream, registry=registry, trade_repo=trade_repo)
        init_routes(app.state.kr_state, sse, None, trade_repo=trade_repo)
    else:
        init_routes(app.state.kr_state, sse, engine, bus=bus, stream=stream, registry=registry, trade_repo=trade_repo)
        init_us_routes(app.state.us_state, sse, None, trade_repo=trade_repo)

    app.include_router(router)
    app.include_router(us_router)

    try:
        from scalpy.backtest.quant_routes import router as qbt_router
        app.include_router(qbt_router)
    except ImportError:
        pass

    _bt_html = _STATIC_DIR / "quant_backtest.html"

    @app.get("/")
    async def index() -> HTMLResponse:
        m = app.state.current_market
        html = _US_HTML if m == "us" else _MAIN_HTML
        return HTMLResponse(html.read_text())

    @app.get("/kr")
    async def kr_page() -> HTMLResponse:
        return HTMLResponse(_MAIN_HTML.read_text())

    @app.get("/us")
    async def us_page() -> HTMLResponse:
        return HTMLResponse(_US_HTML.read_text())

    @app.get("/api/market")
    async def get_market() -> dict[str, str]:
        return {"market": app.state.current_market}

    @app.post("/api/market/switch")
    async def switch_market(body: dict) -> dict[str, Any]:
        target = body.get("market")
        if target not in ("kr", "us"):
            return {"success": False, "error": "invalid market"}

        if target == app.state.current_market:
            return {"success": True, "market": target}

        eng = app.state.engine
        if eng and eng._running:
            return {"success": False, "error": "엔진 실행 중. 먼저 정지하세요."}

        # 기존 스트림/브로커 정리
        old_stream = app.state.stream
        if old_stream:
            await old_stream.stop()

        old_broker = eng._broker if eng else None
        if old_broker and old_broker._connected:
            await old_broker.disconnect()

        # settings 런타임 변경
        settings.set("market", target)

        # 새 컴포넌트 빌드
        from scalpy.main import build_engine, build_registry

        new_registry = build_registry()
        new_engine, new_broker = build_engine(new_registry)

        mock = settings.get("mock", True)
        hts_id = settings.get("kis_hts_id", "")

        if target == "us":
            from scalpy.data.us_stream import USMarketDataStream
            exchange = settings.get("us_trading.exchange", "NASD")
            new_stream = USMarketDataStream(
                app_key=settings.get("kis_app_key", ""),
                app_secret=settings.get("kis_app_secret", ""),
                mock=mock,
                hts_id=hts_id,
                exchange=exchange,
            )
        else:
            from scalpy.data.stream import MarketDataStream
            new_stream = MarketDataStream(
                app_key=settings.get("kis_app_key", ""),
                app_secret=settings.get("kis_app_secret", ""),
                mock=mock,
                hts_id=hts_id,
            )

        new_stream.on_tick(new_engine.on_tick)
        new_stream.on_orderbook(new_engine.on_orderbook)
        new_stream.on_fill(new_engine.on_fill_notice)
        new_stream.on_vi(new_engine.on_vi_event)

        new_engine.set_event_bus(app.state.bus)

        if app.state.trade_repo:
            new_engine.set_trade_repo(app.state.trade_repo)
            new_engine._performance.set_repo(app.state.trade_repo)

        await new_broker.connect()
        cancelled = await new_broker.cancel_all_orders()
        if cancelled > 0:
            logger.info("market_switch.cleared_orders", count=cancelled)
        await new_engine.sync_positions()
        await new_engine.get_cached_balance()
        new_engine.start_sync_loop()

        # app.state 갱신
        app.state.engine = new_engine
        app.state.stream = new_stream
        app.state.registry = new_registry
        app.state.current_market = target

        # 대시보드 라우트 재초기화 (마켓별 분리된 state 사용)
        se = app.state.sse
        b = app.state.bus
        tr = app.state.trade_repo
        kr_st = app.state.kr_state
        us_st = app.state.us_state

        old_market = "kr" if target == "us" else "us"
        old_state = kr_st if old_market == "kr" else us_st
        new_state = us_st if target == "us" else kr_st
        old_state.unregister_handlers(b)
        new_state.register_handlers(b)

        synced_names = getattr(new_broker, "_position_names", {})
        if synced_names:
            new_state.symbol_names.update(synced_names)

        if target == "us":
            init_us_routes(us_st, se, new_engine, bus=b, stream=new_stream, registry=new_registry, trade_repo=tr)
            init_routes(kr_st, se, None)
        else:
            init_routes(kr_st, se, new_engine, bus=b, stream=new_stream, registry=new_registry, trade_repo=tr)
            init_us_routes(us_st, se, None)

        from scalpy.main import update_trade_sync_broker
        update_trade_sync_broker(new_broker, new_engine)

        if app.state.trade_repo:
            try:
                trades = await new_broker.get_trade_history()
                if trades:
                    reasons = getattr(new_engine, "_trade_reasons", {}) if new_engine else {}
                    strats = getattr(new_engine, "_symbol_strategy", {}) if new_engine else {}
                    count = app.state.trade_repo.sync_trades(trades, reason_map=reasons, market=target, strategy_map=strats)
                    if count:
                        logger.info("market_switch.trade_sync", count=count, market=target)
            except Exception as e:
                logger.warning("market_switch.trade_sync_failed", error=str(e))

        logger.info("market_switch.done", market=target)
        return {"success": True, "market": target}

    @app.get("/backtest")
    async def backtest_page() -> HTMLResponse:
        if _bt_html.exists():
            return HTMLResponse(_bt_html.read_text())
        return HTMLResponse("<h1>Backtest not available</h1>")

    return app


def start_dashboard_server(
    state: DashboardState,
    bus: EventBus,
    engine: Any,
    host: str = "0.0.0.0",
    port: int = 8080,
    stream: Any = None,
    registry: StrategyRegistry | None = None,
    trade_repo: Any = None,
) -> asyncio.Task:
    app = create_app(state, bus, engine, stream=stream, registry=registry, trade_repo=trade_repo)

    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None

    task = asyncio.create_task(server.serve())
    task._uvicorn_server = server
    task._sse = app.state.sse
    logger.info("dashboard.started", host=host, port=port, url=f"http://localhost:{port}")
    return task
