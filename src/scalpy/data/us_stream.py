"""미국 주식 실시간 시세 WebSocket 스트림.

KIS 해외주식 실시간 API를 사용하며, 기존 stream.py(국장)를 수정하지 않는 독립 구현체.
TR ID: HDFSCNT0 (체결가), HDFSASP0 (호가), H0GSCNI0/H0GSCNI9 (체결통보)

세션별 tr_key prefix:
  정규장 (KST 22:30~05:00): DNAS/DNYS/DAMS + symbol
  주간거래 (KST 10:00~16:00): RBAQ/RBAY/RBAA + symbol
  공백 (KST 05:00~10:00, 16:00~22:30): WS 데이터 없음
"""

import asyncio
import contextlib
import json
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import Any

import requests
import structlog
import websockets

from scalpy.config import settings
from scalpy.core.us_market import WS_EXTENDED_PREFIX, WS_REGULAR_PREFIX, get_us_session

logger = structlog.get_logger()

TickCallback = Callable[[str, Decimal, int], Any]
OrderbookCallback = Callable[[str, list[tuple[Decimal, int]], list[tuple[Decimal, int]]], Any]
FillCallback = Callable[[dict[str, Any]], Any]


def _get_ws_url(mock: bool) -> str:
    key = "virtual" if mock else "real"
    cfg = settings.get("kis_api.ws_urls", {})
    url = cfg.get(key)
    if not url:
        raise RuntimeError(f"kis_api.ws_urls.{key} 설정 필요")
    return url


def _get_rest_url(mock: bool) -> str:
    key = "virtual" if mock else "real"
    cfg = settings.get("kis_api.rest_urls", {})
    url = cfg.get(key)
    if not url:
        raise RuntimeError(f"kis_api.rest_urls.{key} 설정 필요")
    return url


def _get_approval_key(app_key: str, app_secret: str, *, mock: bool = True) -> str:
    url = _get_rest_url(mock)
    resp = requests.post(
        f"{url}/oauth2/Approval",
        headers={"content-type": "application/json"},
        json={
            "grant_type": "client_credentials",
            "appkey": app_key,
            "secretkey": app_secret,
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["approval_key"]


def _sub_msg(approval_key: str, tr_id: str, tr_key: str) -> str:
    return json.dumps({
        "header": {
            "approval_key": approval_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8",
        },
        "body": {"input": {"tr_id": tr_id, "tr_key": tr_key}},
    })


def _unsub_msg(approval_key: str, tr_id: str, tr_key: str) -> str:
    return json.dumps({
        "header": {
            "approval_key": approval_key,
            "custtype": "P",
            "tr_type": "2",
            "content-type": "utf-8",
        },
        "body": {"input": {"tr_id": tr_id, "tr_key": tr_key}},
    })


class USMarketDataStream:
    def __init__(
        self,
        app_key: str = "",
        app_secret: str = "",
        *,
        mock: bool = True,
        hts_id: str = "",
        exchange: str = "NASD",
    ) -> None:
        self._app_key = app_key
        self._app_secret = app_secret
        self._mock = mock
        self._hts_id = hts_id
        self._exchange = exchange
        self._symbol_exchange: dict[str, str] = {}
        self._tick_callbacks: list[TickCallback] = []
        self._orderbook_callbacks: list[OrderbookCallback] = []
        self._fill_callbacks: list[FillCallback] = []
        self._running = False
        self._ws: Any = None
        self._subscribed: set[str] = set()
        self._approval_key: str = ""
        self._recv_task: asyncio.Task[None] | None = None
        self._stopping = False
        self._last_tick: dict[str, tuple[Decimal, int]] = {}
        self._last_ask: dict[str, Decimal] = {}
        self._last_bid: dict[str, Decimal] = {}

    def on_tick(self, callback: TickCallback) -> None:
        self._tick_callbacks.append(callback)

    def on_orderbook(self, callback: OrderbookCallback) -> None:
        self._orderbook_callbacks.append(callback)

    def on_fill(self, callback: FillCallback) -> None:
        self._fill_callbacks.append(callback)

    def on_vi(self, callback: Any) -> None:
        pass

    def get_latest_ask(self, symbol: str) -> Decimal | None:
        return self._last_ask.get(symbol)

    def get_latest_bid(self, symbol: str) -> Decimal | None:
        return self._last_bid.get(symbol)

    async def emit_tick(self, symbol: str, price: Decimal, volume: int) -> None:
        for cb in self._tick_callbacks:
            result = cb(symbol, price, volume)
            if asyncio.iscoroutine(result):
                await result

    async def emit_orderbook(
        self,
        symbol: str,
        asks: list[tuple[Decimal, int]],
        bids: list[tuple[Decimal, int]],
    ) -> None:
        for cb in self._orderbook_callbacks:
            result = cb(symbol, asks, bids)
            if asyncio.iscoroutine(result):
                await result

    async def _emit_fill(self, data: dict[str, Any]) -> None:
        for cb in self._fill_callbacks:
            result = cb(data)
            if asyncio.iscoroutine(result):
                await result

    def set_symbol_exchanges(self, mapping: dict[str, str]) -> None:
        self._symbol_exchange.update(mapping)

    def _tr_key(self, symbol: str) -> str:
        exg = self._symbol_exchange.get(symbol, self._exchange)
        if get_us_session() == "daytime":
            prefix = WS_EXTENDED_PREFIX.get(exg, "RBAQ")
        else:
            prefix = WS_REGULAR_PREFIX.get(exg, "DNAS")
        return f"{prefix}{symbol}"

    async def start(self, symbols: list[str]) -> None:
        await self._cleanup_recv_task()
        if self._ws:
            try:
                await asyncio.wait_for(self._ws.close(), timeout=2)
            except (asyncio.TimeoutError, Exception):
                pass
            self._ws = None
        self._subscribed.clear()

        if not self._app_key:
            logger.warning("us_stream.no_api_key")
            return

        self._approval_key = _get_approval_key(self._app_key, self._app_secret, mock=self._mock)
        ws_url = _get_ws_url(self._mock)

        self._ws = await websockets.connect(ws_url, ping_interval=None)
        self._running = True

        if self._hts_id:
            cni_tr = "H0GSCNI9" if self._mock else "H0GSCNI0"
            await self._ws.send(_sub_msg(self._approval_key, cni_tr, self._hts_id))
            await asyncio.sleep(0.1)

        for sym in symbols:
            tr_key = self._tr_key(sym)
            await self._ws.send(_sub_msg(self._approval_key, "HDFSCNT0", tr_key))
            await asyncio.sleep(0.1)
            await self._ws.send(_sub_msg(self._approval_key, "HDFSASP0", tr_key))
            await asyncio.sleep(0.1)

        self._subscribed = set(symbols)
        session = get_us_session()
        sample_key = self._tr_key(symbols[0]) if symbols else ""
        logger.info("us_stream.started", symbols=symbols, exchange=self._exchange,
                     session=session, tr_key_sample=sample_key)
        self._recv_task = asyncio.create_task(self._recv_loop())

    async def _recv_loop(self) -> None:
        reconnect_delay = 5
        while self._running:
            try:
                async for raw in self._ws:
                    if not self._running:
                        return
                    if raw[0] in ("0", "1"):
                        reconnect_delay = 5
                    await self._handle_message(raw)
            except websockets.ConnectionClosed:
                logger.warning("us_stream.disconnected")
            except Exception as e:
                logger.error("us_stream.error", error=str(e))

            if not self._running:
                return

            delay = min(reconnect_delay, 60)
            logger.info("us_stream.reconnecting", delay=delay)
            await asyncio.sleep(delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

            try:
                if self._ws:
                    try:
                        await asyncio.wait_for(self._ws.close(), timeout=2)
                    except Exception:
                        pass
                    self._ws = None
                    await asyncio.sleep(3)

                self._approval_key = await asyncio.to_thread(
                    _get_approval_key, self._app_key, self._app_secret, mock=self._mock,
                )
                await asyncio.sleep(1)
                ws_url = _get_ws_url(self._mock)
                self._ws = await websockets.connect(ws_url, ping_interval=None)

                if self._hts_id:
                    cni_tr = "H0GSCNI9" if self._mock else "H0GSCNI0"
                    await self._ws.send(_sub_msg(self._approval_key, cni_tr, self._hts_id))
                    await asyncio.sleep(0.2)

                for sym in self._subscribed:
                    tr_key = self._tr_key(sym)
                    await self._ws.send(_sub_msg(self._approval_key, "HDFSCNT0", tr_key))
                    await asyncio.sleep(0.2)
                    await self._ws.send(_sub_msg(self._approval_key, "HDFSASP0", tr_key))
                    await asyncio.sleep(0.2)

                logger.info("us_stream.reconnected", symbols=len(self._subscribed))
            except Exception as e:
                logger.error("us_stream.reconnect_failed", error=str(e))

    async def _handle_message(self, raw: str) -> None:
        if not self._running:
            return
        if raw[0] in ("0", "1"):
            parts = raw.split("|")
            tr_id = parts[1]
            data = parts[3]

            if tr_id == "HDFSCNT0":
                await self._on_execution(data)
            elif tr_id == "HDFSASP0":
                await self._on_orderbook_data(data)
            elif tr_id in ("H0GSCNI0", "H0GSCNI9"):
                await self._on_fill_notice(data)
        else:
            msg = json.loads(raw)
            tr_id = msg.get("header", {}).get("tr_id", "")
            if tr_id == "PINGPONG":
                await self._ws.send(raw)
            else:
                rt_cd = msg.get("body", {}).get("rt_cd", "")
                msg1 = msg.get("body", {}).get("msg1", "")
                if rt_cd == "0":
                    logger.info("us_stream.subscribed", tr_id=tr_id, msg=msg1)
                else:
                    logger.warning("us_stream.sub_error", tr_id=tr_id, msg=msg1)

    @staticmethod
    def _strip_prefix(raw_symbol: str) -> str:
        """tr_key prefix(4자) 제거: 'DNASNXXT' → 'NXXT'."""
        return raw_symbol[4:] if len(raw_symbol) > 4 else raw_symbol

    async def _on_execution(self, data: str) -> None:
        """HDFSCNT0 (RSYM offset +1): [0]RSYM [1]SYMB ... [11]LAST [19]EVOL"""
        fields = data.split("^")
        symbol = self._strip_prefix(fields[0])
        n = len(fields)

        price = Decimal(fields[11])
        try:
            volume = int(float(fields[19])) if n > 19 and fields[19] else 0
        except (ValueError, IndexError):
            volume = 0

        now_kst = datetime.now().strftime("%H%M%S")
        logger.debug("us_tick.raw", symbol=symbol, last=str(price),
                     low=fields[10] if n > 10 else "",
                     volume=volume, sys_time=now_kst, field_count=n)

        last = self._last_tick.get(symbol)
        if last and last[0] == price and last[1] == volume:
            return
        self._last_tick[symbol] = (price, volume)
        await self.emit_tick(symbol, price, volume)

    async def _on_orderbook_data(self, data: str) -> None:
        """HDFSASP0 (RSYM offset +1): [0]RSYM [1]SYMB ... [11]PBID1 [12]PASK1 [13]VBID1 [14]VASK1"""
        fields = data.split("^")
        symbol = self._strip_prefix(fields[0])
        n = len(fields)
        asks: list[tuple[Decimal, int]] = []
        bids: list[tuple[Decimal, int]] = []

        if n > 14:
            bid_price = Decimal(fields[11]) if fields[11] else Decimal("0")
            ask_price = Decimal(fields[12]) if fields[12] else Decimal("0")
            bid_vol = int(float(fields[13])) if fields[13] else 0
            ask_vol = int(float(fields[14])) if fields[14] else 0
            if ask_price > 0:
                asks.append((ask_price, ask_vol))
                self._last_ask[symbol] = ask_price
            if bid_price > 0:
                bids.append((bid_price, bid_vol))
                self._last_bid[symbol] = bid_price

        logger.debug("us_tick.raw_ob", symbol=symbol, field_count=n,
                     f0=fields[0][:12] if fields[0] else "",
                     f11_bid=fields[11] if n > 11 else "",
                     f12_ask=fields[12] if n > 12 else "",
                     f13_bvol=fields[13] if n > 13 else "",
                     f14_avol=fields[14] if n > 14 else "",
                     bid1=str(bids[0][0]) if bids else "0",
                     ask1=str(asks[0][0]) if asks else "0")
        await self.emit_orderbook(symbol, asks, bids)

    async def _on_fill_notice(self, data: str) -> None:
        fields = data.split("^")
        if len(fields) < 14:
            return
        symbol = fields[8] if len(fields) > 8 else ""
        side_cd = fields[4] if len(fields) > 4 else ""
        fill_data: dict[str, Any] = {
            "symbol": symbol,
            "order_no": fields[2] if len(fields) > 2 else "",
            "side": "sell" if side_cd == "01" else "buy",
            "quantity": int(float(fields[9] or "0")) if len(fields) > 9 else 0,
            "price": float(fields[10] or "0") if len(fields) > 10 else 0,
            "is_fill": fields[13] == "2" if len(fields) > 13 else False,
            "is_rejected": fields[12] == "1" if len(fields) > 12 else False,
        }
        await self._emit_fill(fill_data)

    async def update_subscriptions(self, new_symbols: list[str]) -> None:
        new_set = set(new_symbols)
        to_remove = self._subscribed - new_set
        to_add = new_set - self._subscribed

        if not to_remove and not to_add:
            return

        if self._ws and self._approval_key:
            for sym in to_remove:
                tr_key = self._tr_key(sym)
                try:
                    await self._ws.send(_unsub_msg(self._approval_key, "HDFSCNT0", tr_key))
                    await self._ws.send(_unsub_msg(self._approval_key, "HDFSASP0", tr_key))
                except Exception as e:
                    logger.warning("us_stream.unsub_failed", symbol=sym, error=str(e))
                await asyncio.sleep(0.05)

            for sym in to_add:
                tr_key = self._tr_key(sym)
                try:
                    await self._ws.send(_sub_msg(self._approval_key, "HDFSCNT0", tr_key))
                    await self._ws.send(_sub_msg(self._approval_key, "HDFSASP0", tr_key))
                except Exception as e:
                    logger.warning("us_stream.sub_failed", symbol=sym, error=str(e))
                await asyncio.sleep(0.05)

        self._subscribed = new_set
        logger.info("us_stream.subscriptions_updated",
                     removed=list(to_remove), added=list(to_add), total=len(new_set))

    async def _cleanup_recv_task(self) -> None:
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._recv_task
        self._recv_task = None

    async def stop(self) -> None:
        self._stopping = True
        self._running = False
        await self._cleanup_recv_task()
        if self._ws:
            try:
                await asyncio.wait_for(self._ws.close(), timeout=3)
            except (asyncio.TimeoutError, Exception):
                pass
            self._ws = None
        self._stopping = False
        logger.info("us_stream.stopped")

    @property
    def is_running(self) -> bool:
        return self._running
