"""KIS 해외주식 브로커 — 미국 주식 전용.

KIS Open API의 해외주식 REST/WebSocket 엔드포인트를 사용하며,
기존 KISBroker(국내 전용)를 전혀 수정하지 않는 독립 구현체.
"""

import asyncio
import json
import time as time_mod
from collections.abc import Callable
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import requests
import structlog

from scalpy.broker.base import BaseBroker
from scalpy.config import settings
from scalpy.core.enums import OrderStatus, OrderType, Side
from scalpy.core.exceptions import AuthenticationError
from scalpy.core.models import Order, Position
from scalpy.core.us_market import EXCHANGE_CODE_MAP, get_order_config, get_us_session

logger = structlog.get_logger()

_TOKEN_PATH = Path(__file__).resolve().parent.parent.parent.parent / "config" / ".token.json"


class KISOverseasBroker(BaseBroker):
    """한국투자증권 해외주식 API 브로커."""

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        account_no: str,
        *,
        mock: bool = True,
        exchange: str = "NASD",
        summer_time: bool = True,
    ) -> None:
        super().__init__()
        self._app_key = app_key
        self._app_secret = app_secret
        self._account_no = account_no
        self._cano = account_no[:8]
        self._acnt_prdt_cd = account_no[8:].lstrip("-") or "01"
        self._mock = mock
        self._exchange = exchange
        self._summer_time = summer_time
        self._symbol_exchange: dict[str, str] = {}
        self._connected = False
        self._token: str = ""
        self._token_expires: datetime = datetime.min
        self._last_api_call: float = 0
        self._api_lock = asyncio.Lock()
        self._slippage: float = 0.003

    def _get_exchange(self, symbol: str) -> str:
        return self._symbol_exchange.get(symbol, self._exchange)

    def _base_url(self) -> str:
        key = "virtual" if self._mock else "real"
        cfg = settings.get("kis_api.rest_urls", {})
        url = cfg.get(key)
        if not url:
            raise RuntimeError(f"kis_api.rest_urls.{key} 설정 필요")
        return url

    async def _throttle(self) -> None:
        gap = 1.05 if self._mock else 0.15
        async with self._api_lock:
            elapsed = time_mod.monotonic() - self._last_api_call
            if elapsed < gap:
                await asyncio.sleep(gap - elapsed)
            self._last_api_call = time_mod.monotonic()

    def _ensure_token(self) -> str:
        if self._token and self._token_expires > datetime.now() + timedelta(minutes=5):
            return self._token
        self._refresh_token()
        return self._token

    def _refresh_token(self) -> None:
        if _TOKEN_PATH.exists():
            data = json.loads(_TOKEN_PATH.read_text())
            expires = datetime.fromisoformat(data["valid_until"])
            if expires > datetime.now() + timedelta(minutes=5):
                raw = data["value"]
                self._token = raw if raw.startswith("Bearer ") else f"Bearer {raw}"
                self._token_expires = expires
                return

        url = f"{self._base_url()}/oauth2/tokenP"
        resp = requests.post(url, json={
            "grant_type": "client_credentials",
            "appkey": self._app_key,
            "appsecret": self._app_secret,
        }, timeout=10)
        resp.raise_for_status()
        body = resp.json()
        token_val = body["access_token"]
        expires = datetime.fromisoformat(body["access_token_token_expired"])
        _TOKEN_PATH.write_text(json.dumps({
            "value": token_val,
            "valid_until": expires.isoformat(),
        }))
        raw = token_val
        self._token = raw if raw.startswith("Bearer ") else f"Bearer {raw}"
        self._token_expires = expires
        logger.info("kis_overseas.token_created")

    def _headers(self, tr_id: str) -> dict[str, str]:
        if self._mock:
            tr_id = "V" + tr_id[1:]
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": self._ensure_token(),
            "appkey": self._app_key,
            "appsecret": self._app_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }

    def _get(self, path: str, tr_id: str, params: dict, *, tr_cont: str = "") -> dict:
        url = f"{self._base_url()}{path}"
        headers = self._headers(tr_id)
        if tr_cont:
            headers["tr_cont"] = tr_cont
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code >= 400:
            body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
            msg_cd = body.get("msg_cd", "")
            if "token" in body.get("msg1", "").lower() or msg_cd == "EGW00121":
                self._refresh_token()
                headers = self._headers(tr_id)
                if tr_cont:
                    headers["tr_cont"] = tr_cont
                time_mod.sleep(0.5)
                resp = requests.get(url, headers=headers, params=params, timeout=10)
            elif msg_cd == "EGW00201":
                time_mod.sleep(1)
                resp = requests.get(url, headers=headers, params=params, timeout=10)
            else:
                resp.raise_for_status()
        data = resp.json()
        data["_tr_cont"] = resp.headers.get("tr_cont", "")
        return data

    def _post(self, path: str, tr_id: str, body: dict) -> dict:
        url = f"{self._base_url()}{path}"
        resp = requests.post(url, headers=self._headers(tr_id), json=body, timeout=10)
        if resp.status_code >= 400:
            rj = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
            msg_cd = rj.get("msg_cd", "")
            if "token" in rj.get("msg1", "").lower() or msg_cd == "EGW00121":
                self._refresh_token()
                time_mod.sleep(0.5)
                resp = requests.post(url, headers=self._headers(tr_id), json=body, timeout=10)
            else:
                logger.warning("kis_overseas.post_error",
                               status=resp.status_code, tr_id=tr_id,
                               msg=rj.get("msg1", ""), msg_cd=msg_cd,
                               body=body)
                resp.raise_for_status()
        return resp.json()

    async def connect(self) -> None:
        self._refresh_token()
        self._connected = True
        mode = "모의투자" if self._mock else "실거래"
        logger.info("kis_overseas.connected", mode=mode, exchange=self._exchange)

    async def disconnect(self) -> None:
        self._connected = False
        logger.info("kis_overseas.disconnected")

    async def place_order(self, order: Order) -> Order:
        if not self._connected:
            order.status = OrderStatus.REJECTED
            return order

        session = get_us_session(summer_time=self._summer_time)
        if session == "closed":
            order.status = OrderStatus.REJECTED
            order.reject_reason = "US market closed"
            logger.warning("kis_overseas.market_closed", symbol=order.symbol)
            return order

        await self._throttle()
        try:
            cfg = get_order_config(session, mock=self._mock)
            if cfg is None:
                order.status = OrderStatus.REJECTED
                order.reject_reason = f"{session} not available"
                return order

            tr_id = cfg["tr_id_buy"] if order.side == Side.BUY else cfg["tr_id_sell"]
            if cfg["market_order_ok"] and order.order_type == OrderType.MARKET:
                price_str = "0"
            else:
                slippage = 1 + self._slippage if order.side == Side.BUY else 1 - self._slippage
                price_str = f"{float(order.price) * slippage:.2f}"
                logger.debug("kis_overseas.order_price", symbol=order.symbol,
                             side=order.side.value, base_price=str(order.price),
                             slippage_price=price_str, session=session)

            exg = self._get_exchange(order.symbol)
            body = {
                "CANO": self._cano,
                "ACNT_PRDT_CD": self._acnt_prdt_cd,
                "OVRS_EXCG_CD": exg,
                "PDNO": order.symbol,
                "ORD_QTY": str(order.quantity),
                "OVRS_ORD_UNPR": price_str,
                "ORD_SVR_DVSN_CD": "0",
                "ORD_DVSN": "00",
            }
            if order.side == Side.SELL and session != "daytime":
                body["SLL_TYPE"] = "00"

            data = await asyncio.to_thread(
                self._post, cfg["path"], tr_id, body
            )

            if data.get("rt_cd") == "0":
                order.status = OrderStatus.PENDING
                order.order_id = data.get("output", {}).get("ODNO", "")
                logger.info("kis_overseas.order_submitted",
                            symbol=order.symbol, side=order.side.value,
                            qty=order.quantity, price=price_str,
                            order_id=order.order_id, session=session)
            else:
                order.status = OrderStatus.REJECTED
                order.reject_reason = data.get("msg1", "unknown")
                logger.warning("kis_overseas.order_rejected_detail",
                               symbol=order.symbol, tr_id=tr_id, exchange=exg,
                               price=price_str, body=body)
                logger.warning("kis_overseas.order_rejected",
                               symbol=order.symbol, msg=data.get("msg1", ""))
        except Exception as e:
            order.status = OrderStatus.REJECTED
            order.reject_reason = str(e)
            logger.warning("kis_overseas.order_error", symbol=order.symbol, error=str(e))

        return order

    async def _cancel_order_detail(self, order_id: str, symbol: str, exchange: str, org_no: str) -> bool:
        if not self._connected:
            return False
        await self._throttle()
        try:
            tr_id = "VTTT1004U" if self._mock else "TTTT1004U"
            body = {
                "CANO": self._cano,
                "ACNT_PRDT_CD": self._acnt_prdt_cd,
                "OVRS_EXCG_CD": exchange,
                "PDNO": symbol,
                "ORGN_ODNO": order_id,
                "RVSE_CNCL_DVSN_CD": "02",
                "ORD_QTY": "0",
                "OVRS_ORD_UNPR": "0",
                "KRX_FWDG_ORD_ORGNO": org_no,
            }
            data = await asyncio.to_thread(
                self._post, "/uapi/overseas-stock/v1/trading/order-rvsecncl", tr_id, body
            )
            if data.get("rt_cd") == "0":
                logger.info("kis_overseas.order_cancelled", symbol=symbol, order_id=order_id)
                return True
            else:
                logger.warning("kis_overseas.cancel_rejected", symbol=symbol,
                               order_id=order_id, msg=data.get("msg1", ""))
                return False
        except Exception as e:
            logger.error("kis_overseas.cancel_failed", order_id=order_id, error=str(e))
            return False

    async def cancel_order(self, order_id: str) -> bool:
        return await self._cancel_order_detail(order_id, "", self._exchange, "")

    async def cancel_all_orders(self) -> int:
        if not self._connected:
            return 0
        cancelled = 0
        for exg_code in EXCHANGE_CODE_MAP:
            await self._throttle()
            try:
                params = {
                    "CANO": self._cano,
                    "ACNT_PRDT_CD": self._acnt_prdt_cd,
                    "OVRS_EXCG_CD": exg_code,
                    "SORT_SQN": "DS",
                    "CTX_AREA_FK200": "",
                    "CTX_AREA_NK200": "",
                }
                data = await asyncio.to_thread(
                    self._get, "/uapi/overseas-stock/v1/trading/inquire-nccs", "TTTS3018R", params
                )
                orders = data.get("output", [])
                for o in orders:
                    odno = o.get("odno", "")
                    symbol = o.get("pdno", "")
                    excg = o.get("ovrs_excg_cd", exg_code)
                    org_no = o.get("ord_gno_brno", "")
                    if odno and await self._cancel_order_detail(odno, symbol, excg, org_no):
                        cancelled += 1
            except Exception as e:
                logger.warning("kis_overseas.cancel_all_partial", exchange=exg_code, error=str(e))
        return cancelled

    async def sync_positions(self) -> int:
        if not self._connected:
            return 0

        items: list[dict] = []
        for exg_code in EXCHANGE_CODE_MAP:
            await self._throttle()
            try:
                params = {
                    "CANO": self._cano,
                    "ACNT_PRDT_CD": self._acnt_prdt_cd,
                    "OVRS_EXCG_CD": exg_code,
                    "TR_CRCY_CD": "USD",
                    "CTX_AREA_FK200": "",
                    "CTX_AREA_NK200": "",
                }
                data = await asyncio.to_thread(
                    self._get, "/uapi/overseas-stock/v1/trading/inquire-balance", "TTTS3012R", params
                )
                for item in data.get("output1", []):
                    item["_exg_code"] = exg_code
                    items.append(item)
            except Exception as e:
                logger.warning("kis_overseas.sync_partial", exchange=exg_code, error=str(e))

        try:
            api_positions: dict[str, Position] = {}
            for item in items:
                qty = int(item.get("ovrs_cblc_qty", "0"))
                sellable = int(item.get("ord_psbl_qty", "0"))
                if qty == 0 or sellable == 0:
                    continue
                symbol = item.get("ovrs_pdno", "")
                if not symbol:
                    continue
                name = item.get("ovrs_item_name", symbol)
                self._position_names[symbol] = name
                if "_exg_code" in item:
                    self._symbol_exchange[symbol] = item["_exg_code"]
                pnl = Decimal(item.get("frcr_evlu_pfls_amt", "0"))
                pnl_rt = float(item.get("evlu_pfls_rt", "0"))
                api_positions[symbol] = Position(
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=qty,
                    avg_price=Decimal(item.get("pchs_avg_pric", "0")),
                    current_price=Decimal(item.get("now_pric2", "0")),
                    strategy="synced",
                    opened_at=datetime.now(),
                    unrealized_pnl=pnl,
                )
                api_positions[symbol]._pnl_pct = pnl_rt

            existing = self._pm._positions
            for code, api_pos in api_positions.items():
                if code in existing:
                    existing[code].quantity = api_pos.quantity
                    existing[code].avg_price = api_pos.avg_price
                    existing[code].current_price = api_pos.current_price
                    existing[code].unrealized_pnl = api_pos.unrealized_pnl
                    existing[code]._pnl_pct = api_pos._pnl_pct
                else:
                    existing[code] = api_pos

            gone = [s for s in existing if s not in api_positions]
            for s in gone:
                del existing[s]

            logger.info("kis_overseas.positions_synced", count=len(existing))
            return len(existing)
        except Exception as e:
            logger.warning("kis_overseas.sync_failed", error=str(e))
            return 0

    async def get_balance(self) -> Decimal:
        if not self._connected:
            return Decimal("0")
        try:
            cash = await self.get_available_cash()
            pos_value = Decimal("0")
            for pos in self._pm._positions.values():
                pos_value += pos.current_price * pos.quantity
            total = cash + pos_value
            logger.debug("kis_overseas.balance", cash=str(cash), positions=str(pos_value), total=str(total))
            return total
        except Exception as e:
            logger.warning("kis_overseas.balance_failed", error=str(e))
            return Decimal("0")

    async def get_available_cash(self) -> Decimal:
        """TTTS3007R (매수가능금액) — ITEM_CD 필수이므로 AAPL 기준 조회."""
        if not self._connected:
            return Decimal("0")
        await self._throttle()
        try:
            params = {
                "CANO": self._cano,
                "ACNT_PRDT_CD": self._acnt_prdt_cd,
                "OVRS_EXCG_CD": self._exchange,
                "OVRS_ORD_UNPR": "0",
                "ITEM_CD": "AAPL",
            }
            data = await asyncio.to_thread(
                self._get, "/uapi/overseas-stock/v1/trading/inquire-psamount", "TTTS3007R", params
            )
            if data.get("rt_cd") != "0":
                logger.warning("kis_overseas.cash_api_error",
                               msg=data.get("msg1", ""))
                return Decimal("0")
            output = data.get("output", {})
            cash = output.get("ord_psbl_frcr_amt", "0")
            return Decimal(str(cash or "0"))
        except Exception as e:
            logger.warning("kis_overseas.available_cash_failed", error=str(e))
            return Decimal("0")

    async def get_buyable_qty(self, symbol: str, price: Decimal) -> int:
        if not self._connected or price <= 0:
            return 0
        await self._throttle()
        try:
            exg = self._get_exchange(symbol)
            params = {
                "CANO": self._cano,
                "ACNT_PRDT_CD": self._acnt_prdt_cd,
                "OVRS_EXCG_CD": exg,
                "OVRS_ORD_UNPR": f"{float(price):.2f}",
                "ITEM_CD": symbol,
            }
            data = await asyncio.to_thread(
                self._get, "/uapi/overseas-stock/v1/trading/inquire-psamount", "TTTS3007R", params
            )
            output = data.get("output", {})
            qty = int(output.get("max_ord_psbl_qty", "0"))
            if qty > 0:
                return qty
        except Exception:
            pass
        cash = await self.get_available_cash()
        return int(cash / price)

    async def get_minute_candles(self, symbol: str, count: int = 60) -> list[dict]:
        if not self._connected:
            return []
        excd = EXCHANGE_CODE_MAP.get(self._get_exchange(symbol), "NAS")
        await self._throttle()
        try:
            params = {
                "AUTH": "",
                "EXCD": excd,
                "SYMB": symbol,
                "NMIN": "1",
                "PINC": "1",
                "NEXT": "",
                "NREC": str(min(count, 120)),
                "FILL": "",
                "KEYB": "",
            }
            data = await asyncio.to_thread(
                self._get,
                "/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice",
                "HHDFS76950200",
                params,
            )
            items = data.get("output2", [])
            result = []
            for item in items:
                result.append({
                    "open": float(item.get("open", 0)),
                    "high": float(item.get("high", 0)),
                    "low": float(item.get("low", 0)),
                    "close": float(item.get("last", 0)),
                    "volume": int(item.get("evol", 0)),
                    "time": item.get("xhms", ""),
                })
            result.sort(key=lambda c: c["time"])
            return result[-count:]
        except Exception as e:
            logger.warning("kis_overseas.minute_candles_failed", symbol=symbol, error=str(e))
            return []

    async def get_trade_history(self) -> list[dict[str, Any]]:
        if not self._connected:
            return []
        trades: list[dict[str, Any]] = []
        now = datetime.now()
        today = now.strftime("%Y%m%d")
        yesterday = (now - timedelta(days=1)).strftime("%Y%m%d")
        start_dt = yesterday if now.hour < 10 else today
        for exg_code in EXCHANGE_CODE_MAP:
            ctx_fk = ""
            ctx_nk = ""
            page = 0
            while page < 5:
                await self._throttle()
                try:
                    params = {
                        "CANO": self._cano,
                        "ACNT_PRDT_CD": self._acnt_prdt_cd,
                        "PDNO": "",
                        "ORD_STRT_DT": start_dt,
                        "ORD_END_DT": today,
                        "SLL_BUY_DVSN": "00",
                        "CCLD_NCCS_DVSN": "01",
                        "OVRS_EXCG_CD": exg_code,
                        "SORT_SQN": "DS",
                        "ORD_DT": "",
                        "ORD_GNO_BRNO": "",
                        "ODNO": "",
                        "CTX_AREA_FK200": ctx_fk,
                        "CTX_AREA_NK200": ctx_nk,
                    }
                    cont = "N" if page > 0 else ""
                    data = await asyncio.to_thread(
                        self._get, "/uapi/overseas-stock/v1/trading/inquire-ccnl",
                        "TTTS3035R", params, tr_cont=cont,
                    )
                    rt_cd = data.get("rt_cd", "")
                    items = data.get("output", [])
                    if rt_cd != "0":
                        logger.warning("kis_overseas.trade_history_api_fail",
                                       exchange=exg_code, rt_cd=rt_cd,
                                       msg=data.get("msg1", ""), msg_cd=data.get("msg_cd", ""))
                        break
                    logger.debug("kis_overseas.trade_history_page",
                                exchange=exg_code, page=page, count=len(items),
                                start=start_dt, end=today,
                                tr_cont=data.get("_tr_cont", ""))
                    for item in items:
                        ft_ccld_qty = int(item.get("ft_ccld_qty", "0"))
                        if ft_ccld_qty == 0:
                            continue
                        side_cd = item.get("sll_buy_dvsn_cd", "")
                        trades.append({
                            "order_no": item.get("odno", ""),
                            "order_date": item.get("ord_dt", ""),
                            "ord_time": item.get("ord_tmd", ""),
                            "symbol": item.get("pdno", ""),
                            "name": item.get("prdt_name", ""),
                            "side": "sell" if side_cd == "01" else "buy",
                            "ord_qty": int(item.get("ft_ord_qty", "0")),
                            "ord_price": float(item.get("ft_ord_unpr3", "0")),
                            "tot_ccld_qty": ft_ccld_qty,
                            "avg_price": float(item.get("ft_ccld_unpr3", "0")),
                            "tot_ccld_amt": float(item.get("ft_ccld_amt3", "0")),
                            "rmn_qty": int(item.get("nccs_qty", "0")),
                            "orgn_order_no": item.get("orgn_odno", ""),
                            "ord_dvsn_cd": item.get("ord_dvsn_cd", ""),
                            "cncl_yn": item.get("cncl_yn", ""),
                            "exchange": exg_code,
                        })
                    resp_cont = data.get("_tr_cont", "")
                    ctx_fk = data.get("ctx_area_fk200", "")
                    ctx_nk = data.get("ctx_area_nk200", "")
                    if resp_cont not in ("F", "M") or not items:
                        break
                    page += 1
                except Exception as e:
                    logger.warning("kis_overseas.trade_history_partial",
                                   exchange=exg_code, page=page, error=str(e))
                    break
        logger.info("kis_overseas.trade_history_total", count=len(trades))
        return trades

    async def get_top_volume_stocks(self, count: int = 30) -> list[dict[str, Any]]:
        if not self._connected:
            return []
        all_stocks: list[dict[str, Any]] = []
        for exg_code, excd in EXCHANGE_CODE_MAP.items():
            await self._throttle()
            try:
                params = {
                    "AUTH": "",
                    "EXCD": excd,
                    "SYMB": "",
                    "GUBN": "0",
                    "BYMD": "",
                    "MODP": "0",
                    "KEYB": "",
                }
                data = await asyncio.to_thread(
                    self._get,
                    "/uapi/overseas-price/v1/quotations/inquire-search",
                    "HHDFS76410000",
                    params,
                )
                items = data.get("output2", [])
                for item in items:
                    symbol = item.get("symb", "")
                    if not symbol:
                        continue
                    self._symbol_exchange[symbol] = exg_code
                    all_stocks.append({
                        "symbol": symbol,
                        "name": item.get("name", ""),
                        "volume": int(item.get("tvol", 0)),
                        "price": Decimal(item.get("last", "0")),
                        "change_rate": float(item.get("rate", "0")),
                        "exchange": exg_code,
                    })
            except Exception as e:
                logger.warning("kis_overseas.top_volume_partial", exchange=exg_code, error=str(e))
        all_stocks.sort(key=lambda s: s["volume"], reverse=True)
        logger.info("kis_overseas.top_volume_scanned", total=len(all_stocks), exchanges=list(EXCHANGE_CODE_MAP.keys()))
        return all_stocks[:count]

    async def get_current_price(self, symbol: str) -> Decimal:
        if not self._connected:
            return Decimal("0")
        excd = EXCHANGE_CODE_MAP.get(self._get_exchange(symbol), "NAS")
        await self._throttle()
        try:
            params = {
                "AUTH": "",
                "EXCD": excd,
                "SYMB": symbol,
            }
            data = await asyncio.to_thread(
                self._get,
                "/uapi/overseas-price/v1/quotations/price",
                "HHDFS00000300",
                params,
            )
            output = data.get("output", {})
            return Decimal(output.get("last", "0"))
        except Exception as e:
            logger.warning("kis_overseas.current_price_failed", symbol=symbol, error=str(e))
            return Decimal("0")

    async def subscribe_market_data(
        self, symbols: list[str], callback: Callable[..., Any]
    ) -> None:
        logger.warning("kis_overseas.subscribe_via_broker_not_supported")
