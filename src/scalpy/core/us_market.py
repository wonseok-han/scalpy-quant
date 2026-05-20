"""미국 주식 시장 상수 및 세션 판별.

브로커(kis_overseas), 스트림(us_stream), 대시보드 등에서 공통으로 사용.
"""

from datetime import datetime, time

# --- 거래소 코드 매핑 ---
EXCHANGE_CODE_MAP = {
    "NASD": "NAS",
    "NYSE": "NYS",
    "AMEX": "AMS",
}

# --- WebSocket tr_key prefix ---
WS_REGULAR_PREFIX = {
    "NASD": "DNAS",
    "NYSE": "DNYS",
    "AMEX": "DAMS",
}

WS_EXTENDED_PREFIX = {
    "NASD": "RBAQ",
    "NYSE": "RBAY",
    "AMEX": "RBAA",
}

# --- 세션별 주문 API ---
ORDER_PATH_REGULAR = "/uapi/overseas-stock/v1/trading/order"
ORDER_PATH_DAYTIME = "/uapi/overseas-stock/v1/trading/daytime-order"

ORDER_TR_ID = {
    "regular": {"buy": "TTTT1002U", "sell": "TTTT1006U"},
    "pre_market": {"buy": "TTTT1002U", "sell": "TTTT1006U"},
    "after_market": {"buy": "TTTT1002U", "sell": "TTTT1006U"},
    "daytime": {"buy": "TTTS6036U", "sell": "TTTS6037U"},
}

ORDER_TR_ID_MOCK = {
    "regular": {"buy": "VTTT1002U", "sell": "VTTT1001U"},
    "pre_market": {"buy": "VTTT1002U", "sell": "VTTT1001U"},
    "after_market": {"buy": "VTTT1002U", "sell": "VTTT1001U"},
}

# --- 세션 시간대 (KST) ---
_SESSIONS_SUMMER = [
    (time(5, 0), time(7, 0), "after_market"),
    (time(7, 0), time(10, 0), "closed"),
    (time(10, 0), time(17, 0), "daytime"),
    (time(17, 0), time(22, 30), "pre_market"),
]

_SESSIONS_NORMAL = [
    (time(6, 0), time(7, 0), "after_market"),
    (time(7, 0), time(10, 0), "closed"),
    (time(10, 0), time(18, 0), "daytime"),
    (time(18, 0), time(23, 30), "pre_market"),
]


def get_us_session(*, summer_time: bool = True) -> str:
    """KST 기준 미국 주식 거래 세션 판별.

    Returns: "daytime" | "pre_market" | "regular" | "after_market" | "closed"
    """
    now = datetime.now().time()
    sessions = _SESSIONS_SUMMER if summer_time else _SESSIONS_NORMAL
    for start, end, name in sessions:
        if start <= now < end:
            return name
    return "regular"


def get_order_config(session: str, *, mock: bool = False) -> dict | None:
    """세션별 주문 API 설정 반환.

    Returns: {"path", "tr_id_buy", "tr_id_sell", "market_order_ok"}
    None if session is closed or mock daytime.

    market_order_ok: True only for regular session (OVRS_ORD_UNPR="0" 시장가).
    프리마켓/애프터마켓/주간거래는 지정가만 가능.
    """
    if session == "closed":
        return None
    if session == "daytime":
        if mock:
            return None
        return {
            "path": ORDER_PATH_DAYTIME,
            "tr_id_buy": ORDER_TR_ID["daytime"]["buy"],
            "tr_id_sell": ORDER_TR_ID["daytime"]["sell"],
            "market_order_ok": False,
        }
    tr_ids = ORDER_TR_ID_MOCK[session] if mock else ORDER_TR_ID[session]
    return {
        "path": ORDER_PATH_REGULAR,
        "tr_id_buy": tr_ids["buy"],
        "tr_id_sell": tr_ids["sell"],
        "market_order_ok": session == "regular",
    }
