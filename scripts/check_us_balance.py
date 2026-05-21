"""KIS 해외 매수가능금액 API 응답 필드 확인용 스크립트."""
import json
from datetime import datetime, timedelta
from pathlib import Path

import requests

from scalpy.config import settings

BASE = settings.get("kis_base_url", "https://openapivts.koreainvestment.com:29443")
mock = settings.get("mock", True)
if mock:
    cfg = settings.get("kis_api.rest_urls", {})
    BASE = cfg.get("virtual", BASE)
else:
    cfg = settings.get("kis_api.rest_urls", {})
    BASE = cfg.get("real", BASE)

APP_KEY = settings.get("kis_app_key", "")
APP_SECRET = settings.get("kis_app_secret", "")
ACCT = settings.get("kis_account_no", "")
CANO = ACCT[:8]
ACNT_CD = ACCT[8:].lstrip("-") or "01"

token_path = Path(__file__).resolve().parent.parent / "config" / ".token.json"


def get_token() -> str:
    if token_path.exists():
        data = json.loads(token_path.read_text())
        expires = datetime.fromisoformat(data["valid_until"])
        if expires > datetime.now() + timedelta(minutes=5):
            return data["value"]

    resp = requests.post(f"{BASE}/oauth2/tokenP", json={
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
    }, timeout=10)
    resp.raise_for_status()
    body = resp.json()
    token_path.write_text(json.dumps({
        "value": body["access_token"],
        "valid_until": body["access_token_token_expired"],
    }))
    print("Token refreshed")
    return body["access_token"]


ACCESS_TOKEN = get_token()
if ACCESS_TOKEN.startswith("Bearer "):
    ACCESS_TOKEN = ACCESS_TOKEN.removeprefix("Bearer ")
tr_id = "VTTS3007R" if mock else "TTTS3007R"

headers = {
    "content-type": "application/json; charset=utf-8",
    "authorization": f"Bearer {ACCESS_TOKEN}",
    "appkey": APP_KEY,
    "appsecret": APP_SECRET,
    "tr_id": tr_id,
    "custtype": "P",
}

print(f"Mode: {'모의투자' if mock else '실거래'}")
print(f"Base: {BASE}")

# --- TTTC2101R: 해외증거금 통화별조회 (실거래 전용) ---
print("\n===== TTTC2101R 해외증거금 통화별조회 =====")
margin_tr_id = "TTTC2101R"
margin_headers = {
    "content-type": "application/json; charset=utf-8",
    "authorization": f"Bearer {ACCESS_TOKEN}",
    "appkey": APP_KEY,
    "appsecret": APP_SECRET,
    "tr_id": margin_tr_id,
    "custtype": "P",
}
margin_params = {
    "CANO": CANO,
    "ACNT_PRDT_CD": ACNT_CD,
}
resp = requests.get(
    f"{BASE}/uapi/overseas-stock/v1/trading/foreign-margin",
    headers=margin_headers, params=margin_params,
)
data = resp.json()
print(f"rt_cd: {data.get('rt_cd')}, msg1: {data.get('msg1')}")
print(f"msg_cd: {data.get('msg_cd')}")
output = data.get("output", [])
if isinstance(output, list):
    print(f"output entries: {len(output)}")
    for i, item in enumerate(output):
        print(f"\n  [{i}] crcy_cd={item.get('crcy_cd')}")
        for k, v in sorted(item.items()):
            if v and v != "0" and v != "0.00" and v != "0.0000":
                print(f"      {k}: {v}")
elif isinstance(output, dict):
    print("output is dict (not list):")
    for k, v in sorted(output.items()):
        print(f"  {k}: {v}")
else:
    print(f"output type: {type(output)}")
    print(f"raw output: {output}")

# --- TTTS3007R: 매수가능금액 (with AAPL) ---
print("\n\n===== TTTS3007R 매수가능금액 (AAPL) =====")
params = {
    "CANO": CANO,
    "ACNT_PRDT_CD": ACNT_CD,
    "OVRS_EXCG_CD": "NASD",
    "OVRS_ORD_UNPR": "0",
    "ITEM_CD": "AAPL",
}
resp = requests.get(
    f"{BASE}/uapi/overseas-stock/v1/trading/inquire-psamount",
    headers=headers, params=params,
)
data = resp.json()
print(f"rt_cd: {data.get('rt_cd')}, msg1: {data.get('msg1')}")
output = data.get("output", {})
for k, v in sorted(output.items()):
    if v and v != "0" and v != "0.00" and v != "0.0000":
        print(f"  ** {k}: {v}")
