"""프리마켓 시간대에 KIS REST API가 현재가를 반환하는지 테스트."""
import asyncio
import sys
sys.path.insert(0, "src")

from scalpy.config import settings
from scalpy.broker.kis_overseas import KISOverseasBroker


async def main():
    b = KISOverseasBroker(
        app_key=settings.get("kis_app_key", ""),
        app_secret=settings.get("kis_app_secret", ""),
        account_no=settings.get("kis_account_no", ""),
        mock=False,
        exchange="NASD",
    )
    await b.connect()

    symbols = ["SOXS", "AAPL", "SBFM"]
    for sym in symbols:
        price = await b.get_current_price(sym)
        print(f"{sym}: ${price}")

        candles = await b.get_minute_candles(sym, 5)
        if candles:
            print(f"  최근 분봉 {len(candles)}개: {candles[-1]}")
        else:
            print(f"  분봉 없음")


asyncio.run(main())
