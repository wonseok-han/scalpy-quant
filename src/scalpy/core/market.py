import zoneinfo
from dataclasses import dataclass, field
from datetime import datetime
from datetime import time as dt_time


@dataclass(frozen=True)
class MarketConfig:
    name: str
    timezone: zoneinfo.ZoneInfo
    market_open: dt_time
    market_end: dt_time
    cutoff_buy: dt_time
    cutoff_close: dt_time
    pre_market_init: dt_time
    pre_market_open: dt_time | None = None
    crosses_midnight: bool = False
    currency: str = "KRW"
    exchange_codes: list[str] = field(default_factory=list)

    def is_market_hours(self, now: datetime | None = None) -> bool:
        if now is None:
            now = datetime.now(self.timezone)
        t = now.time()
        start = self.pre_market_open if self.pre_market_open else self.market_open
        if self.crosses_midnight:
            return t >= start or t <= self.market_end
        return start <= t <= self.market_end

    def is_pre_market(self, now: datetime | None = None) -> bool:
        if self.pre_market_open is None:
            return False
        if now is None:
            now = datetime.now(self.timezone)
        t = now.time()
        return self.pre_market_open <= t < self.market_open

    def is_buy_cutoff(self, now: datetime | None = None) -> bool:
        if now is None:
            now = datetime.now(self.timezone)
        t = now.time()
        if self.crosses_midnight:
            return self.cutoff_buy <= t <= dt_time(23, 59, 59) or t <= self.market_end
        return self.cutoff_buy <= t <= self.market_end

    def is_close_window(self, now: datetime | None = None) -> bool:
        if now is None:
            now = datetime.now(self.timezone)
        t = now.time()
        if self.crosses_midnight:
            end_hour = self.market_end.hour
            close_hour = self.cutoff_close.hour
            return (close_hour <= t.hour <= 23) or (t.hour <= end_hour and t <= self.market_end)
        return self.cutoff_close <= t <= self.market_end

    def should_daily_init(self, now: datetime | None = None) -> bool:
        if now is None:
            now = datetime.now(self.timezone)
        return now.time() >= self.pre_market_init


_KST = zoneinfo.ZoneInfo("Asia/Seoul")
_EST = zoneinfo.ZoneInfo("US/Eastern")

KR_MARKET = MarketConfig(
    name="kr",
    timezone=_KST,
    market_open=dt_time(9, 0),
    market_end=dt_time(15, 30),
    cutoff_buy=dt_time(15, 15),
    cutoff_close=dt_time(15, 18),
    pre_market_init=dt_time(8, 50),
    currency="KRW",
)

# EST 기준 — 서머타임 자동 대응 (EDT/EST 전환은 zoneinfo가 처리)
US_MARKET = MarketConfig(
    name="us",
    timezone=_EST,
    pre_market_open=dt_time(4, 0),
    market_open=dt_time(9, 30),
    market_end=dt_time(16, 0),
    cutoff_buy=dt_time(15, 45),
    cutoff_close=dt_time(15, 50),
    pre_market_init=dt_time(3, 30),
    currency="USD",
    exchange_codes=["NASD", "NYSE", "AMEX"],
)
