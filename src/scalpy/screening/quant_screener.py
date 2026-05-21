import math
import ssl
import tempfile
import urllib.request
import zipfile
from typing import Any

import structlog

from scalpy.data.ohlcv import OhlcvRepository

logger = structlog.get_logger()

_WARN_KEYWORDS = ("SPAC", "관리종목", "투자주의", "투자경고", "투자위험", "거래정지")

_warn_symbols: set[str] = set()
_market_condition: dict[str, Any] = {}


def get_market_condition() -> dict[str, Any]:
    return _market_condition

_KOSPI_MST_URL = "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"
_KOSDAQ_MST_URL = (
    "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip"
)

# 종목마스터 시장경고 코드: 00=해당없음, 01=투자주의, 02=투자경고, 03=투자위험
_MST_WARN_CODES = {"03"}


def get_warn_symbols() -> set[str]:
    return _warn_symbols


def load_warn_symbols_from_master() -> set[str]:
    """KIS 종목마스터 파일에서 투자주의/경고/위험/관리종목/거래정지 종목을 일괄 추출."""
    warned: set[str] = set()
    ssl._create_default_https_context = ssl._create_unverified_context

    for url, tail_len, mng_offset, warn_offset, stop_offset in [
        (_KOSPI_MST_URL, 228, 63, 64, 61),
        (_KOSDAQ_MST_URL, 222, 58, 59, 56),
    ]:
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                zip_path = f"{tmpdir}/code.zip"
                urllib.request.urlretrieve(url, zip_path)
                with zipfile.ZipFile(zip_path) as zf:
                    mst_name = zf.namelist()[0]
                    zf.extract(mst_name, tmpdir)
                mst_path = f"{tmpdir}/{mst_name}"

                with open(mst_path, encoding="cp949") as f:
                    for row in f:
                        row = row.rstrip("\n")
                        if len(row) < tail_len + 9:
                            continue
                        code = row[:9].rstrip()
                        tail = row[-tail_len:]

                        mng = tail[mng_offset]
                        warn_code = tail[warn_offset : warn_offset + 2]
                        stop = tail[stop_offset]

                        if mng == "Y" or warn_code in _MST_WARN_CODES or stop == "Y":
                            warned.add(code)
        except Exception as e:
            logger.warning("master_file.download_failed", url=url, error=str(e))

    logger.info("master_file.warn_symbols_loaded", count=len(warned))
    return warned


def scan_market_universe(
    min_volume: int = 100_000,
    min_change_rate: float = -2.0,
    max_change_rate: float = 15.0,
    min_amount: int = 0,
    top_n: int = 100,
    max_price: int = 0,
) -> list[dict[str, Any]]:
    """FinanceDataReader로 전체 KRX 종목 중 기본 조건 통과 종목 반환."""
    global _warn_symbols
    import FinanceDataReader as fdr

    df = fdr.StockListing("KRX")
    df = df[df["Market"].isin(["KOSPI", "KOSDAQ"])]
    if "Dept" in df.columns:
        pattern = "|".join(_WARN_KEYWORDS)
        warn_mask = df["Dept"].str.contains(pattern, na=False)
        _warn_symbols = set(df.loc[warn_mask, "Code"].tolist())
        df = df[~warn_mask]
        logger.info("market_universe.warn_excluded", count=len(_warn_symbols))
    global _market_condition
    all_changes = df["ChagesRatio"].dropna()
    if len(all_changes) > 0:
        avg_change = float(all_changes.mean())
        adv = int((all_changes > 0).sum())
        dec = int((all_changes < 0).sum())
        total = len(all_changes)
        adv_ratio = adv / total if total > 0 else 0.5
        if avg_change > 0.5 and adv_ratio > 0.6:
            regime = "bullish"
            recommend = ["ichimoku", "momentum"]
        elif avg_change < -0.5 and adv_ratio < 0.4:
            regime = "bearish"
            recommend = ["mean_reversion"]
        else:
            regime = "sideways"
            recommend = ["mean_reversion", "factor"]
        _market_condition = {
            "regime": regime,
            "avg_change": round(avg_change, 2),
            "adv_count": adv,
            "dec_count": dec,
            "total": total,
            "adv_ratio": round(adv_ratio * 100, 1),
            "recommend": recommend,
        }
        logger.info(
            "market.condition",
            regime=regime,
            avg_change=round(avg_change, 2),
            adv_ratio=round(adv_ratio * 100, 1),
            recommend=recommend,
        )

    df = df[df["Volume"] >= min_volume]
    df = df[df["ChagesRatio"] >= min_change_rate]
    df = df[df["ChagesRatio"] <= max_change_rate]
    if min_amount > 0:
        df = df[df["Amount"] >= min_amount]
    df = df[df["Code"].str[-1] == "0"]
    if max_price > 0:
        df = df[df["Close"] <= max_price]

    df = df.sort_values("Amount", ascending=False).head(top_n)

    result = []
    for _, row in df.iterrows():
        result.append(
            {
                "symbol": row["Code"],
                "name": row["Name"],
                "close": int(row["Close"]) if row["Close"] else 0,
                "change_rate": float(row["ChagesRatio"]) if row["ChagesRatio"] else 0.0,
                "volume": int(row["Volume"]) if row["Volume"] else 0,
                "amount": int(row["Amount"]) if row["Amount"] else 0,
            }
        )

    master_warned = load_warn_symbols_from_master()
    if master_warned:
        _warn_symbols.update(master_warned)
        before = len(result)
        result = [s for s in result if s["symbol"] not in _warn_symbols]
        excluded = before - len(result)
        if excluded:
            logger.info("market_universe.master_excluded", count=excluded)

    logger.info(
        "market_universe.scanned", filtered=len(df), top_n=top_n, passed=len(result)
    )
    return result


class QuantScreener:
    """OHLCV 히스토리 + 장중 실시간 데이터 기반 퀀트 종목 스크리너.

    장중 실시간 데이터 있을 때:
      샤프 25% + 일봉 거래량급증 10% + 유동성 10% + RSI 10% + ATR% 10%
      + 장중 거래량비율 20% + 장중 모멘텀 15%
    실시간 데이터 없을 때 (fallback):
      샤프 35% + 거래량급증 20% + 유동성 15% + RSI 15% + ATR% 15%
    """

    def __init__(
        self,
        ohlcv_repo: OhlcvRepository,
        max_stocks: int = 10,
        momentum_days: int = 20,
        min_avg_volume: int = 500_000,
        min_momentum: float = 0.0,
        ichimoku_filter: bool = False,
    ) -> None:
        self._repo = ohlcv_repo
        self._max_stocks = max_stocks
        self._momentum_days = momentum_days
        self._min_avg_volume = min_avg_volume
        self._min_momentum = min_momentum
        self._ichimoku_filter = ichimoku_filter
        self.symbol_names: dict[str, str] = {}
        self._last_scan: list[dict[str, Any]] = []

    def scan(
        self,
        symbols: list[str],
        held_symbols: list[str] | None = None,
        live_data: dict[str, dict] | None = None,
    ) -> list[str]:
        held = set(held_symbols or [])
        warned = get_warn_symbols()
        symbols = [s for s in symbols if s not in warned]
        scored: list[dict[str, Any]] = []

        candle_limit = max(self._momentum_days + 10, 62)
        cloud_stats = {"above": 0, "inside": 0, "below": 0, "unknown": 0}

        for sym in symbols:
            candles = self._repo.get_candles(
                sym, interval="1d", limit=candle_limit
            )
            if len(candles) < self._momentum_days:
                continue

            live = (live_data or {}).get(sym)
            factors = self._calc_factors(candles, live)
            if factors is None:
                continue

            if factors["avg_volume"] < self._min_avg_volume:
                continue
            if factors["momentum"] < self._min_momentum:
                continue

            cloud_pos = factors.get("cloud_position", "unknown")
            cloud_stats[cloud_pos] = cloud_stats.get(cloud_pos, 0) + 1
            if self._ichimoku_filter and cloud_pos == "below":
                continue

            scored.append({"symbol": sym, **factors})

        if self._ichimoku_filter:
            logger.info("quant_screener.cloud_filter", **cloud_stats)

        if not scored:
            logger.warning("quant_screener.no_candidates")
            self._last_scan = []
            return list(held)

        ranked = self._rank(scored)
        self._last_scan = ranked

        available_slots = max(0, self._max_stocks - len(held))
        new_symbols = [s["symbol"] for s in ranked if s["symbol"] not in held][
            :available_slots
        ]
        result = list(held) + new_symbols

        logger.info(
            "quant_screener.scan_complete",
            candidates=len(scored),
            selected=len(new_symbols),
            total=len(result),
        )
        return result

    def get_last_scan(self) -> list[dict[str, Any]]:
        return self._last_scan

    def _ichimoku_cloud_position(self, candles: list[dict]) -> str:
        """일봉 기준 일목균형 구름 대비 위치 판정."""
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]
        closes = [c["close"] for c in candles]
        n = len(candles)
        if n < 52:
            return "unknown"

        def midpoint(data_h: list, data_l: list, period: int) -> float:
            h = max(data_h[-period:])
            l = min(data_l[-period:])
            return (h + l) / 2

        tenkan = midpoint(highs, lows, 9)
        kijun = midpoint(highs, lows, 26)
        senkou_a = (tenkan + kijun) / 2
        senkou_b = midpoint(highs, lows, 52)
        cloud_top = max(senkou_a, senkou_b)
        cloud_bottom = min(senkou_a, senkou_b)
        price = closes[-1]

        if price > cloud_top:
            return "above"
        if price < cloud_bottom:
            return "below"
        return "inside"

    def _calc_factors(self, candles: list[dict], live: dict | None = None) -> dict[str, Any] | None:
        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]

        if closes[0] == 0 or len(closes) < 2:
            return None

        momentum = (closes[-1] - closes[0]) / closes[0]

        returns = [
            (closes[i] - closes[i - 1]) / closes[i - 1]
            for i in range(1, len(closes))
            if closes[i - 1] > 0
        ]
        volatility = _std(returns) if len(returns) > 1 else 0

        avg_return = sum(returns) / len(returns) if returns else 0
        sharpe = avg_return / volatility if volatility > 0 else 0

        avg_volume = sum(volumes) / len(volumes) if volumes else 0

        avg_turnover = (
            sum(c * v for c, v in zip(closes, volumes)) / len(closes) if closes else 0
        )

        recent_n = min(5, len(volumes))
        prior_n = len(volumes) - recent_n
        vol_recent = sum(volumes[-recent_n:]) / recent_n if recent_n > 0 else 0
        vol_prior = sum(volumes[:prior_n]) / prior_n if prior_n > 0 else vol_recent
        volume_surge = vol_recent / vol_prior if vol_prior > 0 else 1.0

        true_ranges = []
        for i in range(1, len(candles)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            true_ranges.append(tr)
        atr = sum(true_ranges) / len(true_ranges) if true_ranges else 0
        atr_pct = atr / closes[-1] if closes[-1] > 0 else 0

        rsi = self._calc_rsi(closes)

        cloud_pos = self._ichimoku_cloud_position(candles)

        intraday_vol_ratio = None
        intraday_change = None
        if live and avg_volume > 0:
            live_vol = live.get("volume", 0)
            if live_vol > 0:
                intraday_vol_ratio = round(live_vol / avg_volume, 2)
            intraday_change = live.get("change_rate")

        return {
            "momentum": round(momentum, 4),
            "volatility": round(volatility, 4),
            "sharpe": round(sharpe, 4),
            "avg_volume": int(avg_volume),
            "avg_turnover": int(avg_turnover),
            "volume_surge": round(volume_surge, 2),
            "atr_pct": round(atr_pct, 4),
            "rsi": round(rsi, 1) if rsi is not None else None,
            "close": closes[-1],
            "cloud_position": cloud_pos,
            "intraday_vol_ratio": intraday_vol_ratio,
            "intraday_change": intraday_change,
        }

    def _calc_rsi(self, closes: list[int], period: int = 14) -> float | None:
        if len(closes) < period + 1:
            return None
        changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        recent = changes[-period:]
        gains = [c for c in recent if c > 0]
        losses = [-c for c in recent if c < 0]
        avg_gain = sum(gains) / period if gains else 0
        avg_loss = sum(losses) / period if losses else 0
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _rank(self, scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not scored:
            return []

        has_live = any(s.get("intraday_vol_ratio") is not None for s in scored)

        sharpes = [s["sharpe"] for s in scored]
        surges = [s["volume_surge"] for s in scored]
        turnovers = [s["avg_turnover"] for s in scored]
        atrs = [s["atr_pct"] for s in scored]

        mu_sharpe, sd_sharpe = _mean_std(sharpes)
        mu_surge, sd_surge = _mean_std(surges)
        mu_turn, sd_turn = _mean_std(turnovers)
        mu_atr, sd_atr = _mean_std(atrs)

        if has_live:
            intra_vols = [s.get("intraday_vol_ratio", 1.0) for s in scored]
            intra_chgs = [s.get("intraday_change", 0.0) or 0.0 for s in scored]
            mu_ivol, sd_ivol = _mean_std(intra_vols)
            mu_ichg, sd_ichg = _mean_std(intra_chgs)

        for s in scored:
            z_sharpe = (s["sharpe"] - mu_sharpe) / sd_sharpe if sd_sharpe > 0 else 0
            z_surge = (s["volume_surge"] - mu_surge) / sd_surge if sd_surge > 0 else 0
            z_turn = (s["avg_turnover"] - mu_turn) / sd_turn if sd_turn > 0 else 0

            rsi_score = 0.0
            if s["rsi"] is not None:
                rsi_score = math.exp(-0.5 * ((s["rsi"] - 40) / 15) ** 2)

            z_atr = (s["atr_pct"] - mu_atr) / sd_atr if sd_atr > 0 else 0

            cloud_bonus = 0.3 if self._ichimoku_filter and s.get("cloud_position") == "above" else 0.0

            if has_live:
                ivr = s.get("intraday_vol_ratio", 1.0) or 1.0
                z_ivol = (ivr - mu_ivol) / sd_ivol if sd_ivol > 0 else 0
                ichg = s.get("intraday_change", 0.0) or 0.0
                z_ichg = (ichg - mu_ichg) / sd_ichg if sd_ichg > 0 else 0
                s["score"] = round(
                    z_sharpe * 0.25
                    + z_surge * 0.10
                    + z_turn * 0.10
                    + rsi_score * 0.10
                    + z_atr * 0.10
                    + z_ivol * 0.20
                    + z_ichg * 0.15
                    + cloud_bonus,
                    4,
                )
            else:
                s["score"] = round(
                    z_sharpe * 0.35
                    + z_surge * 0.20
                    + z_turn * 0.15
                    + rsi_score * 0.15
                    + z_atr * 0.15
                    + cloud_bonus,
                    4,
                )

        scored.sort(key=lambda s: s["score"], reverse=True)
        return scored[: self._max_stocks]


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(variance)


def _mean_std(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    mean = sum(values) / len(values)
    if len(values) < 2:
        return mean, 0.0
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return mean, math.sqrt(variance)
