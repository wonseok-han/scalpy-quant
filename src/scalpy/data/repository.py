from datetime import date
from decimal import Decimal

import structlog
from sqlalchemy import create_engine, func, inspect, select, text
from sqlalchemy.orm import Session

from scalpy.data.schema import Base, PositionRow, TradeRow

logger = structlog.get_logger()

_COMMISSION_RATE = Decimal("0.000147")
_SELL_TAX_RATE = Decimal("0.0018")


class TradeRepository:
    def __init__(self, database_url: str, mock: bool = True) -> None:
        self._engine = create_engine(database_url)
        self._mock = mock

    def create_tables(self) -> None:
        Base.metadata.create_all(self._engine)
        self._migrate_mock_column()

    def _migrate_mock_column(self) -> None:
        insp = inspect(self._engine)
        if not insp.has_table("trades"):
            return
        columns = {c["name"] for c in insp.get_columns("trades")}
        if "mock" not in columns:
            with self._engine.begin() as conn:
                conn.execute(text("ALTER TABLE trades ADD COLUMN mock BOOLEAN DEFAULT true"))
            logger.info("migration.added_mock_column")
        if "strategy" not in columns:
            with self._engine.begin() as conn:
                conn.execute(text("ALTER TABLE trades ADD COLUMN strategy VARCHAR(50) DEFAULT ''"))
                conn.execute(text("UPDATE trades SET strategy = 'factor' WHERE strategy = '' OR strategy IS NULL"))
                conn.execute(text("UPDATE positions SET strategy = 'factor' WHERE strategy = 'synced' OR strategy = ''"))
            logger.info("migration.added_strategy_column")
        if "reason" not in columns:
            with self._engine.begin() as conn:
                conn.execute(text("ALTER TABLE trades ADD COLUMN reason VARCHAR(30) DEFAULT ''"))
            logger.info("migration.added_reason_column")
        if "market" not in columns:
            with self._engine.begin() as conn:
                conn.execute(text("ALTER TABLE trades ADD COLUMN market VARCHAR(4) DEFAULT 'kr'"))
            logger.info("migration.added_market_column")
        self._migrate_price_columns()

    def _migrate_price_columns(self) -> None:
        """avg_price/ord_price 등 INTEGER → NUMERIC(15,4) 마이그레이션."""
        insp = inspect(self._engine)
        if not insp.has_table("trades"):
            return
        cols = {c["name"]: c for c in insp.get_columns("trades")}
        avg_col = cols.get("avg_price")
        if avg_col is None:
            return
        col_type = str(avg_col["type"])
        if "NUMERIC" in col_type.upper() or "DOUBLE" in col_type.upper() or "REAL" in col_type.upper():
            return
        price_cols = ["ord_price", "avg_price", "tot_ccld_amt", "fee", "pnl"]
        with self._engine.begin() as conn:
            for col in price_cols:
                if col in cols:
                    conn.execute(text(
                        f"ALTER TABLE trades ALTER COLUMN {col} TYPE NUMERIC(15,4) USING {col}::numeric"
                    ))
        logger.info("migration.converted_price_columns_to_numeric")

    def recreate_trades_table(self) -> None:
        TradeRow.__table__.drop(self._engine, checkfirst=True)
        TradeRow.__table__.create(self._engine, checkfirst=True)

    def sync_trades(self, trades: list[dict], reason_map: dict[str, str] | None = None, market: str = "kr", strategy_map: dict[str, str] | None = None) -> int:
        """ccld API 데이터를 DB에 upsert. (order_no, order_date) 기준."""
        if not trades:
            return 0
        reasons = reason_map or {}
        ext_strats = strategy_map or {}

        new_count = 0
        update_count = 0

        with Session(self._engine) as session:
            existing: dict[tuple[str, str], int] = {}
            trade_dates = {t.get("order_date", "") for t in trades if t.get("order_no")}
            trade_dates.discard("")
            if not trade_dates:
                trade_dates = {date.today().strftime("%Y%m%d")}
            rows = session.execute(
                select(TradeRow.order_no, TradeRow.order_date, TradeRow.tot_ccld_qty).where(
                    TradeRow.order_date.in_(trade_dates),
                    TradeRow.mock == self._mock,
                )
            ).all()
            for r in rows:
                existing[(r.order_no, r.order_date)] = r.tot_ccld_qty

            strat_map: dict[str, str] = {}
            pos_rows = session.scalars(
                select(PositionRow).where(PositionRow.closed_at.is_(None))
            ).all()
            for pr in pos_rows:
                strat_map[pr.symbol] = pr.strategy
            closed_pos = session.execute(
                select(PositionRow.symbol, PositionRow.strategy)
                .where(PositionRow.closed_at.isnot(None))
                .order_by(PositionRow.closed_at.desc())
            ).all()
            for cp in closed_pos:
                if cp.symbol not in strat_map:
                    strat_map[cp.symbol] = cp.strategy
            # positions 테이블에 없으면 기존 trades에서 같은 심볼의 strategy 참조
            existing_strats = session.execute(
                select(TradeRow.symbol, TradeRow.strategy).where(
                    TradeRow.strategy != "",
                    TradeRow.mock == self._mock,
                ).group_by(TradeRow.symbol, TradeRow.strategy)
            ).all()
            for es in existing_strats:
                if es.symbol not in strat_map:
                    strat_map[es.symbol] = es.strategy

            for t in trades:
                order_no = t.get("order_no", "")
                order_date = t.get("order_date", "") or date.today().strftime("%Y%m%d")
                if not order_no:
                    continue

                key = (order_no, order_date)
                prev_qty = existing.get(key)

                if prev_qty is not None and prev_qty == t.get("tot_ccld_qty", 0):
                    continue

                fee = self._calc_fee(t)

                if prev_qty is None:
                    symbol = t.get("symbol", "")
                    side_val = t.get("side", "")
                    reason = reasons.get(symbol, "")
                    if not reason:
                        reason = "signal" if side_val in ("buy", "sell") else ""
                    row = TradeRow(
                        order_no=order_no,
                        order_date=order_date,
                        symbol=symbol,
                        name=t.get("name", ""),
                        side=side_val,
                        ord_qty=t.get("ord_qty", 0),
                        ord_price=t.get("ord_price", 0),
                        ord_time=t.get("ord_time", ""),
                        tot_ccld_qty=t.get("tot_ccld_qty", 0),
                        avg_price=t.get("avg_price", 0),
                        tot_ccld_amt=t.get("tot_ccld_amt", 0),
                        rmn_qty=t.get("rmn_qty", 0),
                        orgn_order_no=t.get("orgn_order_no", ""),
                        ord_dvsn_cd=t.get("ord_dvsn_cd", ""),
                        cncl_yn=t.get("cncl_yn", ""),
                        strategy=strat_map.get(symbol, "") or ext_strats.get(symbol, ""),
                        reason=reason,
                        fee=fee,
                        mock=self._mock,
                        market=market,
                    )
                    session.add(row)
                    existing[key] = t.get("tot_ccld_qty", 0)
                    new_count += 1
                else:
                    session.execute(
                        TradeRow.__table__.update()
                        .where(
                            TradeRow.order_no == order_no,
                            TradeRow.order_date == order_date,
                        )
                        .values(
                            tot_ccld_qty=t.get("tot_ccld_qty", 0),
                            avg_price=t.get("avg_price", 0),
                            tot_ccld_amt=t.get("tot_ccld_amt", 0),
                            rmn_qty=t.get("rmn_qty", 0),
                            fee=fee,
                        )
                    )
                    existing[key] = t.get("tot_ccld_qty", 0)
                    update_count += 1

            total = new_count + update_count
            if total:
                session.commit()
                for td in trade_dates:
                    self._recalc_pnl(session, td)
                logger.info("trade_sync.committed", new=new_count, updated=update_count)
        return new_count + update_count

    def _calc_fee(self, t: dict) -> float:
        amt = float(t.get("tot_ccld_amt", 0))
        commission = round(amt * float(_COMMISSION_RATE), 4)
        if t.get("side") == "sell":
            tax = round(amt * float(_SELL_TAX_RATE), 4)
            return round(commission + tax, 4)
        return commission

    def _recalc_pnl(self, session: Session, order_date: str) -> None:
        """sync 완료 후 sell 건의 pnl을 FIFO 매칭으로 재계산 (전체 기간 매수 대상)."""
        symbols = [r[0] for r in session.execute(
            select(TradeRow.symbol).where(
                TradeRow.order_date == order_date,
                TradeRow.mock == self._mock,
            ).group_by(TradeRow.symbol)
        ).all()]

        for symbol in symbols:
            buys = session.scalars(
                select(TradeRow).where(
                    TradeRow.symbol == symbol,
                    TradeRow.side == "buy",
                    TradeRow.mock == self._mock,
                ).order_by(TradeRow.order_date, TradeRow.ord_time)
            ).all()
            sells = session.scalars(
                select(TradeRow).where(
                    TradeRow.symbol == symbol,
                    TradeRow.side == "sell",
                    TradeRow.mock == self._mock,
                ).order_by(TradeRow.order_date, TradeRow.ord_time)
            ).all()

            buy_queue: list[tuple[int, float, float]] = []
            for b in buys:
                if b.tot_ccld_qty > 0 and b.avg_price > 0:
                    buy_queue.append((b.tot_ccld_qty, float(b.avg_price), float(b.fee)))

            qi = 0
            remaining = buy_queue[0][0] if buy_queue else 0

            for sell in sells:
                if sell.avg_price == 0 or sell.tot_ccld_qty == 0:
                    sell.pnl = None
                    continue

                to_match = sell.tot_ccld_qty
                total_buy_cost = 0.0
                total_buy_fee = 0.0

                while to_match > 0 and qi < len(buy_queue):
                    take = min(to_match, remaining)
                    total_buy_cost += take * buy_queue[qi][1]
                    total_buy_fee += buy_queue[qi][2] * take / buy_queue[qi][0]
                    to_match -= take
                    remaining -= take
                    if remaining <= 0:
                        qi += 1
                        remaining = buy_queue[qi][0] if qi < len(buy_queue) else 0

                if to_match > 0:
                    sell.pnl = None
                    continue

                matched_qty = sell.tot_ccld_qty
                buy_avg = total_buy_cost / matched_qty
                sell.pnl = round((float(sell.avg_price) - buy_avg) * matched_qty - float(sell.fee) - total_buy_fee, 4)

        session.commit()

    def correct_pnl_from_api(self, profit_records: list[dict]) -> int:
        """TTTC8715R 종목별 실제 pnl(수수료/세금 차감 후)로 DB 매도 건을 보정."""
        if not profit_records:
            return 0
        today = date.today().strftime("%Y%m%d")
        corrected = 0
        with Session(self._engine) as session:
            for rec in profit_records:
                symbol = rec.get("symbol", "")
                if not symbol or rec.get("side") != "sell":
                    continue
                actual_pnl_str = rec.get("pnl", "")
                if not actual_pnl_str:
                    continue
                actual_pnl = int(actual_pnl_str)

                sells = session.scalars(
                    select(TradeRow).where(
                        TradeRow.symbol == symbol,
                        TradeRow.side == "sell",
                        TradeRow.order_date == today,
                        TradeRow.mock == self._mock,
                    ).order_by(TradeRow.ord_time)
                ).all()
                if not sells:
                    continue

                if len(sells) == 1:
                    sells[0].pnl = actual_pnl
                    corrected += 1
                else:
                    fifo_total = sum(s.pnl for s in sells if s.pnl is not None)
                    if fifo_total == actual_pnl:
                        continue
                    has_fifo = all(s.pnl is not None for s in sells)
                    if has_fifo:
                        diff = actual_pnl - fifo_total
                        sells[-1].pnl += diff
                        corrected += 1
                    else:
                        total_amt = sum(s.tot_ccld_amt for s in sells) or 1
                        pnl_remaining = actual_pnl
                        for i, s in enumerate(sells):
                            if i == len(sells) - 1:
                                s.pnl = pnl_remaining
                            else:
                                s.pnl = int(actual_pnl * s.tot_ccld_amt / total_amt)
                                pnl_remaining -= s.pnl
                            corrected += 1

            if corrected:
                session.commit()
                logger.info("trade_sync.pnl_corrected", count=corrected)
        return corrected

    def recalc_all_pnl(self) -> int:
        """전체 종목의 pnl을 FIFO로 재계산. 반환값: 갱신된 sell 건수."""
        with Session(self._engine) as session:
            symbols = [r[0] for r in session.execute(
                select(TradeRow.symbol).where(
                    TradeRow.side == "sell",
                    TradeRow.mock == self._mock,
                ).group_by(TradeRow.symbol)
            ).all()]

            updated = 0
            for symbol in symbols:
                buys = session.scalars(
                    select(TradeRow).where(
                        TradeRow.symbol == symbol,
                        TradeRow.side == "buy",
                        TradeRow.mock == self._mock,
                    ).order_by(TradeRow.order_date, TradeRow.ord_time)
                ).all()
                sells = session.scalars(
                    select(TradeRow).where(
                        TradeRow.symbol == symbol,
                        TradeRow.side == "sell",
                        TradeRow.mock == self._mock,
                    ).order_by(TradeRow.order_date, TradeRow.ord_time)
                ).all()

                buy_queue: list[tuple[int, float, float]] = []
                for b in buys:
                    if b.tot_ccld_qty > 0 and b.avg_price > 0:
                        buy_queue.append((b.tot_ccld_qty, float(b.avg_price), float(b.fee)))

                qi = 0
                remaining = buy_queue[0][0] if buy_queue else 0

                for sell in sells:
                    if sell.avg_price == 0 or sell.tot_ccld_qty == 0:
                        sell.pnl = None
                        continue

                    to_match = sell.tot_ccld_qty
                    total_buy_cost = 0.0
                    total_buy_fee = 0.0

                    while to_match > 0 and qi < len(buy_queue):
                        take = min(to_match, remaining)
                        total_buy_cost += take * buy_queue[qi][1]
                        total_buy_fee += buy_queue[qi][2] * take / buy_queue[qi][0]
                        to_match -= take
                        remaining -= take
                        if remaining <= 0:
                            qi += 1
                            remaining = buy_queue[qi][0] if qi < len(buy_queue) else 0

                    if to_match > 0:
                        sell.pnl = None
                        continue

                    matched_qty = sell.tot_ccld_qty
                    buy_avg = total_buy_cost / matched_qty
                    sell.pnl = round((float(sell.avg_price) - buy_avg) * matched_qty - float(sell.fee) - total_buy_fee, 4)
                    updated += 1

            session.commit()
            logger.info("recalc_all_pnl.done", symbols=len(symbols), updated=updated)
            return updated

    def _daily_dates(self, day: date | None, market: str | None) -> list[str]:
        from datetime import datetime, timedelta
        now = datetime.now()
        if day:
            return [day.strftime("%Y%m%d")]
        today = now.date()
        if market == "us" and now.hour < 10:
            yesterday = today - timedelta(days=1)
            return [today.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")]
        return [today.strftime("%Y%m%d")]

    def get_daily_pnl(self, day: date | None = None, market: str | None = None) -> float:
        dates = self._daily_dates(day, market)
        with Session(self._engine) as session:
            q = select(func.coalesce(func.sum(TradeRow.pnl), 0)).where(
                TradeRow.side == "sell",
                TradeRow.order_date.in_(dates),
                TradeRow.mock == self._mock,
            )
            if market:
                q = q.where(TradeRow.market == market)
            return float(session.scalar(q) or 0)

    def get_daily_trade_count(self, day: date | None = None, market: str | None = None) -> int:
        dates = self._daily_dates(day, market)
        with Session(self._engine) as session:
            q = select(func.count(TradeRow.id)).where(
                TradeRow.order_date.in_(dates),
                TradeRow.mock == self._mock,
            )
            if market:
                q = q.where(TradeRow.market == market)
            return session.scalar(q) or 0

    def get_daily_fees(self, day: date | None = None, market: str | None = None) -> float:
        dates = self._daily_dates(day, market)
        with Session(self._engine) as session:
            q = select(func.coalesce(func.sum(TradeRow.fee), 0)).where(
                TradeRow.order_date.in_(dates),
                TradeRow.mock == self._mock,
            )
            if market:
                q = q.where(TradeRow.market == market)
            return float(session.scalar(q) or 0)


    def get_strategy_performance(self, day: date | None = None, market: str | None = None) -> dict[str, dict]:
        """trades 테이블에서 전략별 라운드트립(매수→매도) 기반 성과 집계."""
        dates = self._daily_dates(day, market)
        with Session(self._engine) as session:
            q = select(TradeRow.symbol).where(
                TradeRow.side == "sell",
                TradeRow.order_date.in_(dates),
                TradeRow.mock == self._mock,
                TradeRow.strategy != "",
            )
            if market:
                q = q.where(TradeRow.market == market)
            symbols = [r[0] for r in session.execute(q.group_by(TradeRow.symbol)).all()]

            stats: dict[str, dict] = {}
            for symbol in symbols:
                bq = select(TradeRow).where(
                    TradeRow.symbol == symbol,
                    TradeRow.side == "buy",
                    TradeRow.mock == self._mock,
                )
                if market:
                    bq = bq.where(TradeRow.market == market)
                buys = session.scalars(
                    bq.order_by(TradeRow.order_date, TradeRow.ord_time)
                ).all()
                sq = select(TradeRow).where(
                    TradeRow.symbol == symbol,
                    TradeRow.side == "sell",
                    TradeRow.order_date.in_(dates),
                    TradeRow.mock == self._mock,
                )
                if market:
                    sq = sq.where(TradeRow.market == market)
                sells = session.scalars(
                    sq.order_by(TradeRow.order_date, TradeRow.ord_time)
                ).all()

                buy_queue: list[tuple[int, float, str, str, float]] = []
                for b in buys:
                    if b.tot_ccld_qty > 0 and b.avg_price > 0:
                        buy_queue.append((b.tot_ccld_qty, float(b.avg_price), b.order_date, b.strategy, float(b.fee)))

                qi = 0
                remaining = buy_queue[0][0] if buy_queue else 0

                for sell in sells:
                    if sell.avg_price == 0 or sell.tot_ccld_qty == 0:
                        continue
                    strat = sell.strategy or (buy_queue[qi][3] if qi < len(buy_queue) else "")
                    if not strat:
                        continue

                    to_match = sell.tot_ccld_qty
                    total_buy_cost = 0.0
                    total_buy_fee = 0.0
                    buy_date = buy_queue[qi][2] if qi < len(buy_queue) else sell.order_date

                    while to_match > 0 and qi < len(buy_queue):
                        take = min(to_match, remaining)
                        total_buy_cost += take * buy_queue[qi][1]
                        total_buy_fee += buy_queue[qi][4] * take / buy_queue[qi][0]
                        buy_date = buy_queue[qi][2]
                        to_match -= take
                        remaining -= take
                        if remaining <= 0:
                            qi += 1
                            remaining = buy_queue[qi][0] if qi < len(buy_queue) else 0

                    if to_match > 0:
                        continue

                    matched_qty = sell.tot_ccld_qty
                    buy_avg = total_buy_cost / matched_qty
                    if sell.pnl is not None:
                        pnl = float(sell.pnl)
                    else:
                        pnl = round((float(sell.avg_price) - buy_avg) * matched_qty - float(sell.fee) - total_buy_fee, 4)
                    buy_total = buy_avg * matched_qty + total_buy_fee
                    pnl_pct = round(pnl / buy_total * 100, 2) if buy_total > 0 else 0.0
                    cross_day = buy_date != sell.order_date

                    s = stats.setdefault(strat, {
                        "trades": 0, "wins": 0, "losses": 0,
                        "total_pnl": 0,
                        "max_drawdown": 0, "_peak": 0,
                        "intraday_trades": 0, "cross_day_trades": 0,
                        "avg_pnl_pct": 0.0, "_pnl_pcts": [],
                    })
                    s["trades"] += 1
                    s["total_pnl"] += pnl
                    s["_pnl_pcts"].append(pnl_pct)
                    if pnl > 0:
                        s["wins"] += 1
                    elif pnl < 0:
                        s["losses"] += 1
                    if cross_day:
                        s["cross_day_trades"] += 1
                    else:
                        s["intraday_trades"] += 1
                    if s["total_pnl"] > s["_peak"]:
                        s["_peak"] = s["total_pnl"]
                    dd = s["_peak"] - s["total_pnl"]
                    if dd > s["max_drawdown"]:
                        s["max_drawdown"] = dd

            for s in stats.values():
                s["win_rate"] = round(s["wins"] / s["trades"] * 100, 1) if s["trades"] > 0 else 0.0
                pcts = s.pop("_pnl_pcts")
                s["avg_pnl_pct"] = round(sum(pcts) / len(pcts), 2) if pcts else 0.0
                s["total_pnl"] = str(s["total_pnl"])
                s["max_drawdown"] = str(s["max_drawdown"])
                del s["_peak"]
            return stats

    def get_daily_performance_history(self, market: str | None = None) -> list[dict]:
        """일자별 전략 성과 히스토리."""
        with Session(self._engine) as session:
            q = select(TradeRow.order_date).where(
                TradeRow.side == "sell",
                TradeRow.mock == self._mock,
            )
            if market:
                q = q.where(TradeRow.market == market)
            dates = [r[0] for r in session.execute(
                q.group_by(TradeRow.order_date)
                .order_by(TradeRow.order_date.desc())
            ).all()]

        result = []
        cumulative_pnl = 0
        for day_str in reversed(dates):
            from datetime import datetime as dt
            day = dt.strptime(day_str, "%Y%m%d").date()
            perf = self.get_strategy_performance(day=day, market=market)
            day_trades = 0
            day_wins = 0
            day_losses = 0
            day_pnl = 0
            for s in perf.values():
                day_trades += s["trades"]
                day_wins += s["wins"]
                day_losses += s["losses"]
                day_pnl += float(s["total_pnl"])
            cumulative_pnl += day_pnl
            result.append({
                "date": f"{day_str[:4]}-{day_str[4:6]}-{day_str[6:]}",
                "trades": day_trades,
                "wins": day_wins,
                "losses": day_losses,
                "win_rate": round(day_wins / day_trades * 100, 1) if day_trades > 0 else 0.0,
                "pnl": day_pnl,
                "cumulative_pnl": cumulative_pnl,
            })
        result.reverse()
        return result

    def save_position_open(
        self, symbol: str, strategy: str, opened_at: "datetime | None" = None,
    ) -> None:
        from datetime import datetime as dt
        opened = opened_at or dt.now()
        with Session(self._engine) as session:
            existing = session.scalar(
                select(PositionRow).where(
                    PositionRow.symbol == symbol,
                    PositionRow.closed_at.is_(None),
                )
            )
            if existing:
                existing.closed_at = dt.now()
            session.add(PositionRow(
                symbol=symbol, side="buy", quantity=0,
                avg_price=0, strategy=strategy, opened_at=opened,
            ))
            session.commit()

    def close_position(self, symbol: str) -> None:
        from datetime import datetime as dt
        with Session(self._engine) as session:
            row = session.scalar(
                select(PositionRow).where(
                    PositionRow.symbol == symbol,
                    PositionRow.closed_at.is_(None),
                )
            )
            if row:
                row.closed_at = dt.now()
                session.commit()

    def get_open_position_times(self) -> dict[str, "datetime"]:
        with Session(self._engine) as session:
            rows = session.scalars(
                select(PositionRow).where(PositionRow.closed_at.is_(None))
            ).all()
            return {r.symbol: r.opened_at for r in rows}

    def get_position_strategies(self) -> dict[str, str]:
        """보유 종목별 전략명 조회. positions 테이블 우선, 없으면 trades 최근 매수 건."""
        with Session(self._engine) as session:
            result: dict[str, str] = {}
            pos_rows = session.scalars(
                select(PositionRow).where(PositionRow.closed_at.is_(None))
            ).all()
            for r in pos_rows:
                if r.strategy:
                    result[r.symbol] = r.strategy

            missing = session.scalars(
                select(TradeRow.symbol).where(
                    TradeRow.side == "buy",
                    TradeRow.strategy != "",
                    TradeRow.mock == self._mock,
                ).group_by(TradeRow.symbol)
            ).all()
            for symbol in missing:
                if symbol in result:
                    continue
                row = session.scalar(
                    select(TradeRow.strategy).where(
                        TradeRow.symbol == symbol,
                        TradeRow.side == "buy",
                        TradeRow.strategy != "",
                        TradeRow.mock == self._mock,
                    ).order_by(TradeRow.ord_time.desc()).limit(1)
                )
                if row:
                    result[symbol] = row
            return result

    def get_trades_today(self, day: date | None = None, market: str | None = None) -> list[dict]:
        from datetime import datetime as _dt
        now = _dt.now()
        if day:
            dates = [(day).strftime("%Y%m%d")]
        elif market == "us" and now.hour < 10:
            today = now.date()
            yesterday = today - __import__("datetime").timedelta(days=1)
            dates = [today.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")]
        else:
            dates = [now.strftime("%Y%m%d")]
        with Session(self._engine) as session:
            q = select(TradeRow).where(
                TradeRow.order_date.in_(dates), TradeRow.mock == self._mock,
            )
            if market:
                q = q.where(TradeRow.market == market)
            rows = session.scalars(q.order_by(TradeRow.ord_time.desc())).all()

            result = []
            for r in rows:
                entry: dict = {
                    "order_no": r.order_no,
                    "symbol": r.symbol,
                    "name": r.name,
                    "side": r.side,
                    "strategy": r.strategy or "",
                    "reason": getattr(r, "reason", "") or "",
                    "price": str(r.avg_price),
                    "quantity": r.tot_ccld_qty,
                    "ord_qty": r.ord_qty,
                    "ord_price": str(r.ord_price),
                    "tot_ccld_amt": str(r.tot_ccld_amt),
                    "rmn_qty": r.rmn_qty,
                    "fee": str(r.fee),
                    "pnl": str(r.pnl) if r.pnl is not None else "",
                    "pnl_pct": "",
                    "time": f"{r.ord_time[:2]}:{r.ord_time[2:4]}:{r.ord_time[4:6]}" if len(r.ord_time) >= 6 else r.ord_time,
                }
                if r.side == "sell" and r.pnl is not None and r.tot_ccld_amt > 0:
                    buy_cost = r.tot_ccld_amt - r.pnl + r.fee
                    if buy_cost > 0:
                        entry["pnl_pct"] = round(r.pnl / buy_cost * 100, 2)
                result.append(entry)
            return result
