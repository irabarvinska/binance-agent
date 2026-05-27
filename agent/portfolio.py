"""
Управління портфелем.
Відстежує баланси, розподіл 50/25/25, мілстоуни та конвертацію.
Зберігає дані в SQLite базі даних.
"""

import asyncio
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
import aiosqlite
from loguru import logger

from config import config


class Position:
    """Відкрита торгова позиція."""

    def __init__(
        self,
        symbol: str,
        side: str,         # 'BUY' або 'SELL'
        entry_price: float,
        quantity: float,
        strategy_type: str,   # 'moderate' або 'high_risk'
        order_id: Optional[str] = None,
    ):
        self.symbol = symbol
        self.side = side
        self.entry_price = entry_price
        self.quantity = quantity
        self.strategy_type = strategy_type
        self.order_id = order_id
        self.opened_at = datetime.utcnow()
        self.stop_loss = self._calc_stop_loss()
        self.take_profit = self._calc_take_profit()

    def _calc_stop_loss(self) -> float:
        """Розраховує рівень стоп-лосу."""
        if self.strategy_type == "moderate":
            pct = config.trading.moderate_stop_loss
        else:
            pct = config.trading.high_risk_stop_loss
        return self.entry_price * (1 + pct)

    def _calc_take_profit(self) -> float:
        """Розраховує рівень тейк-профіту."""
        if self.strategy_type == "moderate":
            pct = config.trading.moderate_take_profit
        else:
            pct = config.trading.high_risk_take_profit
        return self.entry_price * (1 + pct)

    def current_pnl_pct(self, current_price: float) -> float:
        """Поточний PnL у відсотках."""
        return (current_price - self.entry_price) / self.entry_price

    def value_usdt(self, current_price: float) -> float:
        """Поточна вартість позиції в USDT."""
        return self.quantity * current_price

    def should_stop_loss(self, current_price: float) -> bool:
        """Чи потрібно спрацювати стоп-лос."""
        return current_price <= self.stop_loss

    def should_take_profit(self, current_price: float) -> bool:
        """Чи потрібно спрацювати тейк-профіт."""
        return current_price >= self.take_profit


class DatabaseManager:
    """SQLite менеджер для зберігання угод та балансів."""

    def __init__(self, db_path: str):
        self._db_path = db_path

    async def initialize(self) -> None:
        """Створює таблиці якщо вони не існують."""
        async with aiosqlite.connect(self._db_path) as db:
            await db.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    strategy_type TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    exit_price REAL,
                    quantity REAL NOT NULL,
                    pnl_usdt REAL,
                    pnl_pct REAL,
                    status TEXT DEFAULT 'open',
                    opened_at TEXT NOT NULL,
                    closed_at TEXT,
                    order_id TEXT,
                    close_reason TEXT
                );

                CREATE TABLE IF NOT EXISTS balance_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    total_usdt REAL NOT NULL,
                    hodl_usdt REAL NOT NULL,
                    moderate_usdt REAL NOT NULL,
                    high_risk_usdt REAL NOT NULL,
                    balances_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS milestones (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    milestone REAL NOT NULL,
                    achieved_at TEXT NOT NULL,
                    balance_at_time REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
                CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
                CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON balance_snapshots(timestamp);
            """)
            await db.commit()
        logger.info("База даних ініціалізована")

    async def save_trade(self, trade: dict) -> int:
        """Зберігає угоду, повертає ID."""
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute(
                """INSERT INTO trades
                   (symbol, side, strategy_type, entry_price, quantity, status, opened_at, order_id)
                   VALUES (?, ?, ?, ?, ?, 'open', ?, ?)""",
                (
                    trade["symbol"], trade["side"], trade["strategy_type"],
                    trade["entry_price"], trade["quantity"],
                    trade.get("opened_at", datetime.utcnow().isoformat()),
                    trade.get("order_id"),
                ),
            )
            await db.commit()
            return cursor.lastrowid

    async def close_trade(
        self, trade_id: int, exit_price: float, pnl_usdt: float, pnl_pct: float, reason: str
    ) -> None:
        """Закриває угоду."""
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                """UPDATE trades SET
                   exit_price=?, pnl_usdt=?, pnl_pct=?, status='closed',
                   closed_at=?, close_reason=?
                   WHERE id=?""",
                (exit_price, pnl_usdt, pnl_pct, datetime.utcnow().isoformat(), reason, trade_id),
            )
            await db.commit()

    async def save_balance_snapshot(self, snapshot: dict) -> None:
        """Зберігає знімок балансу."""
        import json
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                """INSERT INTO balance_snapshots
                   (timestamp, total_usdt, hodl_usdt, moderate_usdt, high_risk_usdt, balances_json)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    datetime.utcnow().isoformat(),
                    snapshot["total_usdt"],
                    snapshot["hodl_usdt"],
                    snapshot["moderate_usdt"],
                    snapshot["high_risk_usdt"],
                    json.dumps(snapshot.get("balances", {})),
                ),
            )
            await db.commit()

    async def record_milestone(self, milestone: float, balance: float) -> None:
        """Записує досягнення мілстоуну."""
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "INSERT INTO milestones (milestone, achieved_at, balance_at_time) VALUES (?, ?, ?)",
                (milestone, datetime.utcnow().isoformat(), balance),
            )
            await db.commit()

    async def is_milestone_achieved(self, milestone: float) -> bool:
        """Перевіряє чи вже записаний мілстоун."""
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM milestones WHERE milestone=?", (milestone,)
            )
            row = await cursor.fetchone()
            return row[0] > 0

    async def get_today_trades(self) -> List[dict]:
        """Повертає угоди за сьогодні."""
        today = date.today().isoformat()
        async with aiosqlite.connect(self._db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM trades WHERE date(closed_at)=? ORDER BY closed_at DESC",
                (today,),
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

    async def get_open_trades(self) -> List[dict]:
        """Повертає відкриті угоди."""
        async with aiosqlite.connect(self._db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM trades WHERE status='open' ORDER BY opened_at DESC"
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

    async def get_last_balance(self) -> Optional[dict]:
        """Повертає останній знімок балансу."""
        async with aiosqlite.connect(self._db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM balance_snapshots ORDER BY timestamp DESC LIMIT 1"
            )
            row = await cursor.fetchone()
            return dict(row) if row else None


class PortfolioManager:
    """
    Управляє портфелем: балансами, позиціями, мілстоунами.
    """

    def __init__(self, db: DatabaseManager):
        self._db = db
        self._positions: Dict[str, Position] = {}  # {symbol: Position}
        self._balances: Dict[str, float] = {}       # {asset: кількість}
        self._prices: Dict[str, float] = {}         # {symbol: ціна в USDT}
        self._achieved_milestones: set = set()

        # Початковий баланс з конфігурації
        self._initial_balances = {
            "BTC": config.portfolio.initial_btc,
            "ETH": config.portfolio.initial_eth,
            "SOL": config.portfolio.initial_sol,
            "SUI": config.portfolio.initial_sui,
            "USDC": config.portfolio.initial_usdc,
            "USDT": config.portfolio.initial_usdt,
        }
        self._balances = dict(self._initial_balances)

    async def initialize(self) -> None:
        """Завантажує стан портфеля з бази даних."""
        await self._db.initialize()

        # Перевіряємо вже досягнуті мілстоуни
        for milestone in [config.portfolio.milestone_1, config.portfolio.milestone_2]:
            if await self._db.is_milestone_achieved(milestone):
                self._achieved_milestones.add(milestone)

        logger.info(f"Портфель ініціалізовано. Активи: {list(self._balances.keys())}")

    def update_price(self, symbol: str, price: float) -> None:
        """Оновлює ціну активу."""
        self._prices[symbol] = price

    def update_balance(self, asset: str, quantity: float) -> None:
        """Оновлює баланс активу."""
        self._balances[asset] = quantity

    def get_asset_price_usdt(self, asset: str) -> float:
        """Повертає ціну активу в USDT."""
        if asset in ("USDT", "USDC"):
            return 1.0
        symbol = f"{asset}USDT"
        return self._prices.get(symbol, 0.0)

    def calculate_total_usdt(self) -> float:
        """Розраховує загальну вартість портфеля в USDT."""
        total = 0.0
        for asset, qty in self._balances.items():
            price = self.get_asset_price_usdt(asset)
            total += qty * price
        # Додаємо відкриті позиції
        for pos in self._positions.values():
            price = self._prices.get(pos.symbol, pos.entry_price)
            total += pos.value_usdt(price)
        return total

    def get_portfolio_breakdown(self) -> Dict[str, float]:
        """
        Повертає розбивку портфеля за категоріями.
        """
        hodl_usdt = 0.0
        for asset in config.portfolio.hodl_assets:
            price = self.get_asset_price_usdt(asset)
            qty = self._balances.get(asset, 0.0)
            hodl_usdt += qty * price

        # USDT/USDC — вважаємо резервом
        stable_usdt = (
            self._balances.get("USDT", 0.0) +
            self._balances.get("USDC", 0.0)
        )

        # Позиції
        moderate_usdt = 0.0
        high_risk_usdt = 0.0
        for pos in self._positions.values():
            price = self._prices.get(pos.symbol, pos.entry_price)
            val = pos.value_usdt(price)
            if pos.strategy_type == "moderate":
                moderate_usdt += val
            else:
                high_risk_usdt += val

        # SUI та інші altcoins
        for asset, qty in self._balances.items():
            if asset not in config.portfolio.hodl_assets and asset not in ("USDT", "USDC"):
                price = self.get_asset_price_usdt(asset)
                moderate_usdt += qty * price

        total = hodl_usdt + moderate_usdt + high_risk_usdt + stable_usdt

        return {
            "total_usdt": total,
            "hodl_usdt": hodl_usdt,
            "moderate_usdt": moderate_usdt,
            "high_risk_usdt": high_risk_usdt,
            "stable_usdt": stable_usdt,
        }

    async def check_milestones(self, on_milestone_callback=None) -> List[float]:
        """
        Перевіряє чи досягнуто мілстоуни.
        Повертає список нових досягнутих мілстоунів.
        """
        total = self.calculate_total_usdt()
        achieved = []

        for milestone in [config.portfolio.milestone_1, config.portfolio.milestone_2]:
            if milestone not in self._achieved_milestones and total >= milestone:
                self._achieved_milestones.add(milestone)
                await self._db.record_milestone(milestone, total)
                achieved.append(milestone)
                logger.info(f"МІЛСТОУН ДОСЯГНУТО: ${milestone:,.0f}! Поточний баланс: ${total:,.2f}")

                if on_milestone_callback:
                    await on_milestone_callback(milestone, total)

        return achieved

    def check_monthly_conversion(self) -> bool:
        """
        Перевіряє чи потрібна щомісячна конвертація в USDT.
        Спрацьовує 20-го числа якщо баланс >= $5000.
        """
        today = date.today()
        total = self.calculate_total_usdt()

        return (
            today.day == config.portfolio.monthly_conversion_day and
            total >= config.portfolio.monthly_conversion_threshold
        )

    def add_position(self, position: Position) -> None:
        """Додає нову відкриту позицію."""
        self._positions[position.symbol] = position
        logger.info(
            f"Позиція відкрита: {position.symbol} {position.side} "
            f"x{position.quantity:.6f} @ ${position.entry_price:.4f}"
        )

    def remove_position(self, symbol: str) -> Optional[Position]:
        """Видаляє і повертає закриту позицію."""
        return self._positions.pop(symbol, None)

    def get_position(self, symbol: str) -> Optional[Position]:
        """Повертає відкриту позицію для символу."""
        return self._positions.get(symbol)

    def get_all_positions(self) -> List[Position]:
        """Повертає всі відкриті позиції."""
        return list(self._positions.values())

    def open_positions_count(self) -> int:
        """Кількість відкритих позицій."""
        return len(self._positions)

    def get_top_positions(self, n: int = 3) -> List[dict]:
        """Топ позиції за поточним PnL."""
        positions_info = []
        for pos in self._positions.values():
            price = self._prices.get(pos.symbol, pos.entry_price)
            pnl_pct = pos.current_pnl_pct(price) * 100
            positions_info.append({
                "symbol": pos.symbol,
                "pnl_pct": pnl_pct,
                "value_usdt": pos.value_usdt(price),
                "strategy": pos.strategy_type,
            })
        return sorted(positions_info, key=lambda x: abs(x["pnl_pct"]), reverse=True)[:n]

    async def save_snapshot(self) -> None:
        """Зберігає поточний знімок балансу."""
        breakdown = self.get_portfolio_breakdown()
        breakdown["balances"] = {
            asset: {"qty": qty, "price": self.get_asset_price_usdt(asset)}
            for asset, qty in self._balances.items()
        }
        await self._db.save_balance_snapshot(breakdown)
