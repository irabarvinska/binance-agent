"""
Головний торговий модуль.
Координує WebSocket дані, аналіз, стратегії та виконання угод.
"""

import asyncio
from datetime import datetime
from typing import Optional, Dict
import time

from binance import AsyncClient
from binance.exceptions import BinanceAPIException, BinanceOrderException
from loguru import logger

from config import config
from agent.analyzer import MarketAnalyzer
from agent.portfolio import PortfolioManager, Position, DatabaseManager
from agent.strategy import StrategyManager, Signal, StrategyType
from agent.telegram_bot import TelegramReporter


# Простий rate limiter: не більше N запитів за секунду
class RateLimiter:
    def __init__(self, calls_per_second: float = 5.0):
        self._interval = 1.0 / calls_per_second
        self._last_call = 0.0

    async def wait(self) -> None:
        now = time.monotonic()
        wait_time = self._interval - (now - self._last_call)
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        self._last_call = time.monotonic()


class BinanceTrader:
    """
    Виконує торгові операції через Binance REST API.
    Координує: аналіз → стратегія → виконання → запис в БД → Telegram.
    """

    def __init__(
        self,
        analyzer: MarketAnalyzer,
        portfolio: PortfolioManager,
        strategy: StrategyManager,
        telegram: TelegramReporter,
        db: DatabaseManager,
    ):
        self._analyzer = analyzer
        self._portfolio = portfolio
        self._strategy = strategy
        self._telegram = telegram
        self._db = db
        self._client: Optional[AsyncClient] = None
        self._rate_limiter = RateLimiter(calls_per_second=3.0)

        # Час останнього аналізу для кожного символу
        self._last_analysis: Dict[str, float] = {}
        # Мінімальний інтервал між аналізами (секунди)
        self._analysis_cooldown = 60

    async def initialize(self) -> None:
        """Ініціалізує Binance клієнт."""
        self._client = await AsyncClient.create(
            api_key=config.binance.api_key,
            api_secret=config.binance.api_secret,
            testnet=config.binance.testnet,
        )
        logger.info(
            f"Binance клієнт ініціалізовано "
            f"({'testnet' if config.binance.testnet else 'mainnet'})"
        )

        # Синхронізуємо реальні баланси
        await self._sync_balances()
        # Завантажуємо історичні свічки — без цього аналіз не починається годинами
        await self._load_historical_data()

    async def _load_historical_data(self) -> None:
        """
        Завантажує 250 історичних свічок через REST API для кожного символу.
        Критично: без цього EMA200 потребує 200 годин накопичення через WebSocket,
        а агент ніколи не почне аналізувати після кожного редеплою.
        """
        from binance.enums import (
            KLINE_INTERVAL_1MINUTE,
            KLINE_INTERVAL_1HOUR,
            KLINE_INTERVAL_4HOUR,
        )
        interval_map = {
            "1m": KLINE_INTERVAL_1MINUTE,
            "1h": KLINE_INTERVAL_1HOUR,
            "4h": KLINE_INTERVAL_4HOUR,
        }

        total = 0
        for symbol in config.trading.watch_symbols:
            for interval in config.trading.kline_intervals:
                try:
                    await self._rate_limiter.wait()
                    klines = await self._client.get_klines(
                        symbol=symbol,
                        interval=interval_map.get(interval, interval),
                        limit=250,
                    )
                    for k in klines:
                        candle = {
                            "symbol": symbol,
                            "interval": interval,
                            "open_time": k[0],
                            "close_time": k[6],
                            "open": float(k[1]),
                            "high": float(k[2]),
                            "low": float(k[3]),
                            "close": float(k[4]),
                            "volume": float(k[5]),
                            "is_closed": True,
                            "quote_volume": float(k[7]),
                            "trades": int(k[8]),
                        }
                        self._analyzer.update_candle(symbol, interval, candle)
                    total += len(klines)
                    logger.debug(f"Завантажено {len(klines)} свічок {symbol}/{interval}")
                except Exception as e:
                    logger.error(f"Помилка завантаження {symbol}/{interval}: {e}")

        logger.info(f"Історичні дані завантажено: {total} свічок для {len(config.trading.watch_symbols)} символів")

    async def _sync_balances(self) -> None:
        """Синхронізує баланси з Binance акаунту."""
        try:
            await self._rate_limiter.wait()
            account = await self._client.get_account()
            for balance in account.get("balances", []):
                asset = balance["asset"]
                free = float(balance["free"])
                locked = float(balance["locked"])
                total = free + locked
                if total > 0:
                    self._portfolio.update_balance(asset, total)
                    logger.debug(f"Баланс {asset}: {total}")
            logger.info("Баланси синхронізовано з Binance")
        except BinanceAPIException as e:
            logger.error(f"Помилка синхронізації балансів: {e}")

    async def _get_available_usdt(self) -> float:
        """
        Повертає доступний USDT для торгівлі.
        Тільки USDT: ринковий ордер з quoteOrderQty списує саме USDT,
        USDC рахувати не можна — ордер впаде з insufficient balance.
        """
        return self._portfolio._balances.get("USDT", 0.0)

    async def process_candle(self, candle_data: dict) -> None:
        """
        Обробляє нову свічку:
        1. Зберігає дані
        2. Аналізує (якщо пройшов cooldown)
        3. Перевіряє стоп-лос/тейк-профіт
        4. Генерує та виконує сигнали
        """
        symbol = candle_data["symbol"]
        interval = candle_data["interval"]

        # Зберігаємо дані свічки в аналізатор
        self._analyzer.update_candle(symbol, interval, candle_data)

        # Оновлюємо ціну в портфелі
        self._portfolio.update_price(symbol, candle_data["close"])

        # Перевіряємо стоп-лос/тейк-профіт для відкритих позицій
        await self._check_position_exits(symbol, candle_data["close"])

        # Аналізуємо тільки на 1h свічках та якщо пройшов cooldown
        if interval != "1h" or not candle_data.get("is_closed"):
            return

        now = asyncio.get_event_loop().time()
        last = self._last_analysis.get(symbol, 0)
        if now - last < self._analysis_cooldown:
            return
        self._last_analysis[symbol] = now

        # Повний аналіз
        analysis = await self._analyzer.analyze(symbol, interval)
        if not analysis:
            return

        # Перевірка мілстоунів
        await self._portfolio.check_milestones(
            on_milestone_callback=self._telegram.send_milestone_alert
        )

        # Перевіряємо щомісячну конвертацію
        if self._portfolio.check_monthly_conversion():
            await self._execute_monthly_conversion()

        # Генеруємо торговий сигнал
        available_usdt = await self._get_available_usdt()
        if available_usdt < config.trading.min_trade_amount:
            logger.warning(
                f"Недостатньо USDT для торгівлі: ${available_usdt:.2f} "
                f"(мінімум ${config.trading.min_trade_amount:.2f}). "
                f"Поповніть USDT або конвертуйте USDC→USDT на Binance."
            )
            return

        has_position = self._portfolio.get_position(symbol) is not None
        signal = self._strategy.get_signal(
            symbol=symbol,
            analysis=analysis,
            available_usdt=available_usdt,
            open_positions_count=self._portfolio.open_positions_count(),
            has_position=has_position,
        )

        if not signal or signal.signal == Signal.HOLD:
            return

        # Виконуємо угоду
        if signal.signal == Signal.BUY and not has_position:
            await self._execute_buy(signal)
        elif signal.signal == Signal.SELL and has_position:
            await self._execute_sell(symbol, "signal")

    async def _check_position_exits(self, symbol: str, current_price: float) -> None:
        """Перевіряє чи потрібно закрити позицію (стоп-лос або тейк-профіт)."""
        position = self._portfolio.get_position(symbol)
        if not position:
            return

        if position.should_stop_loss(current_price):
            logger.warning(
                f"Стоп-лос: {symbol} @ ${current_price:.4f} "
                f"(вхід: ${position.entry_price:.4f})"
            )
            await self._execute_sell(symbol, "stop_loss")

        elif position.should_take_profit(current_price):
            logger.info(
                f"Тейк-профіт: {symbol} @ ${current_price:.4f} "
                f"(вхід: ${position.entry_price:.4f})"
            )
            await self._execute_sell(symbol, "take_profit")

    async def _execute_buy(self, signal) -> None:
        """Виконує BUY ордер."""
        symbol = signal.symbol
        amount_usdt = signal.suggested_amount_usdt

        if amount_usdt < config.trading.min_trade_amount:
            return

        try:
            await self._rate_limiter.wait()

            if config.binance.testnet:
                # В testnet режимі симулюємо ордер
                ticker = self._analyzer.get_ticker(symbol)
                price = ticker["last_price"] if ticker else 0.0
                if price <= 0:
                    return

                quantity = amount_usdt / price
                order_id = f"TEST_{int(asyncio.get_event_loop().time())}"
                logger.info(f"[TESTNET] BUY {symbol}: qty={quantity:.6f} @ ${price:.4f}")
            else:
                # Реальний ринковий ордер
                order = await self._client.order_market_buy(
                    symbol=symbol,
                    quoteOrderQty=amount_usdt,
                )
                quantity = float(order.get("executedQty", 0))
                quote_spent = float(order.get("cummulativeQuoteQty", 0))
                # Середня ціна виконання по всіх fills, а не тільки перший
                price = quote_spent / quantity if quantity > 0 else 0.0
                order_id = str(order["orderId"])
                if quantity <= 0 or price <= 0:
                    logger.error(f"BUY {symbol}: ордер не виконано (executedQty={quantity})")
                    return
                logger.info(f"BUY виконано: {symbol} qty={quantity:.6f} @ ${price:.4f}")

            # Зберігаємо позицію
            position = Position(
                symbol=symbol,
                side="BUY",
                entry_price=price,
                quantity=quantity,
                strategy_type=signal.strategy_type.value,
                order_id=order_id,
            )
            self._portfolio.add_position(position)

            # Зберігаємо в БД
            trade_id = await self._db.save_trade({
                "symbol": symbol,
                "side": "BUY",
                "strategy_type": signal.strategy_type.value,
                "entry_price": price,
                "quantity": quantity,
                "opened_at": datetime.utcnow().isoformat(),
                "order_id": order_id,
            })
            position.db_id = trade_id

            # Telegram алерт
            await self._telegram.send_trade_alert(
                symbol=symbol,
                side="BUY",
                amount_usdt=amount_usdt,
                price=price,
                strategy=signal.strategy_type.value,
                reasons=signal.reasons,
            )

        except BinanceAPIException as e:
            logger.error(f"Binance API помилка при BUY {symbol}: {e}")
        except BinanceOrderException as e:
            logger.error(f"Binance ордер помилка при BUY {symbol}: {e}")
        except Exception as e:
            logger.error(f"Неочікувана помилка при BUY {symbol}: {e}")

    async def _execute_sell(self, symbol: str, reason: str) -> None:
        """Виконує SELL ордер для відкритої позиції."""
        position = self._portfolio.get_position(symbol)
        if not position:
            return

        try:
            await self._rate_limiter.wait()
            ticker = self._analyzer.get_ticker(symbol)
            current_price = ticker["last_price"] if ticker else position.entry_price

            if config.binance.testnet:
                # В testnet режимі симулюємо
                logger.info(
                    f"[TESTNET] SELL {symbol}: qty={position.quantity:.6f} "
                    f"@ ${current_price:.4f} причина={reason}"
                )
            else:
                order = await self._client.order_market_sell(
                    symbol=symbol,
                    quantity=round(position.quantity, 6),
                )
                sold_qty = float(order.get("executedQty", 0))
                quote_received = float(order.get("cummulativeQuoteQty", 0))
                if sold_qty > 0 and quote_received > 0:
                    current_price = quote_received / sold_qty
                logger.info(f"SELL виконано: {symbol} @ ${current_price:.4f}")

            # Розраховуємо PnL
            pnl_pct = (current_price - position.entry_price) / position.entry_price
            pnl_usdt = position.quantity * (current_price - position.entry_price)

            logger.info(
                f"Позиція закрита: {symbol} | PnL: {pnl_pct*100:+.2f}% | "
                f"${pnl_usdt:+.2f} | причина: {reason}"
            )

            # Закриваємо в БД
            if hasattr(position, "db_id"):
                await self._db.close_trade(
                    trade_id=position.db_id,
                    exit_price=current_price,
                    pnl_usdt=pnl_usdt,
                    pnl_pct=pnl_pct,
                    reason=reason,
                )

            # Видаляємо позицію
            self._portfolio.remove_position(symbol)

            # Telegram алерти
            if reason == "stop_loss":
                await self._telegram.send_stop_loss_alert(
                    symbol=symbol,
                    entry_price=position.entry_price,
                    exit_price=current_price,
                    pnl_usdt=pnl_usdt,
                    strategy=position.strategy_type,
                )
            elif reason == "take_profit":
                await self._telegram.send_take_profit_alert(
                    symbol=symbol,
                    entry_price=position.entry_price,
                    exit_price=current_price,
                    pnl_usdt=pnl_usdt,
                    strategy=position.strategy_type,
                )

        except BinanceAPIException as e:
            logger.error(f"Binance API помилка при SELL {symbol}: {e}")
        except Exception as e:
            logger.error(f"Неочікувана помилка при SELL {symbol}: {e}")

    async def _execute_monthly_conversion(self) -> None:
        """
        Щомісячна конвертація $1000 в USDT.
        Продаємо частину активів для досягнення суми.
        """
        total = self._portfolio.calculate_total_usdt()
        available = await self._get_available_usdt()
        target = config.portfolio.monthly_conversion_amount

        if available >= target:
            logger.info(f"Щомісячна конвертація: вже є ${available:.2f} USDT")
            await self._telegram.send_monthly_conversion_alert(target, total)
            return

        needed = target - available
        logger.info(f"Щомісячна конвертація: потрібно ${needed:.2f} USDT")
        # Логіка продажу активів для конвертації
        # (спрощена версія — в реальному використанні потрібно визначити які активи продавати)
        await self._telegram.send_monthly_conversion_alert(target, total)

    async def shutdown(self) -> None:
        """Закриває з'єднання."""
        if self._client:
            await self._client.close_connection()
        logger.info("Binance клієнт закрито")
