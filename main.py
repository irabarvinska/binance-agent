"""
Точка входу Binance Trading Agent.
Запускає WebSocket клієнт, аналізатор, трейдер та Telegram бот.
"""

import asyncio
import signal
import sys
import os
from loguru import logger

from config import config
from agent.websocket_client import BinanceWebSocketClient
from agent.analyzer import MarketAnalyzer
from agent.portfolio import PortfolioManager, DatabaseManager
from agent.strategy import StrategyManager
from agent.telegram_bot import TelegramReporter
from agent.trader import BinanceTrader


def setup_logging() -> None:
    """Налаштовує loguru логування в файл та консоль."""
    os.makedirs("logs", exist_ok=True)
    os.makedirs("data", exist_ok=True)

    logger.remove()  # Видаляємо стандартний обробник

    # Консольне логування
    logger.add(
        sys.stderr,
        level=config.log_level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
        colorize=True,
    )

    # Файлове логування з ротацією
    logger.add(
        config.log_path,
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{function}:{line} | {message}",
        rotation="10 MB",   # Новий файл при досягненні 10MB
        retention="30 days", # Зберігаємо 30 днів
        compression="zip",   # Стискаємо старі файли
        encoding="utf-8",
    )

    logger.info("Логування налаштовано")


class TradingAgent:
    """
    Головний клас агента — координує всі компоненти.
    """

    def __init__(self):
        # Ініціалізація компонентів
        self._db = DatabaseManager(config.db_path)
        self._analyzer = MarketAnalyzer()
        self._portfolio = PortfolioManager(self._db)
        self._strategy = StrategyManager()
        self._telegram = TelegramReporter()
        self._trader = BinanceTrader(
            analyzer=self._analyzer,
            portfolio=self._portfolio,
            strategy=self._strategy,
            telegram=self._telegram,
            db=self._db,
        )
        self._ws = BinanceWebSocketClient()
        self._running = False

    async def initialize(self) -> bool:
        """Ініціалізує всі компоненти."""
        logger.info("=" * 50)
        logger.info("Binance Trading Agent v1.0")
        logger.info(f"Режим: {'TESTNET' if config.binance.testnet else 'MAINNET'}")
        logger.info("=" * 50)

        # Перевіряємо конфігурацію
        if not config.validate():
            logger.error("Помилка конфігурації! Перевірте .env файл")
            return False

        # Ініціалізуємо компоненти
        # Telegram першим — щоб алерти про помилки теж надходили
        await self._portfolio.initialize()
        await self._telegram.initialize()
        await self._trader.initialize()

        # Прив'язуємо портфель до Telegram для звітів
        self._telegram.set_portfolio(self._portfolio, self._db, self._analyzer)

        # Реєструємо WebSocket обробники
        self._ws.on("kline", self._on_kline)
        self._ws.on("ticker", self._on_ticker)
        self._ws.on("connected", self._on_ws_connected)
        self._ws.on("disconnected", self._on_ws_disconnected)
        self._ws.on("error", self._on_ws_error)

        logger.info("Всі компоненти ініціалізовано")
        return True

    async def _on_kline(self, data: dict) -> None:
        """Обробник нової свічки від WebSocket."""
        await self._trader.process_candle(data)

    async def _on_ticker(self, data: dict) -> None:
        """Обробник тікера від WebSocket."""
        symbol = data.get("symbol", "")
        price = data.get("last_price", 0)

        # Оновлюємо ціни в аналізаторі та портфелі
        self._analyzer.update_ticker(symbol, data)
        if price > 0:
            self._portfolio.update_price(symbol, price)

    async def _on_ws_connected(self, data: dict) -> None:
        """WebSocket підключено."""
        streams_count = len(data.get("streams", []))
        logger.info(f"WebSocket підключено. Стрімів: {streams_count}")
        # Зберігаємо знімок балансу при запуску
        await self._portfolio.save_snapshot()

    async def _on_ws_disconnected(self, data: dict) -> None:
        """WebSocket відключено."""
        logger.warning(f"WebSocket відключено: {data.get('reason', 'невідома причина')}")

    async def _on_ws_error(self, data: dict) -> None:
        """Помилка WebSocket."""
        logger.error(f"WebSocket помилка: {data.get('error', 'невідома')}")

    async def _periodic_snapshot(self) -> None:
        """Зберігає знімок балансу кожну годину."""
        while self._running:
            await asyncio.sleep(3600)  # 1 година
            try:
                await self._portfolio.save_snapshot()
                total = self._portfolio.calculate_total_usdt()
                logger.info(f"Поточний баланс: ${total:,.2f}")
            except Exception as e:
                logger.error(f"Помилка збереження знімка: {e}")

    async def run(self) -> None:
        """Запускає агента."""
        if not await self._initialize_with_retry():
            return

        self._running = True

        # Надсилаємо повідомлення про запуск
        await self._telegram.send_startup_message()

        logger.info("Агент запущено! Очікуємо WebSocket дані...")

        # Запускаємо завдання паралельно
        tasks = [
            asyncio.create_task(self._ws.connect(), name="websocket"),
            asyncio.create_task(self._periodic_snapshot(), name="snapshot"),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Агент отримав сигнал зупинки")
        finally:
            await self.shutdown()

    async def _initialize_with_retry(self, max_attempts: int = 3) -> bool:
        """Ініціалізація з повторними спробами."""
        for attempt in range(1, max_attempts + 1):
            try:
                return await self.initialize()
            except Exception as e:
                logger.error(f"Помилка ініціалізації (спроба {attempt}/{max_attempts}): {e}")
                if attempt < max_attempts:
                    await asyncio.sleep(5 * attempt)
        return False

    async def shutdown(self) -> None:
        """Зупиняє всі компоненти."""
        self._running = False
        logger.info("Зупинка агента...")

        await self._ws.disconnect()
        await self._trader.shutdown()
        await self._telegram.shutdown()

        logger.info("Агент зупинено")


def handle_shutdown(signum, frame, agent_task):
    """Обробник сигналів SIGINT/SIGTERM."""
    logger.info(f"Отримано сигнал {signum}, зупиняємо...")
    agent_task.cancel()


async def main() -> None:
    """Головна функція."""
    setup_logging()

    agent = TradingAgent()

    # Запускаємо у фоні
    agent_task = asyncio.create_task(agent.run())

    # Обробка Ctrl+C та SIGTERM
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda s=sig: handle_shutdown(s, None, agent_task)
        )

    try:
        await agent_task
    except asyncio.CancelledError:
        pass

    logger.info("Програма завершена")


if __name__ == "__main__":
    asyncio.run(main())
