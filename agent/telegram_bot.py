"""
Telegram бот для звітів та алертів.
Щоденний звіт о 11:00 (Київ), алерти при мілстоунах та великих угодах.
"""

import asyncio
from datetime import datetime, date
from typing import Optional, List
import pytz

from telegram import Bot
from telegram.error import TelegramError
from telegram.constants import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger

from config import config


class TelegramReporter:
    """
    Надсилає звіти та алерти в Telegram.
    Використовує APScheduler для щоденного звіту.
    """

    def __init__(self):
        self._bot: Optional[Bot] = None
        self._chat_id = config.telegram.chat_id
        self._scheduler = AsyncIOScheduler(timezone=config.timezone)
        self._portfolio_manager = None   # встановлюється ззовні через set_portfolio
        self._db_manager = None

    def set_portfolio(self, portfolio_manager, db_manager) -> None:
        """Прив'язує менеджер портфеля для генерації звітів."""
        self._portfolio_manager = portfolio_manager
        self._db_manager = db_manager

    async def initialize(self) -> None:
        """Ініціалізує бот та планувальник."""
        if not config.telegram.bot_token:
            logger.warning("TELEGRAM_BOT_TOKEN не встановлено — Telegram відключено")
            return

        self._bot = Bot(token=config.telegram.bot_token)

        # Перевіряємо з'єднання
        try:
            me = await self._bot.get_me()
            logger.info(f"Telegram бот підключено: @{me.username}")
        except TelegramError as e:
            logger.error(f"Помилка підключення Telegram: {e}")
            self._bot = None
            return

        # Плануємо щоденний звіт (11:00 Київ = 08:00 UTC)
        self._scheduler.add_job(
            self._send_daily_report,
            "cron",
            hour=config.telegram.daily_report_hour,
            minute=config.telegram.daily_report_minute,
            id="daily_report",
        )
        self._scheduler.start()
        logger.info(
            f"Планувальник запущено. Щоденний звіт о "
            f"{config.telegram.daily_report_hour:02d}:{config.telegram.daily_report_minute:02d} UTC"
        )

    async def send(self, text: str, parse_mode: str = ParseMode.HTML) -> bool:
        """Надсилає повідомлення в Telegram."""
        if not self._bot:
            logger.debug(f"[Telegram відключено] {text[:100]}")
            return False

        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=text,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
            return True
        except TelegramError as e:
            logger.error(f"Помилка надсилання Telegram: {e}")
            return False

    async def send_startup_message(self) -> None:
        """Повідомлення про запуск агента."""
        mode = "🔴 TESTNET" if config.binance.testnet else "🟢 MAINNET"
        text = (
            f"🤖 <b>Binance Trading Agent запущено</b>\n\n"
            f"Режим: {mode}\n"
            f"Стратегія: 50% HODl / 25% Помірний / 25% Високий ризик\n"
            f"Мілстоун 1: <b>${config.portfolio.milestone_1:,.0f}</b>\n"
            f"Мілстоун 2: <b>${config.portfolio.milestone_2:,.0f}</b>\n\n"
            f"Час запуску: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await self.send(text)

    async def send_milestone_alert(self, milestone: float, balance: float) -> None:
        """Алерт про досягнення мілстоуну."""
        milestone_num = "1️⃣" if milestone == config.portfolio.milestone_1 else "2️⃣"
        text = (
            f"🎉 <b>МІЛСТОУН ДОСЯГНУТО!</b> {milestone_num}\n\n"
            f"🏆 Ціль: <b>${milestone:,.0f}</b>\n"
            f"💰 Поточний баланс: <b>${balance:,.2f}</b>\n"
            f"📅 Дата: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
            f"{'🔥 Чудова робота! Йдемо до наступного мілстоуну!' if milestone < config.portfolio.milestone_2 else '🚀 Всі мілстоуни досягнуто! Продовжуємо зростати!'}"
        )
        await self.send(text)

    async def send_trade_alert(
        self,
        symbol: str,
        side: str,
        amount_usdt: float,
        price: float,
        strategy: str,
        reasons: List[str],
    ) -> None:
        """Алерт про велику угоду (> $20)."""
        if amount_usdt < config.trading.large_trade_alert:
            return

        emoji = "🟢" if side == "BUY" else "🔴"
        text = (
            f"{emoji} <b>{side} {symbol}</b>\n\n"
            f"💵 Сума: <b>${amount_usdt:.2f}</b>\n"
            f"💲 Ціна: ${price:.4f}\n"
            f"📋 Стратегія: {strategy}\n"
            f"📝 Причини:\n"
        )
        for reason in reasons[:3]:
            text += f"  • {reason}\n"
        text += f"\n⏰ {datetime.now().strftime('%H:%M:%S')}"
        await self.send(text)

    async def send_stop_loss_alert(
        self,
        symbol: str,
        entry_price: float,
        exit_price: float,
        pnl_usdt: float,
        strategy: str,
    ) -> None:
        """Алерт про спрацювання стоп-лосу."""
        pct = (exit_price - entry_price) / entry_price * 100
        text = (
            f"⛔ <b>СТОП-ЛОС СПРАЦЮВАВ</b>\n\n"
            f"Символ: <b>{symbol}</b>\n"
            f"Вхід: ${entry_price:.4f}\n"
            f"Вихід: ${exit_price:.4f}\n"
            f"PnL: <b>${pnl_usdt:.2f} ({pct:.2f}%)</b>\n"
            f"Стратегія: {strategy}\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
        )
        await self.send(text)

    async def send_take_profit_alert(
        self,
        symbol: str,
        entry_price: float,
        exit_price: float,
        pnl_usdt: float,
        strategy: str,
    ) -> None:
        """Алерт про спрацювання тейк-профіту."""
        pct = (exit_price - entry_price) / entry_price * 100
        text = (
            f"💚 <b>ТЕЙК-ПРОФІТ!</b>\n\n"
            f"Символ: <b>{symbol}</b>\n"
            f"Вхід: ${entry_price:.4f}\n"
            f"Вихід: ${exit_price:.4f}\n"
            f"PnL: <b>+${pnl_usdt:.2f} (+{pct:.2f}%)</b>\n"
            f"Стратегія: {strategy}\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
        )
        await self.send(text)

    async def send_monthly_conversion_alert(self, amount_usdt: float, balance: float) -> None:
        """Алерт про щомісячну конвертацію в USDT."""
        text = (
            f"🔄 <b>ЩОМІСЯЧНА КОНВЕРТАЦІЯ</b>\n\n"
            f"Конвертовано: <b>${amount_usdt:.0f} → USDT</b>\n"
            f"Баланс: ${balance:,.2f}\n"
            f"📅 {date.today().strftime('%d.%m.%Y')}"
        )
        await self.send(text)

    async def _send_daily_report(self) -> None:
        """Генерує та надсилає щоденний звіт."""
        if not self._portfolio_manager or not self._db_manager:
            logger.warning("Портфель не прив'язано — щоденний звіт пропущено")
            return

        try:
            report = await self._build_daily_report()
            await self.send(report)
            logger.info("Щоденний звіт надіслано")
        except Exception as e:
            logger.error(f"Помилка генерації щоденного звіту: {e}")

    async def _build_daily_report(self) -> str:
        """Будує текст щоденного звіту."""
        pm = self._portfolio_manager
        breakdown = pm.get_portfolio_breakdown()
        total = breakdown["total_usdt"]

        # Дані за день
        today_trades = await self._db_manager.get_today_trades()
        profitable = sum(1 for t in today_trades if (t.get("pnl_usdt") or 0) > 0)
        loss_count = sum(1 for t in today_trades if (t.get("pnl_usdt") or 0) < 0)
        pnl_today = sum(t.get("pnl_usdt") or 0 for t in today_trades)

        # Прогрес до Мілстоуну 1
        progress_m1 = min(100.0, total / config.portfolio.milestone_1 * 100)
        progress_m2 = min(100.0, total / config.portfolio.milestone_2 * 100)

        # Топ позиції
        top_positions = pm.get_top_positions(3)
        positions_text = ""
        if top_positions:
            for pos in top_positions:
                sign = "+" if pos["pnl_pct"] >= 0 else ""
                positions_text += (
                    f"  • {pos['symbol']}: {sign}{pos['pnl_pct']:.2f}% "
                    f"(${pos['value_usdt']:.2f})\n"
                )
        else:
            positions_text = "  Відкритих позицій немає\n"

        # Отримуємо market summary
        market_data = pm._latest_tickers if hasattr(pm, "_latest_tickers") else {}

        # Sentiment з останнього аналізу
        sentiment_score = getattr(pm, "_last_sentiment", 0.0)
        if sentiment_score > 0.2:
            sentiment_text = "🟢 Позитивний"
        elif sentiment_score < -0.2:
            sentiment_text = "🔴 Негативний"
        else:
            sentiment_text = "🟡 Нейтральний"

        pnl_sign = "+" if pnl_today >= 0 else ""
        date_str = datetime.now().strftime("%d.%m.%Y")

        report = (
            f"📊 <b>DAILY REPORT — {date_str}</b>\n\n"
            f"💰 Баланс: <b>${total:,.2f}</b> ({pnl_sign}${pnl_today:.2f} сьогодні)\n"
            f"📈 Прогрес:\n"
            f"  • Milestone 1 (${config.portfolio.milestone_1:,.0f}): <b>{progress_m1:.1f}%</b>\n"
            f"  • Milestone 2 (${config.portfolio.milestone_2:,.0f}): <b>{progress_m2:.1f}%</b>\n\n"
            f"<b>ПОРТФЕЛЬ:</b>\n"
            f"• HODl (50%): ${breakdown['hodl_usdt']:,.2f}\n"
            f"• Помірний ризик (25%): ${breakdown['moderate_usdt']:,.2f}\n"
            f"• Високий ризик (25%): ${breakdown['high_risk_usdt']:,.2f}\n"
            f"• Стейблкоїни: ${breakdown['stable_usdt']:,.2f}\n\n"
            f"<b>УГОДИ ЗА ДЕНЬ:</b> {len(today_trades)}\n"
            f"• Прибуткових: {profitable} ✅\n"
            f"• Збиткових: {loss_count} ❌\n\n"
            f"<b>ТОП ПОЗИЦІЇ:</b>\n"
            f"{positions_text}\n"
            f"<b>РИНКОВИЙ СЕНТИМЕНТ:</b> {sentiment_text}\n"
        )

        return report

    async def shutdown(self) -> None:
        """Зупиняє планувальник."""
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
        logger.info("Telegram планувальник зупинено")
