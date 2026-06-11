"""
Telegram бот для звітів та алертів.
Щоденний звіт о 11:00 (Київ), алерти при мілстоунах та великих угодах.
Команди: /report — звіт зараз, /status — статус агента, /balance — баланс.
"""

import asyncio
from datetime import datetime, date
from typing import Optional, List
import pytz

from telegram import Update
from telegram.error import TelegramError
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger

from config import config


class TelegramReporter:
    """
    Надсилає звіти та алерти в Telegram.
    Слухає команди: /report, /status, /balance.
    Один Application для надсилання і отримання (уникаємо конфлікту Bot + Application).
    """

    def __init__(self):
        self._application: Optional[Application] = None
        self._chat_id = config.telegram.chat_id
        self._scheduler = AsyncIOScheduler(timezone=config.timezone)
        self._portfolio_manager = None
        self._db_manager = None
        self._analyzer = None

    def set_portfolio(self, portfolio_manager, db_manager, analyzer=None) -> None:
        """Прив'язує менеджер портфеля для генерації звітів."""
        self._portfolio_manager = portfolio_manager
        self._db_manager = db_manager
        self._analyzer = analyzer

    async def initialize(self) -> None:
        """Ініціалізує бот, планувальник та обробник команд."""
        if not config.telegram.bot_token:
            logger.warning("TELEGRAM_BOT_TOKEN не встановлено — Telegram відключено")
            return

        # Один Application — і для надсилання, і для отримання команд.
        # Окремий Bot() з тим самим токеном конфліктує з polling.
        self._application = (
            Application.builder()
            .token(config.telegram.bot_token)
            .build()
        )
        self._application.add_handler(CommandHandler("start", self._cmd_start))
        self._application.add_handler(CommandHandler("report", self._cmd_report))
        self._application.add_handler(CommandHandler("status", self._cmd_status))
        self._application.add_handler(CommandHandler("balance", self._cmd_balance))
        self._application.add_error_handler(self._on_handler_error)

        await self._application.initialize()

        try:
            me = await self._application.bot.get_me()
            logger.info(f"Telegram бот підключено: @{me.username}")
        except TelegramError as e:
            logger.error(f"Помилка підключення Telegram: {e}")
            self._application = None
            return

        await self._application.start()
        await self._application.updater.start_polling(drop_pending_updates=True)
        logger.info("Telegram команди активні: /report /status /balance")

        # Щоденний звіт за розкладом (11:00 Київ = 08:00 UTC)
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
            f"{config.telegram.daily_report_hour:02d}:{config.telegram.daily_report_minute:02d} ({config.timezone})"
        )

    # ──────────────────────────────────────────
    # Обробники команд
    # ──────────────────────────────────────────

    async def _on_handler_error(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Логуємо помилки в обробниках команд — інакше вони губляться."""
        logger.error(f"Помилка в Telegram обробнику: {context.error}")

    async def _is_authorized(self, update: Update) -> bool:
        """Дозволяємо команди тільки з нашого chat_id."""
        incoming = str(update.effective_chat.id)
        authorized = incoming == str(self._chat_id).strip()
        if not authorized:
            logger.warning(f"Команда від невідомого chat_id={incoming} (очікувався {self._chat_id})")
            # Відповідаємо щоб користувач побачив розбіжність і виправив TELEGRAM_CHAT_ID
            await update.message.reply_text(
                f"⛔ Цей чат не авторизований.\n"
                f"Ваш chat_id: {incoming}\n"
                f"Встановіть TELEGRAM_CHAT_ID={incoming} в Railway Variables."
            )
        return authorized

    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """/start — діагностика: показує chat_id і чи збігається з конфігурацією."""
        incoming = str(update.effective_chat.id)
        match = "✅ збігається з TELEGRAM_CHAT_ID" if incoming == str(self._chat_id).strip() \
            else f"❌ НЕ збігається з TELEGRAM_CHAT_ID ({self._chat_id})"
        await update.message.reply_text(
            f"🤖 Binance Trading Agent\n\n"
            f"Ваш chat_id: {incoming}\n"
            f"{match}\n\n"
            f"Команди: /report /status /balance"
        )

    async def _cmd_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """/report — надіслати звіт прямо зараз."""
        logger.info(f"/report від chat_id={update.effective_chat.id}")
        if not await self._is_authorized(update):
            return

        if not self._portfolio_manager or not self._db_manager:
            await update.message.reply_text("⚠️ Портфель ще не ініціалізовано (Binance підключається)")
            return

        try:
            report = await self._build_daily_report()
            # Відповідаємо в той самий чат звідки прийшла команда
            await update.message.reply_text(report, parse_mode=ParseMode.HTML)
            logger.info("Звіт надіслано за командою /report")
        except Exception as e:
            logger.exception("Помилка генерації звіту за /report")
            await update.message.reply_text(f"❌ Помилка генерації звіту: {e}")

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """/status — статус агента."""
        logger.info(f"/status від chat_id={update.effective_chat.id}")
        if not await self._is_authorized(update):
            return

        pm = self._portfolio_manager
        if not pm:
            await update.message.reply_text("⚠️ Агент ще не готовий (Binance підключається)")
            return

        total = pm.calculate_total_usdt()
        positions = pm.open_positions_count()
        text = (
            f"✅ <b>Агент працює</b>\n\n"
            f"💰 Баланс: <b>${total:,.2f}</b>\n"
            f"📂 Відкритих позицій: {positions}/{config.trading.max_open_positions}\n"
            f"⏰ {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
        )
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)

    async def _cmd_balance(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """/balance — детальний баланс по активах."""
        logger.info(f"/balance від chat_id={update.effective_chat.id}")
        if not await self._is_authorized(update):
            return

        pm = self._portfolio_manager
        if not pm:
            await update.message.reply_text("⚠️ Агент ще не готовий (Binance підключається)")
            return

        breakdown = pm.get_portfolio_breakdown()
        lines = ["💼 <b>БАЛАНС ПОРТФЕЛЯ</b>\n"]

        for asset, qty in pm._balances.items():
            price = pm.get_asset_price_usdt(asset)
            value = qty * price
            if value > 0.01:
                lines.append(f"• {asset}: {qty:.6f} ≈ <b>${value:.2f}</b>")

        lines.append(f"\n<b>Разом: ${breakdown['total_usdt']:,.2f}</b>")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

    # ──────────────────────────────────────────
    # Надсилання повідомлень
    # ──────────────────────────────────────────

    async def send(self, text: str, parse_mode: str = ParseMode.HTML) -> bool:
        """Надсилає повідомлення в Telegram."""
        if not self._application:
            logger.debug(f"[Telegram відключено] {text[:100]}")
            return False

        try:
            await self._application.bot.send_message(
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
            f"Команди: /report /status /balance\n"
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
            f"{'🔥 Йдемо до наступного мілстоуну!' if milestone < config.portfolio.milestone_2 else '🚀 Всі мілстоуни досягнуто!'}"
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

    # ──────────────────────────────────────────
    # Щоденний звіт
    # ──────────────────────────────────────────

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

        today_trades = await self._db_manager.get_today_trades()
        profitable = sum(1 for t in today_trades if (t.get("pnl_usdt") or 0) > 0)
        loss_count = sum(1 for t in today_trades if (t.get("pnl_usdt") or 0) < 0)
        pnl_today = sum(t.get("pnl_usdt") or 0 for t in today_trades)

        progress_m1 = min(100.0, total / config.portfolio.milestone_1 * 100)
        progress_m2 = min(100.0, total / config.portfolio.milestone_2 * 100)

        top_positions = pm.get_top_positions(3)
        if top_positions:
            positions_text = ""
            for pos in top_positions:
                sign = "+" if pos["pnl_pct"] >= 0 else ""
                positions_text += (
                    f"  • {pos['symbol']}: {sign}{pos['pnl_pct']:.2f}% "
                    f"(${pos['value_usdt']:.2f})\n"
                )
        else:
            positions_text = "  Відкритих позицій немає\n"

        sentiment_score = self._analyzer._news.last_score if self._analyzer else 0.0
        if sentiment_score > 0.2:
            sentiment_text = "🟢 Позитивний"
        elif sentiment_score < -0.2:
            sentiment_text = "🔴 Негативний"
        else:
            sentiment_text = "🟡 Нейтральний"

        pnl_sign = "+" if pnl_today >= 0 else ""
        date_str = datetime.now().strftime("%d.%m.%Y")

        return (
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

    async def shutdown(self) -> None:
        """Зупиняє планувальник та Application."""
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
        if self._application:
            try:
                await self._application.updater.stop()
                await self._application.stop()
                await self._application.shutdown()
            except Exception as e:
                logger.debug(f"Telegram shutdown: {e}")
        logger.info("Telegram зупинено")
