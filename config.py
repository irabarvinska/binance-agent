"""
Конфігурація торгового агента Binance.
Всі секрети завантажуються з .env файлу.
"""

import os
from dataclasses import dataclass, field
from typing import List
from dotenv import load_dotenv

load_dotenv()


@dataclass
class BinanceConfig:
    api_key: str = field(default_factory=lambda: os.getenv("BINANCE_API_KEY", ""))
    api_secret: str = field(default_factory=lambda: os.getenv("BINANCE_API_SECRET", ""))
    testnet: bool = field(default_factory=lambda: os.getenv("BINANCE_TESTNET", "true").lower() == "true")
    # WebSocket endpoint
    ws_base_url: str = "wss://stream.binance.com:9443"
    ws_testnet_url: str = "wss://testnet.binance.vision"
    # REST endpoints
    rest_base_url: str = "https://api.binance.com"
    rest_testnet_url: str = "https://testnet.binance.vision"
    # Проксі (опційно): потрібен якщо деплой на сервері в заблокованому регіоні (напр. США)
    # Формат: "http://user:pass@host:port" або "socks5://host:port"
    https_proxy: str = field(default_factory=lambda: os.getenv("HTTPS_PROXY", os.getenv("https_proxy", "")))


@dataclass
class TelegramConfig:
    bot_token: str = field(default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", ""))
    chat_id: str = field(default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", ""))
    # Час щоденного звіту (UTC)
    daily_report_hour: int = 8   # 11:00 Київ = 08:00 UTC
    daily_report_minute: int = 0


@dataclass
class NewsConfig:
    api_key: str = field(default_factory=lambda: os.getenv("NEWS_API_KEY", ""))
    # Ключові слова для пошуку новин
    keywords: List[str] = field(default_factory=lambda: [
        "Bitcoin", "Ethereum", "Solana", "crypto", "cryptocurrency",
        "blockchain", "DeFi", "altcoin"
    ])
    # Кількість новин для аналізу
    max_articles: int = 20
    # Оновлення кожні N хвилин
    update_interval_minutes: int = 30


@dataclass
class PortfolioConfig:
    # Початковий баланс (реальні дані користувача)
    initial_btc: float = 0.00112001
    initial_eth: float = 0.02223049
    initial_sol: float = 0.33059296
    initial_sui: float = 17.79
    initial_usdc: float = 10.0
    initial_usdt: float = 1.0

    # Розподіл портфеля
    hodl_allocation: float = 0.50       # 50% — HODl
    moderate_allocation: float = 0.25   # 25% — помірний ризик
    high_risk_allocation: float = 0.25  # 25% — високий ризик

    # HODl активи
    hodl_assets: List[str] = field(default_factory=lambda: ["BTC", "ETH", "SOL"])

    # Помірний ризик — топ-20 altcoins
    moderate_assets: List[str] = field(default_factory=lambda: [
        "BNB", "ADA", "AVAX", "DOT", "MATIC", "LINK", "UNI", "ATOM",
        "XLM", "VET", "FIL", "ICP", "NEAR", "ALGO", "HBAR"
    ])

    # Мілстоуни балансу
    milestone_1: float = 1000.0
    milestone_2: float = 10000.0

    # Щомісячна конвертація (якщо баланс >= threshold)
    monthly_conversion_threshold: float = 5000.0
    monthly_conversion_amount: float = 1000.0
    monthly_conversion_day: int = 20


@dataclass
class TradingConfig:
    # Ліміти на угоди
    min_trade_amount: float = field(
        default_factory=lambda: float(os.getenv("MIN_TRADE_AMOUNT", "10.0"))
    )
    max_trade_amount: float = field(
        default_factory=lambda: float(os.getenv("MAX_TRADE_AMOUNT", "50.0"))
    )
    max_open_positions: int = 3

    # Стоп-лосс та тейк-профіт
    moderate_stop_loss: float = -0.05    # -5%
    moderate_take_profit: float = 0.10   # +10%
    high_risk_stop_loss: float = -0.08   # -8%
    high_risk_take_profit: float = 0.20  # +20%

    # RSI пороги
    rsi_oversold: float = 35.0
    rsi_overbought: float = 70.0
    rsi_period: int = 14

    # MACD параметри
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9

    # Bollinger Bands
    bb_period: int = 20
    bb_std: float = 2.0

    # EMA
    ema_short: int = 20
    ema_medium: int = 50
    ema_long: int = 200

    # Вага сигналів (технічний + новини)
    technical_weight: float = 0.70
    news_weight: float = 0.30

    # Поріг сигналу для угоди
    buy_signal_threshold: float = 0.6
    sell_signal_threshold: float = -0.4

    # Великі угоди для алерту (USDT)
    large_trade_alert: float = 20.0

    # Символи для моніторингу WebSocket
    watch_symbols: List[str] = field(default_factory=lambda: [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "SUIUSDT",
        "BNBUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT"
    ])

    # Свічкові інтервали
    kline_intervals: List[str] = field(default_factory=lambda: ["1m", "1h", "4h"])


@dataclass
class AppConfig:
    binance: BinanceConfig = field(default_factory=BinanceConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    news: NewsConfig = field(default_factory=NewsConfig)
    portfolio: PortfolioConfig = field(default_factory=PortfolioConfig)
    trading: TradingConfig = field(default_factory=TradingConfig)

    timezone: str = field(default_factory=lambda: os.getenv("TIMEZONE", "Europe/Kyiv"))
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    db_path: str = "data/trading.db"
    log_path: str = "logs/agent.log"

    def validate(self) -> bool:
        """Перевірка обов'язкових налаштувань."""
        errors = []

        if not self.binance.api_key:
            errors.append("BINANCE_API_KEY не встановлено")
        if not self.binance.api_secret:
            errors.append("BINANCE_API_SECRET не встановлено")
        if not self.telegram.bot_token:
            errors.append("TELEGRAM_BOT_TOKEN не встановлено")
        if not self.telegram.chat_id:
            errors.append("TELEGRAM_CHAT_ID не встановлено")

        if errors:
            for err in errors:
                print(f"[ПОМИЛКА КОНФІГУРАЦІЇ] {err}")
            return False
        return True


# Глобальний екземпляр конфігурації
config = AppConfig()
