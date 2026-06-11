"""
Аналізатор ринку.
Технічний аналіз (RSI, MACD, BB, EMA) + аналіз новин через NewsAPI.
Комбінований сигнал: технічний (70%) + новини (30%).
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import deque

import aiohttp
import pandas as pd
import numpy as np
import ta as ta_lib
from loguru import logger

from config import config


class MarketData:
    """Зберігає OHLCV дані для одного символу та інтервалу."""

    def __init__(self, symbol: str, interval: str, max_candles: int = 300):
        self.symbol = symbol
        self.interval = interval
        # Зберігаємо останні max_candles свічок
        self._candles: deque = deque(maxlen=max_candles)

    def add_candle(self, candle: dict) -> None:
        """Додає або оновлює останню свічку."""
        if self._candles and self._candles[-1]["open_time"] == candle["open_time"]:
            # Оновлюємо незакриту свічку
            self._candles[-1] = candle
        else:
            self._candles.append(candle)

    def to_dataframe(self) -> pd.DataFrame:
        """Конвертує у DataFrame для технічного аналізу."""
        if len(self._candles) < 30:
            return pd.DataFrame()

        df = pd.DataFrame(list(self._candles))
        df["close"] = df["close"].astype(float)
        df["open"] = df["open"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        df["volume"] = df["volume"].astype(float)
        return df

    def __len__(self) -> int:
        return len(self._candles)


class TechnicalIndicators:
    """Розраховує технічні індикатори на основі OHLCV даних (бібліотека `ta`)."""

    @staticmethod
    def calculate_rsi(df: pd.DataFrame, period: int = 14) -> Optional[float]:
        """RSI (Relative Strength Index)."""
        if len(df) < period + 1:
            return None
        rsi_series = ta_lib.momentum.RSIIndicator(df["close"], window=period).rsi()
        val = rsi_series.iloc[-1]
        return float(val) if not pd.isna(val) else None

    @staticmethod
    def calculate_macd(
        df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9
    ) -> Optional[Dict[str, float]]:
        """MACD (Moving Average Convergence Divergence)."""
        if len(df) < slow + signal:
            return None
        macd_obj = ta_lib.trend.MACD(df["close"], window_fast=fast, window_slow=slow, window_sign=signal)
        macd_line = macd_obj.macd()
        signal_line = macd_obj.macd_signal()
        histogram = macd_obj.macd_diff()
        if len(histogram) < 2:
            return None
        return {
            "macd": float(macd_line.iloc[-1]),
            "signal": float(signal_line.iloc[-1]),
            "histogram": float(histogram.iloc[-1]),
            "prev_histogram": float(histogram.iloc[-2]),
        }

    @staticmethod
    def calculate_bollinger_bands(
        df: pd.DataFrame, period: int = 20, std: float = 2.0
    ) -> Optional[Dict[str, float]]:
        """Bollinger Bands."""
        if len(df) < period:
            return None
        bb = ta_lib.volatility.BollingerBands(df["close"], window=period, window_dev=std)
        upper = bb.bollinger_hband().iloc[-1]
        middle = bb.bollinger_mavg().iloc[-1]
        lower = bb.bollinger_lband().iloc[-1]
        price = df["close"].iloc[-1]
        band_width = (upper - lower) / middle if middle else 0
        percent_b = (price - lower) / (upper - lower) if (upper - lower) > 0 else 0.5
        return {
            "upper": float(upper),
            "middle": float(middle),
            "lower": float(lower),
            "bandwidth": float(band_width),
            "percent_b": float(percent_b),
        }

    @staticmethod
    def calculate_ema(df: pd.DataFrame, period: int) -> Optional[float]:
        """Exponential Moving Average."""
        if len(df) < period:
            return None
        ema = ta_lib.trend.EMAIndicator(df["close"], window=period).ema_indicator()
        val = ema.iloc[-1]
        return float(val) if not pd.isna(val) else None

    @staticmethod
    def determine_trend(
        current_price: float,
        ema20: Optional[float],
        ema50: Optional[float],
        ema200: Optional[float],
    ) -> str:
        """
        Визначає ринковий тренд.
        uptrend: ціна > EMA20 > EMA50 > EMA200
        downtrend: ціна < EMA20 < EMA50
        sideways: інакше
        """
        if not all([ema20, ema50]):
            return "sideways"

        if current_price > ema20 and ema20 > ema50:
            if ema200 and ema50 > ema200:
                return "strong_uptrend"
            return "uptrend"
        elif current_price < ema20 and ema20 < ema50:
            if ema200 and ema50 < ema200:
                return "strong_downtrend"
            return "downtrend"
        else:
            return "sideways"


class NewsAnalyzer:
    """Аналізує крипто-новини через NewsAPI та розраховує sentiment score."""

    # Позитивні та негативні слова для базового sentiment аналізу
    POSITIVE_WORDS = {
        "bullish", "surge", "rally", "gain", "rise", "up", "high", "all-time",
        "adoption", "partnership", "upgrade", "launch", "approve", "etf",
        "institutional", "growth", "positive", "support", "buy", "accumulate",
        "record", "breakthrough", "success", "profit", "pump", "moon"
    }
    NEGATIVE_WORDS = {
        "bearish", "crash", "fall", "drop", "decline", "down", "low", "ban",
        "hack", "scam", "fraud", "sell", "dump", "loss", "fear", "panic",
        "regulation", "lawsuit", "fine", "collapse", "warning", "risk",
        "manipulation", "bubble", "correction", "plunge"
    }

    def __init__(self):
        self._api_key = config.news.api_key
        self._cache: Dict[str, Tuple[float, float]] = {}  # {query: (timestamp, score)}
        self._cache_ttl = config.news.update_interval_minutes * 60
        self.last_score: float = 0.0  # останній розрахований sentiment

    async def get_sentiment(self, query: str = "bitcoin ethereum crypto") -> float:
        """
        Отримує sentiment score для крипторинку (-1 до +1).
        Використовує кеш для зменшення кількості запитів до API.
        """
        now = asyncio.get_event_loop().time()

        # Перевіряємо кеш
        if query in self._cache:
            cached_time, cached_score = self._cache[query]
            if now - cached_time < self._cache_ttl:
                return cached_score

        if not self._api_key:
            logger.warning("NEWS_API_KEY не встановлено, sentiment = 0")
            return 0.0

        try:
            score = await self._fetch_sentiment(query)
            self._cache[query] = (now, score)
            self.last_score = score
            return score
        except Exception as e:
            logger.error(f"Помилка аналізу новин: {e}")
            # Повертаємо попередній результат або 0
            if query in self._cache:
                return self._cache[query][1]
            return 0.0

    async def _fetch_sentiment(self, query: str) -> float:
        """Завантажує новини та розраховує sentiment."""
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": query,
            "apiKey": self._api_key,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": config.news.max_articles,
            # Новини за останні 24 години
            "from": (datetime.utcnow() - timedelta(hours=24)).isoformat(),
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    logger.warning(f"NewsAPI статус: {resp.status}")
                    return 0.0
                data = await resp.json()

        articles = data.get("articles", [])
        if not articles:
            return 0.0

        scores = []
        for article in articles:
            text = f"{article.get('title', '')} {article.get('description', '')}".lower()
            score = self._score_text(text)
            scores.append(score)

        # Середній score
        avg_score = sum(scores) / len(scores) if scores else 0.0
        logger.info(f"Новинний sentiment: {avg_score:.3f} ({len(articles)} статей)")
        return avg_score

    def _score_text(self, text: str) -> float:
        """Розраховує sentiment для одного тексту."""
        words = set(text.split())
        positive_count = len(words & self.POSITIVE_WORDS)
        negative_count = len(words & self.NEGATIVE_WORDS)

        total = positive_count + negative_count
        if total == 0:
            return 0.0
        return (positive_count - negative_count) / total


class MarketAnalyzer:
    """
    Головний аналізатор ринку.
    Об'єднує технічний аналіз та новинний sentiment.
    """

    def __init__(self):
        self._market_data: Dict[str, Dict[str, MarketData]] = {}
        self._indicators = TechnicalIndicators()
        self._news = NewsAnalyzer()
        self._latest_tickers: Dict[str, dict] = {}

    def update_candle(self, symbol: str, interval: str, candle: dict) -> None:
        """Оновлює свічкові дані для символу."""
        if symbol not in self._market_data:
            self._market_data[symbol] = {}
        if interval not in self._market_data[symbol]:
            self._market_data[symbol][interval] = MarketData(symbol, interval)
        self._market_data[symbol][interval].add_candle(candle)

    def update_ticker(self, symbol: str, ticker: dict) -> None:
        """Оновлює ticker дані."""
        self._latest_tickers[symbol] = ticker

    def get_ticker(self, symbol: str) -> Optional[dict]:
        """Повертає останній ticker для символу."""
        return self._latest_tickers.get(symbol)

    async def analyze(self, symbol: str, interval: str = "1h") -> Optional[Dict]:
        """
        Повний аналіз символу.
        Повертає словник з індикаторами, трендом та комбінованим сигналом.
        """
        if symbol not in self._market_data or interval not in self._market_data[symbol]:
            logger.debug(f"Недостатньо даних для аналізу {symbol} {interval}")
            return None

        df = self._market_data[symbol][interval].to_dataframe()
        if df.empty or len(df) < 50:
            return None

        current_price = df["close"].iloc[-1]

        # Технічні індикатори
        rsi = self._indicators.calculate_rsi(df, config.trading.rsi_period)
        macd = self._indicators.calculate_macd(
            df, config.trading.macd_fast, config.trading.macd_slow, config.trading.macd_signal
        )
        bb = self._indicators.calculate_bollinger_bands(
            df, config.trading.bb_period, config.trading.bb_std
        )
        ema20 = self._indicators.calculate_ema(df, config.trading.ema_short)
        ema50 = self._indicators.calculate_ema(df, config.trading.ema_medium)
        ema200 = self._indicators.calculate_ema(df, config.trading.ema_long)

        trend = self._indicators.determine_trend(current_price, ema20, ema50, ema200)

        # Технічний сигнал (-1 до +1)
        tech_signal = self._calculate_technical_signal(rsi, macd, bb, trend, current_price)

        # Новинний sentiment (один запит для всього ринку)
        base_symbol = symbol.replace("USDT", "").replace("USDC", "")
        news_query = f"{base_symbol} cryptocurrency"
        news_sentiment = await self._news.get_sentiment(news_query)

        # Комбінований сигнал: 70% технічний + 30% новини
        combined_signal = (
            tech_signal * config.trading.technical_weight +
            news_sentiment * config.trading.news_weight
        )

        result = {
            "symbol": symbol,
            "interval": interval,
            "timestamp": datetime.utcnow().isoformat(),
            "price": current_price,
            "trend": trend,
            "rsi": rsi,
            "macd": macd,
            "bollinger_bands": bb,
            "ema": {"ema20": ema20, "ema50": ema50, "ema200": ema200},
            "technical_signal": tech_signal,
            "news_sentiment": news_sentiment,
            "combined_signal": combined_signal,
            "candles_count": len(df),
        }

        rsi_str = f"{rsi:.1f}" if rsi is not None else "N/A"
        logger.debug(
            f"{symbol} {interval}: RSI={rsi_str}, "
            f"trend={trend}, signal={combined_signal:.3f}"
        )
        return result

    def _calculate_technical_signal(
        self,
        rsi: Optional[float],
        macd: Optional[Dict],
        bb: Optional[Dict],
        trend: str,
        price: float,
    ) -> float:
        """
        Розраховує технічний сигнал від -1 (сильний продаж) до +1 (сильна покупка).
        """
        signals = []

        # RSI сигнал
        if rsi is not None:
            if rsi < 30:
                signals.append(1.0)    # Сильно перепроданий
            elif rsi < 40:
                signals.append(0.5)
            elif rsi > 70:
                signals.append(-1.0)   # Сильно перекуплений
            elif rsi > 60:
                signals.append(-0.5)
            else:
                signals.append(0.0)

        # MACD кросовер
        if macd:
            # Позитивний кросовер: гістограм перейшов з від'ємного в позитивний
            if macd["prev_histogram"] < 0 < macd["histogram"]:
                signals.append(0.8)
            # Негативний кросовер
            elif macd["prev_histogram"] > 0 > macd["histogram"]:
                signals.append(-0.8)
            # MACD вище нуля
            elif macd["macd"] > 0:
                signals.append(0.2)
            else:
                signals.append(-0.2)

        # Bollinger Bands
        if bb and price:
            bb_signal = 1 - 2 * bb.get("percent_b", 0.5)  # +1 внизу, -1 вгорі діапазону
            signals.append(bb_signal * 0.5)

        # Тренд
        trend_scores = {
            "strong_uptrend": 0.6,
            "uptrend": 0.3,
            "sideways": 0.0,
            "downtrend": -0.3,
            "strong_downtrend": -0.6,
        }
        signals.append(trend_scores.get(trend, 0.0))

        if not signals:
            return 0.0
        return max(-1.0, min(1.0, sum(signals) / len(signals)))

    def get_market_summary(self) -> Dict[str, dict]:
        """Повертає короткий огляд ринку для всіх відстежуваних символів."""
        summary = {}
        for symbol, ticker in self._latest_tickers.items():
            summary[symbol] = {
                "price": ticker.get("last_price", 0),
                "change_24h": ticker.get("price_change_pct", 0),
                "volume_24h": ticker.get("quote_volume", 0),
            }
        return summary
