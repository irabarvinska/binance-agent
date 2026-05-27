"""
Торгові стратегії агента.
Генерує сигнали BUY/SELL на основі технічного аналізу та новин.
"""

from enum import Enum
from typing import Optional, Dict, List
from dataclasses import dataclass
from loguru import logger

from config import config


class Signal(Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class StrategyType(Enum):
    HODL = "hodl"
    MODERATE = "moderate"
    HIGH_RISK = "high_risk"


@dataclass
class TradeSignal:
    """Торговий сигнал з деталями обґрунтування."""
    symbol: str
    signal: Signal
    strategy_type: StrategyType
    strength: float          # 0-1, сила сигналу
    reasons: List[str]       # причини
    analysis: Dict           # повні дані аналізу
    suggested_amount_usdt: float = 0.0


class BaseStrategy:
    """Базовий клас стратегії."""

    def generate_signal(self, symbol: str, analysis: dict, available_usdt: float) -> Optional[TradeSignal]:
        raise NotImplementedError


class HodlStrategy(BaseStrategy):
    """
    HODl стратегія (50% портфеля).
    BTC, ETH, SOL — тримаємо, продаємо тільки при дуже сильних сигналах.
    """

    SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    def generate_signal(self, symbol: str, analysis: dict, available_usdt: float) -> Optional[TradeSignal]:
        if symbol not in self.SYMBOLS:
            return None

        rsi = analysis.get("rsi")
        trend = analysis.get("trend", "sideways")
        combined = analysis.get("combined_signal", 0)
        reasons = []

        # HODl: купуємо тільки при дуже сильному oversold
        if rsi and rsi < 25 and combined > 0.5:
            reasons.append(f"RSI={rsi:.1f} — екстремально перепроданий")
            reasons.append(f"Позитивний сигнал: {combined:.2f}")
            amount = min(available_usdt * 0.1, config.trading.max_trade_amount)
            return TradeSignal(
                symbol=symbol,
                signal=Signal.BUY,
                strategy_type=StrategyType.HODL,
                strength=0.7,
                reasons=reasons,
                analysis=analysis,
                suggested_amount_usdt=amount,
            )

        return TradeSignal(
            symbol=symbol,
            signal=Signal.HOLD,
            strategy_type=StrategyType.HODL,
            strength=0.0,
            reasons=["HODl стратегія — утримуємо"],
            analysis=analysis,
        )


class ModerateRiskStrategy(BaseStrategy):
    """
    Помірний ризик (25% портфеля).
    Топ-20 altcoins, сигнали RSI + MACD.
    Stop-loss: -5%, Take-profit: +10%
    """

    def generate_signal(self, symbol: str, analysis: dict, available_usdt: float) -> Optional[TradeSignal]:
        # Перевіряємо чи є символ у списку помірного ризику
        base = symbol.replace("USDT", "")
        if base not in config.portfolio.moderate_assets and symbol not in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
            return None

        rsi = analysis.get("rsi")
        macd = analysis.get("macd")
        trend = analysis.get("trend", "sideways")
        combined = analysis.get("combined_signal", 0)
        reasons = []

        # BUY: RSI < 35 + MACD кросовер угору + позитивний sentiment
        buy_conditions = 0
        if rsi and rsi < config.trading.rsi_oversold:
            buy_conditions += 1
            reasons.append(f"RSI={rsi:.1f} (перепроданий < {config.trading.rsi_oversold})")

        if macd and macd.get("prev_histogram", 0) < 0 < macd.get("histogram", 0):
            buy_conditions += 1
            reasons.append("MACD бичачий кросовер")

        if analysis.get("news_sentiment", 0) > 0.1:
            buy_conditions += 1
            reasons.append(f"Позитивний новинний sentiment: {analysis['news_sentiment']:.2f}")

        if trend in ("uptrend", "strong_uptrend"):
            buy_conditions += 1
            reasons.append(f"Висхідний тренд: {trend}")

        # Потрібно мінімум 2 умови з 4
        if buy_conditions >= 2 and combined > config.trading.buy_signal_threshold:
            strength = min(1.0, buy_conditions / 3)
            amount = min(
                available_usdt * 0.15,
                config.trading.max_trade_amount,
            )
            amount = max(amount, config.trading.min_trade_amount)
            return TradeSignal(
                symbol=symbol,
                signal=Signal.BUY,
                strategy_type=StrategyType.MODERATE,
                strength=strength,
                reasons=reasons,
                analysis=analysis,
                suggested_amount_usdt=amount,
            )

        # SELL: RSI > 70 + негативний тренд
        sell_conditions = 0
        sell_reasons = []

        if rsi and rsi > config.trading.rsi_overbought:
            sell_conditions += 1
            sell_reasons.append(f"RSI={rsi:.1f} (перекуплений > {config.trading.rsi_overbought})")

        if trend in ("downtrend", "strong_downtrend"):
            sell_conditions += 1
            sell_reasons.append(f"Низхідний тренд: {trend}")

        if macd and macd.get("prev_histogram", 0) > 0 > macd.get("histogram", 0):
            sell_conditions += 1
            sell_reasons.append("MACD ведмежий кросовер")

        if sell_conditions >= 2 and combined < config.trading.sell_signal_threshold:
            return TradeSignal(
                symbol=symbol,
                signal=Signal.SELL,
                strategy_type=StrategyType.MODERATE,
                strength=min(1.0, sell_conditions / 3),
                reasons=sell_reasons,
                analysis=analysis,
            )

        return TradeSignal(
            symbol=symbol,
            signal=Signal.HOLD,
            strategy_type=StrategyType.MODERATE,
            strength=0.0,
            reasons=["Немає чіткого сигналу"],
            analysis=analysis,
        )


class HighRiskStrategy(BaseStrategy):
    """
    Висока ризик (25% портфеля).
    Momentum торгівля, нові лістинги.
    Stop-loss: -8%, Take-profit: +20%
    """

    def generate_signal(self, symbol: str, analysis: dict, available_usdt: float) -> Optional[TradeSignal]:
        trend = analysis.get("trend", "sideways")
        combined = analysis.get("combined_signal", 0)
        macd = analysis.get("macd")
        bb = analysis.get("bollinger_bands")
        reasons = []

        # Momentum BUY: сильний тренд + MACD + вихід з нижньої BB
        momentum_score = 0

        if trend == "strong_uptrend":
            momentum_score += 2
            reasons.append("Сильний висхідний тренд")
        elif trend == "uptrend":
            momentum_score += 1
            reasons.append("Висхідний тренд")

        if macd and macd.get("histogram", 0) > 0 and macd.get("macd", 0) > 0:
            momentum_score += 1
            reasons.append("MACD позитивний")

        if bb and bb.get("percent_b", 0.5) < 0.2:
            momentum_score += 1
            reasons.append("Ціна поблизу нижньої Bollinger Band")

        if analysis.get("news_sentiment", 0) > 0.2:
            momentum_score += 1
            reasons.append(f"Сильний позитивний sentiment")

        # Поріг для momentum стратегії вищий
        if momentum_score >= 3 and combined > 0.55:
            # Менший розмір позиції через вищий ризик
            amount = min(
                available_usdt * 0.10,
                config.trading.max_trade_amount * 0.7,
            )
            amount = max(amount, config.trading.min_trade_amount)
            return TradeSignal(
                symbol=symbol,
                signal=Signal.BUY,
                strategy_type=StrategyType.HIGH_RISK,
                strength=min(1.0, momentum_score / 4),
                reasons=reasons,
                analysis=analysis,
                suggested_amount_usdt=amount,
            )

        # Швидкий SELL при розвороті momentum
        if trend in ("downtrend", "strong_downtrend") and combined < -0.3:
            return TradeSignal(
                symbol=symbol,
                signal=Signal.SELL,
                strategy_type=StrategyType.HIGH_RISK,
                strength=0.7,
                reasons=["Розворот momentum", f"Тренд: {trend}"],
                analysis=analysis,
            )

        return TradeSignal(
            symbol=symbol,
            signal=Signal.HOLD,
            strategy_type=StrategyType.HIGH_RISK,
            strength=0.0,
            reasons=["Недостатній momentum"],
            analysis=analysis,
        )


class StrategyManager:
    """
    Управляє всіма стратегіями та вибирає найкращий сигнал.
    """

    def __init__(self):
        self._hodl = HodlStrategy()
        self._moderate = ModerateRiskStrategy()
        self._high_risk = HighRiskStrategy()

    def get_signal(
        self,
        symbol: str,
        analysis: dict,
        available_usdt: float,
        open_positions_count: int,
        has_position: bool,
    ) -> Optional[TradeSignal]:
        """
        Генерує торговий сигнал для символу.

        Повертає None якщо:
        - Досягнуто ліміт відкритих позицій
        - Немає чіткого сигналу
        """
        # Перевіряємо ліміт позицій
        if not has_position and open_positions_count >= config.trading.max_open_positions:
            logger.debug(f"Ліміт позицій ({config.trading.max_open_positions}) досягнуто")
            return None

        # Пробуємо стратегії в порядку пріоритету
        candidates: List[TradeSignal] = []

        for strategy in [self._hodl, self._moderate, self._high_risk]:
            signal = strategy.generate_signal(symbol, analysis, available_usdt)
            if signal and signal.signal != Signal.HOLD:
                candidates.append(signal)

        if not candidates:
            return None

        # Вибираємо сигнал з найбільшою силою
        best = max(candidates, key=lambda s: s.strength)

        if best.signal == Signal.BUY:
            logger.info(
                f"BUY сигнал: {best.symbol} | {best.strategy_type.value} | "
                f"сила={best.strength:.2f} | {', '.join(best.reasons[:2])}"
            )
        elif best.signal == Signal.SELL:
            logger.info(
                f"SELL сигнал: {best.symbol} | {best.strategy_type.value} | "
                f"сила={best.strength:.2f}"
            )

        return best
