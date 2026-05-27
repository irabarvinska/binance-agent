"""
WebSocket клієнт для Binance Streams.
Підписується на kline/candlestick та ticker потоки.
Автоматично перепідключається при розриві з'єднання.
"""

import asyncio
import json
import time
from typing import Callable, Dict, List, Optional
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from loguru import logger

from config import config


class BinanceWebSocketClient:
    """
    Клієнт для роботи з Binance WebSocket Streams.
    Підтримує: @kline_1m, @kline_1h, @kline_4h, @ticker
    """

    def __init__(self):
        self._callbacks: Dict[str, List[Callable]] = {}
        self._running = False
        self._reconnect_delay = 5      # секунди між спробами
        self._max_reconnect_delay = 60 # максимальна затримка
        self._ping_interval = 20       # пінг кожні 20с
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

        # Вибір URL залежно від режиму (testnet/mainnet)
        if config.binance.testnet:
            self._base_url = config.binance.ws_testnet_url
        else:
            self._base_url = config.binance.ws_base_url

        # Формуємо список стрімів для підписки
        self._streams = self._build_streams()

    def _build_streams(self) -> List[str]:
        """Будує список WebSocket стрімів для підписки."""
        streams = []
        symbols = [s.lower() for s in config.trading.watch_symbols]
        intervals = config.trading.kline_intervals

        # Свічки для кожного символу та інтервалу
        for symbol in symbols:
            for interval in intervals:
                streams.append(f"{symbol}@kline_{interval}")
            # 24-годинний тікер
            streams.append(f"{symbol}@ticker")

        logger.info(f"Підготовлено {len(streams)} WebSocket стрімів")
        return streams

    def on(self, event_type: str, callback: Callable) -> None:
        """
        Реєструє обробник події.

        event_type: 'kline' | 'ticker' | 'error' | 'connected' | 'disconnected'
        """
        if event_type not in self._callbacks:
            self._callbacks[event_type] = []
        self._callbacks[event_type].append(callback)

    async def _emit(self, event_type: str, data: any) -> None:
        """Викликає всі зареєстровані обробники для події."""
        for callback in self._callbacks.get(event_type, []):
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(data)
                else:
                    callback(data)
            except Exception as e:
                logger.error(f"Помилка в обробнику {event_type}: {e}")

    def _get_ws_url(self) -> str:
        """Формує URL для combined stream (кілька стрімів в одному з'єднанні)."""
        streams_path = "/".join(self._streams)
        return f"{self._base_url}/stream?streams={streams_path}"

    async def _process_message(self, raw_message: str) -> None:
        """Парсить та маршрутизує отримані повідомлення."""
        try:
            data = json.loads(raw_message)

            # Combined stream обгортає дані в {"stream": "...", "data": {...}}
            if "data" in data:
                stream_name = data.get("stream", "")
                payload = data["data"]
            else:
                payload = data
                stream_name = payload.get("e", "")

            event_type = payload.get("e", "")

            if event_type == "kline":
                await self._handle_kline(payload, stream_name)
            elif event_type == "24hrTicker":
                await self._handle_ticker(payload)
            else:
                logger.debug(f"Невідомий тип події: {event_type}")

        except json.JSONDecodeError as e:
            logger.error(f"Помилка розбору JSON: {e}")
        except Exception as e:
            logger.error(f"Помилка обробки повідомлення: {e}")

    async def _handle_kline(self, data: dict, stream: str) -> None:
        """Обробляє kline (свічку) повідомлення."""
        kline = data.get("k", {})
        processed = {
            "symbol": data.get("s", ""),
            "interval": kline.get("i", ""),
            "open_time": kline.get("t", 0),
            "close_time": kline.get("T", 0),
            "open": float(kline.get("o", 0)),
            "high": float(kline.get("h", 0)),
            "low": float(kline.get("l", 0)),
            "close": float(kline.get("c", 0)),
            "volume": float(kline.get("v", 0)),
            "is_closed": kline.get("x", False),  # True якщо свічка закрита
            "quote_volume": float(kline.get("q", 0)),
            "trades": int(kline.get("n", 0)),
        }
        await self._emit("kline", processed)

    async def _handle_ticker(self, data: dict) -> None:
        """Обробляє 24h ticker повідомлення."""
        processed = {
            "symbol": data.get("s", ""),
            "price_change": float(data.get("p", 0)),
            "price_change_pct": float(data.get("P", 0)),
            "last_price": float(data.get("c", 0)),
            "volume": float(data.get("v", 0)),
            "quote_volume": float(data.get("q", 0)),
            "high_24h": float(data.get("h", 0)),
            "low_24h": float(data.get("l", 0)),
            "open_price": float(data.get("o", 0)),
        }
        await self._emit("ticker", processed)

    async def connect(self) -> None:
        """
        Підключається до WebSocket та слухає повідомлення.
        Автоматично перепідключається при розриві.
        """
        self._running = True
        reconnect_delay = self._reconnect_delay

        while self._running:
            url = self._get_ws_url()
            logger.info(f"Підключення до Binance WebSocket... ({'testnet' if config.binance.testnet else 'mainnet'})")

            try:
                async with websockets.connect(
                    url,
                    ping_interval=self._ping_interval,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=1024 * 1024,  # 1MB максимальний розмір повідомлення
                ) as ws:
                    self._ws = ws
                    reconnect_delay = self._reconnect_delay  # скидаємо затримку після успішного підключення
                    logger.info(f"WebSocket підключено! Стрімів: {len(self._streams)}")
                    await self._emit("connected", {"streams": self._streams})

                    async for message in ws:
                        await self._process_message(message)

            except ConnectionClosed as e:
                logger.warning(f"WebSocket з'єднання закрито: {e}")
                await self._emit("disconnected", {"reason": str(e)})

            except WebSocketException as e:
                logger.error(f"WebSocket помилка: {e}")
                await self._emit("error", {"error": str(e)})

            except Exception as e:
                logger.error(f"Неочікувана помилка WebSocket: {e}")
                await self._emit("error", {"error": str(e)})

            finally:
                self._ws = None

            if self._running:
                logger.info(f"Перепідключення через {reconnect_delay}с...")
                await asyncio.sleep(reconnect_delay)
                # Експоненційне збільшення затримки до максимуму
                reconnect_delay = min(reconnect_delay * 2, self._max_reconnect_delay)

    async def disconnect(self) -> None:
        """Закриває WebSocket з'єднання."""
        self._running = False
        if self._ws:
            await self._ws.close()
        logger.info("WebSocket відключено")

    def is_connected(self) -> bool:
        """Перевіряє чи активне з'єднання."""
        return self._ws is not None and not self._ws.closed
