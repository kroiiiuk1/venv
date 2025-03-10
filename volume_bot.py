import asyncio
import requests
from telegram import Bot
from pybit.unified_trading import WebSocket
from collections import deque
import numpy as np
import pandas as pd
import pandas_ta as ta
from datetime import datetime
import threading
import queue
import time

# Настройки
TELEGRAM_TOKEN = '7387387044:AAGFdeu0kJH1IQ_e2PXegkF4z4WtWRrwL0A'
CHAT_ID = '-1002362256912'
VOLUME_THRESHOLD = 1.0  # Снижено для теста
PRICE_THRESHOLD = 1
LIQUIDATION_THRESHOLD = 100  # Снижено для теста
CHECK_INTERVAL = 120
EMA_SHORT_PERIOD = 5
EMA_LONG_PERIOD = 20
ATR_PERIOD = 14
ATR_MULTIPLIER = 1  # Снижено для теста
RSI_PERIOD = 14

# Вставьте свои API-ключи Bybit сюда
API_KEY = 'ESUNXn5qTs26yci2W2'
API_SECRET = 'OtFi15vAl5pHyrOTHr6ZR2w2NmVk8aZtRzLi'

# Уровни
COINS = {
    'bitcoin': {'id': 'bitcoin', 'symbol': 'BTCUSDT', 'levels': [84000, 84500, 85000]},
    'ethereum': {'id': 'ethereum', 'symbol': 'ETHUSDT', 'levels': [2100, 2130, 2160]},
    'solana': {'id': 'solana', 'symbol': 'SOLUSDT', 'levels': [132, 135, 138]},
    'ripple': {'id': 'ripple', 'symbol': 'XRPUSDT', 'levels': [2.2, 2.23, 2.26]},
    'cardano': {'id': 'cardano', 'symbol': 'ADAUSDT', 'levels': [0.76, 0.78, 0.80]}
}

# Инициализация
bot = Bot(TELEGRAM_TOKEN)
ws = WebSocket(testnet=False, channel_type="linear", api_key=API_KEY, api_secret=API_SECRET)
liquidation_queue = queue.Queue()
first_liquidation_processed = False

# Получение данных с CoinGecko с обработкой ошибок 429
def get_coin_data(coin_id):
    url = f'https://api.coingecko.com/api/v3/coins/{coin_id}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return {
            'volume': data['market_data']['total_volume']['usd'],
            'price': data['market_data']['current_price']['usd']
        }
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print(f"Превышен лимит запросов для {coin_id}. Ожидание 60 секунд...")
            time.sleep(60)
            return get_coin_data(coin_id)
        else:
            print(f"Ошибка при получении данных для {coin_id}: {e}")
            return None
    except Exception as e:
        print(f"Ошибка при получении данных для {coin_id}: {e}")
        return None

# Отправка сообщений
async def send_message(chat_id, text):
    try:
        await bot.send_message(chat_id=chat_id, text=text)
        print(f"Отправлено: {text}")
    except Exception as e:
        print(f"Ошибка отправки: {e}")

# Проверка пробоя/подхода к уровням
def check_levels(coin_name, price, last_price, levels):
    closest_level = min(levels, key=lambda x: abs(x - price))
    diff_percent = ((price - closest_level) / closest_level) * 100
    if abs(diff_percent) <= PRICE_THRESHOLD and last_price is not None:
        if price > closest_level and last_price <= closest_level:
            return f"Пробит уровень сопротивления {closest_level} USD вверх"
        elif price < closest_level and last_price >= closest_level:
            return f"Пробит уровень поддержки {closest_level} USD вниз"
    elif abs(diff_percent) <= 1:
        return f"Подход к уровню {closest_level} USD ({diff_percent:.1f}%)"
    return None

# Вычисление EMA
def calculate_ema(prices, period):
    if len(prices) < period:
        return sum(prices) / len(prices) if prices else 0
    prices = np.array(prices)
    weights = np.exp(np.linspace(-1., 0., period))
    weights /= weights.sum()
    ema = np.convolve(prices, weights, mode='valid')[0]
    return ema

# Вычисление RSI
def calculate_rsi(prices, period):
    if len(prices) < period + 1:
        return 50
    price_diff = np.diff(prices)
    gains = np.where(price_diff > 0, price_diff, 0)
    losses = np.where(price_diff < 0, -price_diff, 0)
    avg_gain = np.mean(gains[-period:]) if np.mean(gains[-period:]) else 0.0001
    avg_loss = np.mean(losses[-period:]) if np.mean(losses[-period:]) else 0.0001
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# Вычисление ATR
def calculate_atr(prices, highs, lows, period):
    if len(prices) < period:
        return 0
    df = pd.DataFrame({
        'high': highs,
        'low': lows,
        'close': prices
    })
    atr = ta.atr(df['high'], df['low'], df['close'], length=period).iloc[-1]
    return atr if not np.isnan(atr) else 0

# Обработка WebSocket сообщений
def handle_liquidation(message):
    print(f"Получено WebSocket сообщение: {message}")
    try:
        data = message.get('data', {})
        if not data:
            print("Данные пусты, пропускаем.")
            return
        symbol = data.get('symbol')
        side = data.get('side')
        volume = float(data.get('size', 0))
        price = float(data.get('price', 0))
        if symbol and side:
            for coin_name, coin_info in COINS.items():
                if symbol == coin_info['symbol']:
                    total_value = volume * price
                    liquidation_queue.put((coin_name, side, total_value))
                    print(f"Ликвидация для {coin_name}: {side}, {total_value:,.0f} USD")
    except Exception as e:
        print(f"Ошибка обработки ликвидаций: {e}")

# Функция для запуска WebSocket в отдельном потоке
def run_websocket(symbols):
    ws.liquidation_stream(callback=handle_liquidation, symbol=symbols)

# Определение сигналов
def detect_trade_signal(coin_name, volume_change, price_change, price, last_price, levels, long_liq, short_liq, ema_short, ema_long, rsi, atr):
    signal = None
    level_info = check_levels(coin_name, price, last_price, levels)
    trend_up = ema_short > ema_long if ema_long else False
    trend_down = ema_short < ema_long if ema_long else False
    price_move = abs(price - last_price)
    atr_threshold = atr * ATR_MULTIPLIER

    # Отладочная информация
    print(f"DEBUG {coin_name.upper()}: level_info={level_info}, volume_change={volume_change:.2f}, trend_up={trend_up}, trend_down={trend_down}, rsi={rsi:.1f}, price_move={price_move:.2f}, atr_threshold={atr_threshold:.2f}, long_liq={long_liq:.0f}, short_liq={short_liq:.0f}")

    if level_info and "Пробит" in level_info and "вверх" in level_info and volume_change > VOLUME_THRESHOLD and trend_up and rsi < 70 and price_move > atr_threshold:
        signal = "ЛОНГ"
        reason = f"{level_info}, объем вырос на {volume_change:.2f}%, EMA (5/20): {ema_short:.2f}/{ema_long:.2f} (Золотой крест), RSI: {rsi:.1f}, движение выше ATR*{ATR_MULTIPLIER} ({price_move:.2f} USD)"
        if short_liq > LIQUIDATION_THRESHOLD:
            reason += f", ликвидации шортов: {short_liq:,.0f} USD"
        sl = min([l for l in levels if l < price], default=price * 0.98)
        tp = max([l for l in levels if l > price], default=price * 1.03)
        return f"🚀 {signal} для {coin_name.upper()}\n- Цена: {price:,.2f} USD\n- {reason}\n- SL: {sl:,.2f} USD\n- TP: {tp:,.2f} USD\n- Время: {datetime.utcnow().strftime('%H:%M')} (UTC)"

    elif level_info and "Пробит" in level_info and "вниз" in level_info and volume_change > VOLUME_THRESHOLD and trend_down and rsi > 30 and price_move > atr_threshold:
        signal = "ШОРТ"
        reason = f"{level_info}, объем вырос на {volume_change:.2f}%, EMA (5/20): {ema_short:.2f}/{ema_long:.2f} (Мёртвый крест), RSI: {rsi:.1f}, движение выше ATR*{ATR_MULTIPLIER} ({price_move:.2f} USD)"
        if long_liq > LIQUIDATION_THRESHOLD:
            reason += f", ликвидации лонгов: {long_liq:,.0f} USD"
        sl = max([l for l in levels if l > price], default=price * 1.02)
        tp = min([l for l in levels if l < price], default=price * 0.97)
        return f"📉 {signal} для {coin_name.upper()}\n- Цена: {price:,.2f} USD\n- {reason}\n- SL: {sl:,.2f} USD\n- TP: {tp:,.2f} USD\n- Время: {datetime.utcnow().strftime('%H:%M')} (UTC)"

    elif level_info and "Подход" in level_info:
        return f"⚠ {level_info} для {coin_name.upper()}\n- Цена: {price:,.2f} USD\n- Ликвидации: {long_liq + short_liq:,.0f} USD (Лонги: {long_liq:,.0f}, Шорты: {short_liq:,.0f})\n- Время: {datetime.utcnow().strftime('%H:%M')} (UTC)"

    return None

# Основной цикл
async def main():
    # Тестовое сообщение в Telegram для проверки
    await send_message(CHAT_ID, "Бот запущен. Тестовое сообщение.")

    last_volumes = {coin: None for coin in COINS}
    last_prices = {coin: None for coin in COINS}
    price_history = {coin: deque(maxlen=max(EMA_LONG_PERIOD, ATR_PERIOD, RSI_PERIOD)) for coin in COINS}
    high_history = {coin: deque(maxlen=ATR_PERIOD) for coin in COINS}
    low_history = {coin: deque(maxlen=ATR_PERIOD) for coin in COINS}
    liquidations = {coin: {'long': 0, 'short': 0} for coin in COINS}
    cycle_liquidations = {coin: {'long': 0, 'short': 0} for coin in COINS}

    # Подписка на WebSocket в отдельном потоке
    symbols = [coin_info['symbol'] for coin_info in COINS.values()]
    ws_thread = threading.Thread(target=run_websocket, args=(symbols,), daemon=True)
    ws_thread.start()

    while True:
        # Обработка данных из очереди ликвидаций
        while not liquidation_queue.empty():
            try:
                coin, side, value = liquidation_queue.get_nowait()
                if side == 'Sell':
                    liquidations[coin]['short'] += value
                    cycle_liquidations[coin]['short'] += value
                elif side == 'Buy':
                    liquidations[coin]['long'] += value
                    cycle_liquidations[coin]['long'] += value
                print(f"Обработано из очереди: {coin}, {side}, {value:,.0f} USD")
                # Тестовый сигнал после первой ликвидации
                global first_liquidation_processed
                if not first_liquidation_processed:
                    first_liquidation_processed = True
                    test_signal = f"⚠ Тестовое предупреждение для {coin.upper()}\n- Цена: {last_prices.get(coin, 0):,.2f} USD\n- Ликвидации: {value:,.0f} USD\n- Время: {datetime.utcnow().strftime('%H:%M')} (UTC)"
                    await send_message(CHAT_ID, test_signal)
            except queue.Empty:
                break

        for coin_name, coin_info in COINS.items():
            coin_data = get_coin_data(coin_info['id'])
            if coin_data is None:
                continue

            current_volume = coin_data['volume']
            current_price = coin_data['price']
            price_history[coin_name].append(current_price)
            high_history[coin_name].append(current_price)  # Упрощение
            low_history[coin_name].append(current_price)  # Упрощение
            ema_short = calculate_ema(list(price_history[coin_name]), EMA_SHORT_PERIOD)
            ema_long = calculate_ema(list(price_history[coin_name]), EMA_LONG_PERIOD)
            rsi = calculate_rsi(list(price_history[coin_name]), RSI_PERIOD)
            atr = calculate_atr(list(price_history[coin_name]), list(high_history[coin_name]), list(low_history[coin_name]), ATR_PERIOD)

            if last_volumes[coin_name] is not None and last_prices[coin_name] is not None:
                volume_change = ((current_volume - last_volumes[coin_name]) / last_volumes[coin_name]) * 100
                price_change = ((current_price - last_prices[coin_name]) / last_prices[coin_name]) * 100

                signal = detect_trade_signal(
                    coin_name, volume_change, price_change, current_price, last_prices[coin_name],
                    coin_info['levels'], cycle_liquidations[coin_name]['long'], cycle_liquidations[coin_name]['short'],
                    ema_short, ema_long, rsi, atr
                )
                if signal:
                    await send_message(CHAT_ID, signal)

                print(f"{coin_name.upper()} - Объем: {current_volume:,.0f} USD, Цена: {current_price:,.2f} USD, "
                      f"EMA (5/20): {ema_short:,.2f}/{ema_long:,.2f}, RSI: {rsi:.1f}, ATR: {atr:,.2f}, "
                      f"Изм. объема: {volume_change:.2f}%, Изм. цены: {price_change:.2f}%, "
                      f"Лонги: {liquidations[coin_name]['long']:,.0f} USD, Шорты: {liquidations[coin_name]['short']:,.0f} USD, "
                      f"Цикл Лонги: {cycle_liquidations[coin_name]['long']:,.0f} USD, Цикл Шорты: {cycle_liquidations[coin_name]['short']:,.0f} USD")
            else:
                print(f"{coin_name.upper()} - Объем: {current_volume:,.0f} USD, Цена: {current_price:,.2f} USD, "
                      f"EMA (5/20): {ema_short:,.2f}/{ema_long:,.2f}, RSI: {rsi:.1f}, ATR: {atr:,.2f}, "
                      f"Изм. объема: 0.00%, Изм. цены: 0.00%, "
                      f"Лонги: {liquidations[coin_name]['long']:,.0f} USD, Шорты: {liquidations[coin_name]['short']:,.0f} USD, "
                      f"Цикл Лонги: {cycle_liquidations[coin_name]['long']:,.0f} USD, Цикл Шорты: {cycle_liquidations[coin_name]['short']:,.0f} USD")

            last_volumes[coin_name] = current_volume
            last_prices[coin_name] = current_price
            await asyncio.sleep(10)

        # Сброс только цикл-ликвидаций
        for coin in cycle_liquidations:
            cycle_liquidations[coin] = {'long': 0, 'short': 0}

        await asyncio.sleep(CHECK_INTERVAL)

# Запуск
if __name__ == "__main__":
    asyncio.run(main())