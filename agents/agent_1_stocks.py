import os
import json
import yfinance as yf
import pandas as pd
import pandas_ta as ta
from sqlalchemy import create_engine, text
import telegram
import asyncio
import logging
from datetime import datetime, timezone

# Konfiguracja Logów
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("Stocks_Agent_1")

# Zmienne środowiskowe
DB_URL = os.getenv("DATABASE_URL")
TG_TOKEN = os.getenv("TELEGRAM_SIGNAL_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

engine = create_engine(DB_URL)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'params_stocks.json')

def load_config():
    try:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Nie znaleziono pliku konfiguracyjnego w {CONFIG_PATH}")
        exit(1)

async def send_tg(message):
    if not TG_TOKEN or not CHAT_ID:
        logger.warning("Brak konfiguracji Telegram.")
        return
    try:
        bot = telegram.Bot(token=TG_TOKEN)
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Błąd Telegram: {e}")

def is_market_open(last_candle_time):
    """
    Zabezpieczenie: Sprawdza, czy ostatnia świeca nie jest starsza niż 2 godziny.
    Zapobiega generowaniu sygnałów w weekendy lub po zamknięciu sesji.
    """
    now = datetime.now(timezone.utc)
    # yfinance zwraca timezone-aware datetime
    diff = now - last_candle_time
    return diff.total_seconds() < 7200 # 2 godziny

def calculate_indicators(df, config):
    ind_cfg = config['indicators']

    # yfinance używa wielkich liter dla kolumn
    df['rsi'] = ta.rsi(df['Close'], length=ind_cfg['rsi_period'])
    df['atr'] = ta.atr(df['High'], df['Low'], df['Close'], length=ind_cfg['atr_period'])
    df['ema_fast'] = ta.ema(df['Close'], length=ind_cfg['ema_fast'])
    df['ema_slow'] = ta.ema(df['Close'], length=ind_cfg['ema_slow'])

    # VWAP (Volume Weighted Average Price) - Kluczowe dla akcji!
    try:
        df['vwap'] = ta.vwap(high=df['High'], low=df['Low'], close=df['Close'], volume=df['Volume'])
    except Exception:
        df['vwap'] = df['Close'] # Fallback jeśli brakuje danych do VWAP

    # VSA / Big Guy
    avg_vol = df['Volume'].rolling(window=20).mean()
    vol_spike = df['Volume'] > (avg_vol * ind_cfg['vsa_volume_multiplier'])
    spread = df['High'] - df['Low']
    avg_spread = spread.rolling(window=20).mean()
    narrow_spread = spread < avg_spread
    df['is_big_guy'] = vol_spike & narrow_spread

    return df

async def scan_market():
    config = load_config()
    scan_cfg = config['scan_settings']
    mat_cfg = config['maturation']
    tickers = scan_cfg['active_tickers']

    logger.info(f"Rozpoczynam skanowanie {len(tickers)} akcji (Interwał: {scan_cfg['timeframe']})...")
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

    for symbol in tickers:
        try:
            # Pobieranie danych z yfinance - POPRAWKA: używamy period="1mo" zamiast limit
            ticker_obj = yf.Ticker(symbol)
            df = ticker_obj.history(interval=scan_cfg['timeframe'], period="1mo")

            if df.empty or len(df) < 50:
                continue

            # Weryfikacja czy giełda w ogóle działa dla tego waloru
            last_candle_time = df.index[-1]
            if not is_market_open(last_candle_time):
                continue # Pomijamy, giełda zamknięta

            # Obliczenia wskaźników
            df = calculate_indicators(df, config)
            last_row = df.iloc[-1]

            if pd.isna(last_row['rsi']) or pd.isna(last_row['atr']):
                continue

            # Rzutowanie zmiennych (Zabezpieczenie przed błędem np.float64)
            price = float(last_row['Close'])
            volume = float(last_row['Volume'])

            indicators_json = {
                "rsi": round(float(last_row['rsi']), 2),
                "atr": float(last_row['atr']),
                "vwap": float(last_row['vwap']),
                "ema_fast": float(last_row['ema_fast']),
                "ema_slow": float(last_row['ema_slow']),
                "vsa_signal": bool(last_row['is_big_guy'])
            }

            # Zapis do bazy
            with engine.connect() as conn:
                query = text("""
                    INSERT INTO price_history (time, symbol, market_type, price, volume, indicators)
                    VALUES (NOW(), :sym, 'stocks', :pr, :vol, :ind)
                """)
                conn.execute(query, {
                    "sym": symbol, "pr": price, "vol": volume,
                    "ind": json.dumps(indicators_json)
                })
                conn.commit()

            # Logika Dojrzewania
            with engine.connect() as conn:
                check_query = text(f"""
                    SELECT count(*) FROM price_history
                    WHERE symbol = :sym
                    AND market_type = 'stocks'
                    AND (indicators->>'vsa_signal')::boolean = true
                    AND time > NOW() - INTERVAL '{mat_cfg['lookback_minutes']} minutes'
                """)

                count = conn.scalar(check_query, {"sym": symbol})

                is_uptrend = float(last_row['ema_fast']) > float(last_row['ema_slow'])
                above_vwap = price >= float(last_row['vwap'])

                if count >= mat_cfg['required_signals'] and is_uptrend and above_vwap:
                    msg = (
                        f"🏢 *SYGNAŁ GIEŁDOWY DOJRZAŁ*: {symbol}\n"
                        f"Instytucje kupują (VSA: {count}x w {mat_cfg['lookback_minutes']} min). Walor znajduje się w strefie premium.\n"
                        f"📊 *Szczegóły (Interwał {scan_cfg['timeframe']}):*\n"
                        f"• Cena: `{price:.2f}`\n"
                        f"• VWAP (Instytucje): `{indicators_json['vwap']:.2f}`\n"
                        f"• RSI: `{indicators_json['rsi']:.2f}`\n"
                        f"• Trend: `Zgodny z momentum`\n"
                        f"⏳ Przygotowuję pakiet dla AI do wyznaczenia SL/TP..."
                    )
                    await send_tg(msg)
                    logger.info(f"Wysłano powiadomienie o akcjach dla {symbol}")

        except Exception as e:
            logger.error(f"Błąd przetwarzania akcji {symbol}: {e}")

if __name__ == "__main__":
    asyncio.run(scan_market())
