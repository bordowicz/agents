import os
import json
import ccxt
import pandas as pd
import pandas_ta as ta
from sqlalchemy import create_engine, text
import telegram
import asyncio
import logging

# Konfiguracja Logów
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("Ultimate_Crypto_Agent")

# --- LISTA STABLECOINÓW DO FILTROWANIA ---
STABLECOINS = {
    'USDT', 'USDC', 'DAI', 'USDE', 'FDUSD', 'TUSD',
    'PYUSD', 'USDP', 'USDD', 'EURS', 'BUSD'
}

# ENV
DB_URL = os.getenv("DATABASE_URL")
TG_TOKEN = os.getenv("TELEGRAM_SIGNAL_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

engine = create_engine(DB_URL)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'params_crypto.json')

def load_config():
    try:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Nie znaleziono pliku konfiguracyjnego w {CONFIG_PATH}")
        exit(1)

def is_stable_pair(symbol):
    """Zwraca True, jeśli oba aktywa w parze są stablecoinami."""
    try:
        base, quote = symbol.split('/')
        return base in STABLECOINS and quote in STABLECOINS
    except Exception:
        return False

async def send_tg(message):
    if not TG_TOKEN or not CHAT_ID: return
    try:
        bot = telegram.Bot(token=TG_TOKEN)
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Telegram Error: {e}")

def calculate_vwap(df):
    df['tp'] = (df['h'] + df['l'] + df['c']) / 3
    df['tpv'] = df['tp'] * df['v']
    df['vwap'] = df['tpv'].cumsum() / df['v'].cumsum()
    return df

def calculate_ultimate_indicators(df, config):
    ind_cfg = config['indicators']
    df['rsi'] = ta.rsi(df['c'], length=ind_cfg['rsi_period'])
    df['atr'] = ta.atr(df['h'], df['l'], df['c'], length=ind_cfg['atr_period'])
    df['ema_fast'] = ta.ema(df['c'], length=ind_cfg['ema_fast'])
    df['ema_slow'] = ta.ema(df['c'], length=ind_cfg['ema_slow'])

    bb = ta.bbands(df['c'], length=ind_cfg['bb_period'], std=ind_cfg['bb_std'])
    if bb is not None and not bb.empty:
        df['bb_lower'] = bb.filter(like='BBL').iloc[:, 0]
        df['bb_mid']   = bb.filter(like='BBM').iloc[:, 0]
        df['bb_upper'] = bb.filter(like='BBU').iloc[:, 0]
        df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_mid']
        df['is_squeeze'] = df['bb_width'] < ind_cfg['bb_squeeze_threshold']
    else:
        df['is_squeeze'] = False

    df = calculate_vwap(df)
    avg_vol = df['v'].rolling(window=20).mean()
    vol_spike = df['v'] > (avg_vol * ind_cfg['vsa_volume_multiplier'])
    spread = df['h'] - df['l']
    narrow_spread = spread < spread.rolling(window=20).mean()
    df['is_big_guy'] = vol_spike & narrow_spread
    df['swing_high'] = df['h'].rolling(window=ind_cfg['swing_lookback']).max()
    df['swing_low'] = df['l'].rolling(window=ind_cfg['swing_lookback']).min()
    return df

async def scan_market():
    config = load_config()
    scan_cfg = config['scan_settings']
    mat_cfg = config['maturation']

    exchange = ccxt.binance({'enableRateLimit': True})
    tickers = exchange.fetch_tickers()

    # --- MODYFIKACJA: FILTRACJA STABLECOINÓW ---
    usdt_markets = [
        sym for sym, data in tickers.items()
        if '/USDT' in sym
        and data['quoteVolume']
        and not is_stable_pair(sym) # Odrzuca np. USDE/USDT
    ]

    # Wybór Top X na podstawie wolumenu z konfiguracji JSON
    top_symbols = sorted(
        usdt_markets,
        key=lambda x: tickers[x]['quoteVolume'],
        reverse=True
    )[:scan_cfg['top_n_volume']]

    logger.info(f"Skanowanie Ultimate Filter dla {len(top_symbols)} rynków (Top N: {scan_cfg['top_n_volume']}, Interwał: {scan_cfg['timeframe']})...")

    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

    for symbol in top_symbols:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe=scan_cfg['timeframe'], limit=scan_cfg['limit_candles'])
            df = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
            if df.empty or len(df) < 50: continue

            df = calculate_ultimate_indicators(df, config)
            last = df.iloc[-1]
            if pd.isna(last['rsi']): continue

            indicators_json = {
                "rsi": float(last['rsi']),
                "atr": float(last['atr']),
                "vwap": float(last['vwap']),
                "ema_fast": float(last['ema_fast']),
                "ema_slow": float(last['ema_slow']),
                "is_squeeze": bool(last['is_squeeze']),
                "vsa_signal": bool(last['is_big_guy']),
                "swing_h": float(last['swing_high']),
                "swing_l": float(last['swing_low'])
            }

            with engine.connect() as conn:
                query = text("""
                    INSERT INTO price_history (time, symbol, market_type, price, volume, indicators)
                    VALUES (NOW(), :s, 'crypto', :p, :v, :i)
                """)
                conn.execute(query, {
                    "s": symbol, "p": float(last['c']), "v": float(last['v']), "i": json.dumps(indicators_json)
                })
                conn.commit()

            with engine.connect() as conn:
                check_q = text(f"""
                    SELECT count(*) FROM price_history
                    WHERE symbol = :s
                    AND market_type = 'crypto'
                    AND (indicators->>'vsa_signal')::boolean = true
                    AND time > NOW() - INTERVAL '{mat_cfg['lookback_minutes']} minutes'
                """)
                vsa_count = conn.scalar(check_q, {"s": symbol})
                bullish = last['ema_fast'] > last['ema_slow']
                above_vwap = last['c'] >= last['vwap']

                if vsa_count >= mat_cfg['required_signals'] and bullish and above_vwap:
                    msg = (
                        f"💎 *ULTIMATE SETUP DETECTED*: {symbol}\n"
                        f"Złota Koniunkcja: VSA ({vsa_count}x), Trend Bullish, Wsparcie VWAP.\n\n"
                        f"📊 *Analiza Techniczna:*\n"
                        f"• Cena: `{float(last['c'])}`\n"
                        f"• VWAP: `{float(last['vwap']):.4f}`\n"
                        f"• RSI: `{float(last['rsi']):.2f}`\n"
                        f"• Bollinger Squeeze: `{'AKTYWNY' if last['is_squeeze'] else 'BRAK'}`\n"
                        f"📍 *Kontekst:* H: `{float(last['swing_high'])}` | L: `{float(last['swing_low'])}`\n\n"
                        f"⏳ Oczekiwanie na analizę AI..."
                    )
                    await send_tg(msg)
                    logger.info(f"Wysłano powiadomienie o Ultimate Setup dla {symbol}")

        except Exception as e:
            logger.error(f"Error {symbol}: {e}")

if __name__ == "__main__":
    asyncio.run(scan_market())
