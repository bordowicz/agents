import os
import json
import ccxt
import pandas as pd
import pandas_ta as ta
from sqlalchemy import create_engine, text
import telegram
import asyncio
import logging
from decimal import Decimal

# Konfiguracja Logów
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("Ultimate_Crypto_Agent")

# --- LISTA STABLECOINÓW DO FILTROWANIA ---
STABLECOINS = {
    'USDT', 'USDC', 'DAI', 'USDE', 'FDUSD', 'TUSD',
    'PYUSD', 'USDP', 'USDD', 'EURS', 'BUSD'
}

# ENV
DB_URL: str = os.getenv("DATABASE_URL", "")
TG_TOKEN: str = os.getenv("TELEGRAM_SIGNAL_TOKEN", "")
CHAT_ID: str = os.getenv("CHAT_ID", "")

engine = create_engine(DB_URL)
BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH: str = os.path.join(BASE_DIR, 'config', 'params_crypto.json')

def load_config() -> dict:
    try:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Nie znaleziono pliku konfiguracyjnego w {CONFIG_PATH}")
        exit(1)

def is_stable_pair(symbol: str) -> bool:
    """Zwraca True, jeśli oba aktywa w parze są stablecoinami."""
    try:
        base, quote = symbol.split('/')
        return base in STABLECOINS and quote in STABLECOINS
    except Exception:
        return False

async def send_tg(message: str) -> None:
    if not TG_TOKEN or not CHAT_ID: return
    try:
        bot = telegram.Bot(token=TG_TOKEN)
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='HTML') # Zmiana na HTML dla lepszej czytelności
    except Exception as e:
        logger.error(f"Telegram Error: {e}")

def calculate_vwap(df: pd.DataFrame) -> pd.DataFrame:
    df['tp'] = (df['h'] + df['l'] + df['c']) / 3
    df['tpv'] = df['tp'] * df['v']
    df['vwap'] = df['tpv'].cumsum() / df['v'].cumsum()
    return df

def calculate_ultimate_indicators(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    ind_cfg = config['indicators']
    df['rsi'] = ta.rsi(df['c'], length=ind_cfg['rsi_period'])
    df['atr'] = ta.atr(df['h'], df['l'], df['c'], length=ind_cfg['atr_period'])
    df['ema_fast'] = ta.ema(df['c'], length=ind_cfg['ema_fast'])
    df['ema_slow'] = ta.ema(df['c'], length=ind_cfg['ema_slow'])

    macd = ta.macd(df['c'], fast=ind_cfg['macd_fast'], slow=ind_cfg['macd_slow'], signal=ind_cfg['macd_signal'])
    if macd is not None and not macd.empty:
        df['macd'] = macd.iloc[:, 0]
        df['macdh'] = macd.iloc[:, 1]
        df['macds'] = macd.iloc[:, 2]
    else:
        df['macd'] = pd.NA
        df['macdh'] = pd.NA
        df['macds'] = pd.NA

    adx = ta.adx(df['h'], df['l'], df['c'], length=ind_cfg['adx_period'])
    if adx is not None and not adx.empty:
        df['adx'] = adx.iloc[:, 0]
        df['dmp'] = adx.iloc[:, 1]
        df['dmn'] = adx.iloc[:, 2]
    else:
        df['adx'] = pd.NA
        df['dmp'] = pd.NA
        df['dmn'] = pd.NA

    bb = ta.bbands(df['c'], length=ind_cfg['bb_period'], std=ind_cfg['bb_std'])
    if bb is not None and not bb.empty:
        df['bb_lower'] = bb.iloc[:, 0]
        df['bb_mid']   = bb.iloc[:, 1]
        df['bb_upper'] = bb.iloc[:, 2]
        df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_mid']
        df['is_squeeze'] = df['bb_width'] < ind_cfg['bb_squeeze_threshold']
    else:
        df['is_squeeze'] = False
        df['bb_lower'] = pd.NA
        df['bb_mid'] = pd.NA
        df['bb_upper'] = pd.NA
        df['bb_width'] = pd.NA

    df = calculate_vwap(df)
    avg_vol = df['v'].rolling(window=20).mean()
    vol_spike = df['v'] > (avg_vol * ind_cfg['vsa_volume_multiplier'])
    spread = df['h'] - df['l']
    narrow_spread = spread < spread.rolling(window=20).mean()
    df['is_big_guy'] = vol_spike & narrow_spread
    df['swing_high'] = df['h'].rolling(window=ind_cfg['swing_lookback']).max()
    df['swing_low'] = df['l'].rolling(window=ind_cfg['swing_lookback']).min()
    df['rsi_slope'] = df['rsi'].diff(3)

    return df

async def scan_market() -> None:
    config: dict = load_config()
    scan_cfg: dict = config['scan_settings']
    mat_cfg: dict = config['maturation']
    ind_cfg: dict = config['indicators']

    exchange = ccxt.binance({'enableRateLimit': True})
    tickers = exchange.fetch_tickers()

    usdt_markets = [
        sym for sym, data in tickers.items()
        if '/USDT' in sym
        and data['quoteVolume'] is not None
        and not is_stable_pair(sym)
    ]

    top_symbols: list[str] = sorted(
        usdt_markets,
        key=lambda x: tickers[x]['quoteVolume'],
        reverse=True
    )[:scan_cfg['top_n_volume']]

    logger.info(f"Skanowanie Ultimate Filter dla {len(top_symbols)} rynków...")

    for symbol in top_symbols:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe=scan_cfg['timeframe'], limit=scan_cfg['limit_candles'])
            df = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])

            max_lookback = max(
                ind_cfg['rsi_period'], ind_cfg['atr_period'], ind_cfg['ema_slow'],
                ind_cfg['bb_period'], ind_cfg['macd_slow'] + ind_cfg['macd_signal'],
                ind_cfg['adx_period'], ind_cfg['swing_lookback']
            )

            if df.empty or len(df) < max_lookback + 10:
                continue

            df['o'] = pd.to_numeric(df['o'], errors='coerce')
            df['h'] = pd.to_numeric(df['h'], errors='coerce')
            df['l'] = pd.to_numeric(df['l'], errors='coerce')
            df['c'] = pd.to_numeric(df['c'], errors='coerce')
            df['v'] = pd.to_numeric(df['v'], errors='coerce')

            df = calculate_ultimate_indicators(df, config)
            last = df.iloc[-1]
            if pd.isna(last['rsi']) or pd.isna(last['vwap']) or pd.isna(last['macd']) or pd.isna(last['adx']):
                continue

            indicators_json = {
                "rsi": float(last['rsi']),
                "atr": float(last['atr']),
                "vwap": float(last['vwap']),
                "ema_fast": float(last['ema_fast']),
                "ema_slow": float(last['ema_slow']),
                "macd": float(last['macd']),
                "macdh": float(last['macdh']),
                "macds": float(last['macds']),
                "adx": float(last['adx']),
                "dmp": float(last['dmp']),
                "dmn": float(last['dmn']),
                "is_squeeze": bool(last['is_squeeze']),
                "vsa_signal": bool(last['is_big_guy']),
                "swing_h": float(last['swing_high']),
                "swing_l": float(last['swing_low']),
                "rsi_slope": float(last['rsi_slope']) if not pd.isna(last['rsi_slope']) else 0.0
            }

            current_price: Decimal = Decimal(str(last['c']))
            current_volume: Decimal = Decimal(str(last['v']))

            with engine.connect() as conn:
                query = text("""
                    INSERT INTO price_history (time, symbol, market_type, price, volume, indicators)
                    VALUES (NOW(), :s, 'crypto', :p, :v, :i)
                """)
                conn.execute(query, {
                    "s": symbol, "p": current_price, "v": current_volume, "i": json.dumps(indicators_json)
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
                vsa_count: int = conn.scalar(check_q, {"s": symbol})

                recent_candles = df.tail(3)
                price_above_vwap: bool = (recent_candles['c'] >= recent_candles['vwap']).all()
                price_below_vwap: bool = (recent_candles['c'] <= recent_candles['vwap']).all()

                # --- LOGIKA LONG ---
                long_bullish_ema: bool = last['ema_fast'] > last['ema_slow']
                long_macd_bullish: bool = last['macd'] > last['macds'] and last['macdh'] > 0
                long_adx_strong: bool = last['adx'] > ind_cfg['adx_threshold'] and last['dmp'] > last['dmn']
                long_rsi_healthy: bool = last['rsi'] > 45 and last['rsi_slope'] >= -1.0

                # --- LOGIKA SHORT ---
                short_bearish_ema: bool = last['ema_fast'] < last['ema_slow']
                short_macd_bearish: bool = last['macd'] < last['macds'] and last['macdh'] < 0
                short_adx_strong: bool = last['adx'] > ind_cfg['adx_threshold'] and last['dmn'] > last['dmp']
                short_rsi_weak: bool = last['rsi'] < 55 and last['rsi_slope'] <= 1.0

                setup_detected = False
                direction = ""
                reasoning = ""

                if vsa_count >= mat_cfg['required_signals']:
                    if price_above_vwap and long_bullish_ema and long_macd_bullish and long_adx_strong and long_rsi_healthy:
                        setup_detected = True
                        direction = "LONG"
                        reasoning = "Wzrostowy (Cena nad VWAP, Trend Bullish)"
                    elif price_below_vwap and short_bearish_ema and short_macd_bearish and short_adx_strong and short_rsi_weak:
                        setup_detected = True
                        direction = "SHORT"
                        reasoning = "Spadkowy (Cena pod VWAP, Trend Bearish)"

                if setup_detected:
                    direction_icon = "📈" if direction == "LONG" else "📉"
                    direction_text = "WZROST" if direction == "LONG" else "SPADEK"

                    msg = (
                        f"{direction_icon} <b>POTENCJALNY {direction_text} (Setup {direction}): {symbol}</b>\n\n"
                        f"🚀 <b>Dlaczego?</b>\n"
                        f"• Trend: <u>{reasoning}</u>\n"
                        f"• Aktywność Dużych Graczy (VSA): Tak ({vsa_count}x)\n"
                        f"• Siła Trendu (ADX): {float(last['adx']):.1f} (Silny)\n\n"
                        f"📊 <b>Dane techniczne:</b>\n"
                        f"• Cena: <code>{current_price}</code>\n"
                        f"• RSI: <code>{float(last['rsi']):.1f}</code>\n"
                        f"• Bollinger Squeeze: <code>{'TAK' if last['is_squeeze'] else 'NIE'}</code>\n\n"
                        f"⏳ <i>Przekazuję dane do eksperta AI w celu wyznaczenia SL/TP...</i>"
                    )
                    await send_tg(msg)
                    logger.info(f"Wykryto setup {direction} dla {symbol}")

        except Exception as e:
            logger.error(f"Error {symbol}: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(scan_market())
