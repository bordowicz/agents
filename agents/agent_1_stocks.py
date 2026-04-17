import os
import json
import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np
from sqlalchemy import create_engine, text
import requests
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal

# Konfiguracja Logów
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("Stocks_Agent_1")

# Zmienne środowiskowe
DB_URL: str = os.getenv("DATABASE_URL", "")
TG_TOKEN: str = os.getenv("TELEGRAM_SIGNAL_TOKEN", "")
CHAT_ID: str = os.getenv("GROUP_CHAT_ID", "")
SCANNER_THREAD_ID: str = os.getenv("SCANNER_THREAD_STOCKS_ID", "9")

engine = create_engine(DB_URL)
BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH: str = os.path.join(BASE_DIR, 'config', 'params_stocks.json')

def load_config() -> dict:
    try:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Nie znaleziono pliku konfiguracyjnego w {CONFIG_PATH}")
        exit(1)

def safe_float(val):
    """Konwertuje wartość na float, zamieniając NaN/Inf na 0.0."""
    try:
        if val is None or np.isnan(val) or np.isinf(val):
            return 0.0
        return float(val)
    except:
        return 0.0

async def send_tg_topic(message: str) -> None:
    if not TG_TOKEN or not CHAT_ID: return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    if SCANNER_THREAD_ID:
        payload["message_thread_id"] = int(SCANNER_THREAD_ID)

    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: requests.post(url, json=payload, timeout=10))
    except Exception as e:
        logger.error(f"Telegram Error: {e}")

def is_market_open(last_candle_time: datetime) -> bool:
    now = datetime.now(timezone.utc)
    diff = now - last_candle_time
    return diff.total_seconds() < 86400

def calculate_indicators(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    ind_cfg = config['indicators']
    df['rsi'] = ta.rsi(df['Close'], length=ind_cfg['rsi_period'])
    df['atr'] = ta.atr(df['High'], df['Low'], df['Close'], length=ind_cfg['atr_period'])
    df['ema_fast'] = ta.ema(df['Close'], length=ind_cfg['ema_fast'])
    df['ema_slow'] = ta.ema(df['Close'], length=ind_cfg['ema_slow'])

    macd = ta.macd(df['Close'], fast=ind_cfg['macd_fast'], slow=ind_cfg['macd_slow'], signal=ind_cfg['macd_signal'])
    if macd is not None and not macd.empty:
        df['macd'] = macd.iloc[:, 0]
        df['macdh'] = macd.iloc[:, 1]
        df['macds'] = macd.iloc[:, 2]
    else:
        df['macd'] = 0.0; df['macdh'] = 0.0; df['macds'] = 0.0

    adx = ta.adx(df['High'], df['Low'], df['Close'], length=ind_cfg['adx_period'])
    if adx is not None and not adx.empty:
        df['adx'] = adx.iloc[:, 0]; df['dmp'] = adx.iloc[:, 1]; df['dmn'] = adx.iloc[:, 2]
    else:
        df['adx'] = 0.0; df['dmp'] = 0.0; df['dmn'] = 0.0

    stoch = ta.stoch(df['High'], df['Low'], df['Close'], k=ind_cfg['stoch_k'], d=ind_cfg['stoch_d'])
    if stoch is not None and not stoch.empty:
        df['stoch_k'] = stoch.iloc[:, 0]; df['stoch_d'] = stoch.iloc[:, 1]
    else:
        df['stoch_k'] = 0.0; df['stoch_d'] = 0.0

    try:
        vwap_val = ta.vwap(high=df['High'], low=df['Low'], close=df['Close'], volume=df['Volume'])
        df['vwap'] = vwap_val if vwap_val is not None else df['Close']
    except Exception:
        df['vwap'] = df['Close']

    df['swing_high'] = df['High'].rolling(window=ind_cfg['swing_lookback']).max()
    df['swing_low'] = df['Low'].rolling(window=ind_cfg['swing_lookback']).min()
    df['hybrid_sl_long'] = df[['swing_low', 'vwap']].min(axis=1) - (0.5 * df['atr'].fillna(0))
    df['hybrid_tp_long'] = df['swing_high'] - (0.2 * df['atr'].fillna(0))
    df['hybrid_sl_short'] = df[['swing_high', 'vwap']].max(axis=1) + (0.5 * df['atr'].fillna(0))
    df['hybrid_tp_short'] = df['swing_low'] + (0.2 * df['atr'].fillna(0))

    avg_vol = df['Volume'].rolling(window=20).mean()
    df['is_big_guy'] = (df['Volume'] > (avg_vol * ind_cfg['vsa_volume_multiplier'])) & \
                       ((df['High'] - df['Low']) < (df['High'] - df['Low']).rolling(window=20).mean())
    df['rsi_slope'] = df['rsi'].diff(3)
    return df

async def scan_market() -> None:
    config: dict = load_config()
    scan_cfg: dict = config['scan_settings']
    mat_cfg: dict = config['maturation']
    ind_cfg: dict = config['indicators']
    tickers: list[str] = scan_cfg['active_tickers']

    logger.info(f"Skanowanie {len(tickers)} akcji...")

    for symbol in tickers:
        try:
            ticker_obj = yf.Ticker(symbol)
            df = ticker_obj.history(interval=scan_cfg['timeframe'], period="1mo")

            if df.empty or len(df) < 50: continue
            if not is_market_open(df.index[-1]): continue

            df = calculate_indicators(df, config)
            last_row = df.iloc[-1]

            if pd.isna(last_row['rsi']): continue

            current_price = Decimal(str(last_row['Close']))
            current_volume = Decimal(str(last_row['Volume']))

            # Bezpieczna budowa JSON
            indicators_json = {
                "rsi": safe_float(last_row['rsi']),
                "atr": safe_float(last_row['atr']),
                "vwap": safe_float(last_row['vwap']),
                "ema_fast": safe_float(last_row['ema_fast']),
                "ema_slow": safe_float(last_row['ema_slow']),
                "macd": safe_float(last_row['macd']),
                "macdh": safe_float(last_row['macdh']),
                "macds": safe_float(last_row['macds']),
                "adx": safe_float(last_row['adx']),
                "dmp": safe_float(last_row['dmp']),
                "dmn": safe_float(last_row['dmn']),
                "stoch_k": safe_float(last_row['stoch_k']),
                "stoch_d": safe_float(last_row['stoch_d']),
                "vsa_signal": bool(last_row['is_big_guy']),
                "rsi_slope": safe_float(last_row['rsi_slope']),
                "sl_hybrid_long": safe_float(last_row['hybrid_sl_long']),
                "tp_hybrid_long": safe_float(last_row['hybrid_tp_long']),
                "sl_hybrid_short": safe_float(last_row['hybrid_sl_short']),
                "tp_hybrid_short": safe_float(last_row['hybrid_tp_short'])
            }

            with engine.connect() as conn:
                query = text("""
                    INSERT INTO price_history (time, symbol, market_type, price, volume, indicators)
                    VALUES (NOW(), :sym, 'stocks', :pr, :vol, :ind)
                """)
                conn.execute(query, {"sym": symbol, "pr": current_price, "vol": current_volume, "ind": json.dumps(indicators_json)})
                conn.commit()

            with engine.connect() as conn:
                check_query = text(f"""
                    SELECT count(*) FROM price_history
                    WHERE symbol = :sym AND market_type = 'stocks'
                    AND (indicators->>'vsa_signal')::boolean = true
                    AND time > NOW() - INTERVAL '{mat_cfg['lookback_minutes']} minutes'
                """)
                vsa_count: int = conn.scalar(check_query, {"sym": symbol})

                recent = df.tail(3)
                price_above_vwap = (recent['Close'] >= recent['vwap']).all()
                price_below_vwap = (recent['Close'] <= recent['vwap']).all()

                # --- NOWOŚĆ: Obliczanie RR i segregacja alertów ---
                setup_detected = False
                manual_alert = False
                direction = ""
                sl_val, tp_val, rr_ratio = 0.0, 0.0, 0.0

                if vsa_count >= mat_cfg['required_signals']:
                    curr_price_f = safe_float(last_row['Close'])

                    # Wczytanie konfiguracji ryzyka raz dla obu kierunków
                    risk_cfg: dict = config.get('risk_management', {'min_rr_ratio': 2.0, 'manual_alert_rr_ratio': 1.5})
                    MIN_RR = risk_cfg['min_rr_ratio']
                    MANUAL_RR = risk_cfg['manual_alert_rr_ratio']

                    # WERYFIKACJA LONG
                    if price_above_vwap and safe_float(last_row['ema_fast']) > safe_float(last_row['ema_slow']):
                        direction = "LONG"
                        sl_val = safe_float(last_row['hybrid_sl_long'])
                        tp_val = safe_float(last_row['hybrid_tp_long'])
                        risk = curr_price_f - sl_val
                        reward = tp_val - curr_price_f
                        rr_ratio = reward / risk if risk > 0 else 0

                        if rr_ratio >= MIN_RR:
                            setup_detected = True
                        elif rr_ratio >= MANUAL_RR:
                            manual_alert = True

                    # WERYFIKACJA SHORT
                    elif price_below_vwap and safe_float(last_row['ema_fast']) < safe_float(last_row['ema_slow']):
                        direction = "SHORT"
                        sl_val = safe_float(last_row['hybrid_sl_short'])
                        tp_val = safe_float(last_row['hybrid_tp_short'])
                        risk = sl_val - curr_price_f
                        reward = curr_price_f - tp_val
                        rr_ratio = reward / risk if risk > 0 else 0

                        if rr_ratio >= MIN_RR:
                            setup_detected = True
                        elif rr_ratio >= MANUAL_RR:
                            manual_alert = True

                # SCIEŻKA 1: Setup dla AI (RR >= MIN_RR)
                if setup_detected:
                    icon = "📈" if direction == "LONG" else "📉"
                    msg = (
                        f"{icon} <b>DETEKCJA SETUPU (STOCKS): {symbol}</b>\n\n"
                        f"🚀 <b>Typ:</b> {direction}\n"
                        f"• VSA Signals: {vsa_count}x\n\n"
                        f"📊 <b>Dane:</b>\n"
                        f"• Cena: <code>{safe_float(last_row['Close']):.2f}</code>\n"
                        f"• RSI: <code>{safe_float(last_row['rsi']):.1f}</code>\n"
                        f"• Hybrydowy SL: <code>{sl_val:.2f}</code>\n"
                        f"• Hybrydowy TP: <code>{tp_val:.2f}</code>\n"
                        f"⚖️ <b>Risk/Reward:</b> <code>1:{rr_ratio:.2f}</code>\n\n"
                        f"⏳ <i>Przekazuję dane do analizy AI...</i>"
                    )
                    await send_tg_topic(msg)
                    logger.info(f"Wykryto setup {direction} dla {symbol} (RR: {rr_ratio:.2f})")

                # SCIEŻKA 2: Setup manualny (MANUAL_RR <= RR < MIN_RR)
                elif manual_alert:
                    icon = "👀"
                    msg = (
                        f"{icon} <b>OBSERWACJA RĘCZNA (STOCKS): {symbol}</b>\n\n"
                        f"🚀 <b>Typ:</b> {direction}\n"
                        f"• VSA Signals: {vsa_count}x\n\n"
                        f"📊 <b>Dane:</b>\n"
                        f"• Cena: <code>{safe_float(last_row['Close']):.2f}</code>\n"
                        f"• Hybrydowy SL: <code>{sl_val:.2f}</code>\n"
                        f"• Hybrydowy TP: <code>{tp_val:.2f}</code>\n"
                        f"⚖️ <b>Risk/Reward:</b> <code>1:{rr_ratio:.2f}</code>\n\n"
                        f"⛔ <i>RR zbyt niskie dla AI. Trade do oceny manualnej.</i>"
                    )
                    await send_tg_topic(msg)
                    logger.info(f"Wykryto manualny setup {direction} dla {symbol} (RR: {rr_ratio:.2f} - odrzucony przez twardy filtr)")

        except Exception as e:
            logger.error(f"Błąd {symbol}: {e}")

if __name__ == "__main__":
    asyncio.run(scan_market())