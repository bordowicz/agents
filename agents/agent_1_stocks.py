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
from decimal import Decimal

# Konfiguracja Logów
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("Stocks_Agent_1")

# Zmienne środowiskowe
DB_URL: str = os.getenv("DATABASE_URL", "")
TG_TOKEN: str = os.getenv("TELEGRAM_SIGNAL_TOKEN", "")
CHAT_ID: str = os.getenv("CHAT_ID", "")

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

async def send_tg(message: str) -> None:
    if not TG_TOKEN or not CHAT_ID:
        logger.warning("Brak konfiguracji Telegram.")
        return
    try:
        bot = telegram.Bot(token=TG_TOKEN)
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='HTML') # Zmiana na HTML dla lepszej czytelności
    except Exception as e:
        logger.error(f"Błąd Telegram: {e}")

def is_market_open(last_candle_time: datetime) -> bool:
    """
    Zabezpieczenie: Sprawdza, czy ostatnia świeca nie jest starsza niż 2 godziny.
    Zapobiega generowaniu sygnałów w weekendy lub po zamknięciu sesji.
    """
    now = datetime.now(timezone.utc)
    # yfinance zwraca timezone-aware datetime
    diff = now - last_candle_time
    return diff.total_seconds() < 7200 # 2 godziny

def calculate_indicators(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    ind_cfg = config['indicators']

    # yfinance używa wielkich liter dla kolumn
    df['rsi'] = ta.rsi(df['Close'], length=ind_cfg['rsi_period'])
    df['atr'] = ta.atr(df['High'], df['Low'], df['Close'], length=ind_cfg['atr_period'])
    df['ema_fast'] = ta.ema(df['Close'], length=ind_cfg['ema_fast'])
    df['ema_slow'] = ta.ema(df['Close'], length=ind_cfg['ema_slow'])

    # MACD
    macd = ta.macd(df['Close'], fast=ind_cfg['macd_fast'], slow=ind_cfg['macd_slow'], signal=ind_cfg['macd_signal'])
    if macd is not None and not macd.empty:
        df['macd'] = macd.iloc[:, 0]
        df['macdh'] = macd.iloc[:, 1]
        df['macds'] = macd.iloc[:, 2]
    else:
        df['macd'] = pd.NA
        df['macdh'] = pd.NA
        df['macds'] = pd.NA

    # ADX
    adx = ta.adx(df['High'], df['Low'], df['Close'], length=ind_cfg['adx_period'])
    if adx is not None and not adx.empty:
        df['adx'] = adx.iloc[:, 0]
        df['dmp'] = adx.iloc[:, 1]
        df['dmn'] = adx.iloc[:, 2]
    else:
        df['adx'] = pd.NA
        df['dmp'] = pd.NA
        df['dmn'] = pd.NA

    # Stochastic
    stoch = ta.stoch(df['High'], df['Low'], df['Close'], k=ind_cfg['stoch_k'], d=ind_cfg['stoch_d'])
    if stoch is not None and not stoch.empty:
        df['stoch_k'] = stoch.iloc[:, 0]
        df['stoch_d'] = stoch.iloc[:, 1]
    else:
        df['stoch_k'] = pd.NA
        df['stoch_d'] = pd.NA

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

    # Dodatkowe: RSI Trend (nachylenie z ostatnich 3 świec)
    df['rsi_slope'] = df['rsi'].diff(3)

    return df

async def scan_market() -> None:
    config: dict = load_config()
    scan_cfg: dict = config['scan_settings']
    mat_cfg: dict = config['maturation']
    ind_cfg: dict = config['indicators']
    tickers: list[str] = scan_cfg['active_tickers']

    logger.info(f"Rozpoczynam skanowanie {len(tickers)} akcji (Interwał: {scan_cfg['timeframe']})...")

    for symbol in tickers:
        try:
            # Pobieranie danych z yfinance
            ticker_obj = yf.Ticker(symbol)
            df = ticker_obj.history(interval=scan_cfg['timeframe'], period="1mo")

            max_lookback = max(
                ind_cfg['rsi_period'], ind_cfg['atr_period'], ind_cfg['ema_slow'],
                ind_cfg['macd_slow'] + ind_cfg['macd_signal'], ind_cfg['adx_period'],
                ind_cfg['stoch_k'] + ind_cfg['stoch_d']
            )

            if df.empty or len(df) < max_lookback + 10:
                logger.warning(f"Niewystarczająca ilość danych dla {symbol}. Dostępne: {len(df)}")
                continue

            # Weryfikacja czy giełda w ogóle działa dla tego waloru
            last_candle_time = df.index[-1]
            if not is_market_open(last_candle_time):
                continue # Pomijamy, giełda zamknięta

            # Obliczenia wskaźników
            df = calculate_indicators(df, config)
            last_row = df.iloc[-1]

            if pd.isna(last_row['rsi']) or pd.isna(last_row['atr']) or pd.isna(last_row['macd']) or pd.isna(last_row['adx']):
                continue

            # Precision using Decimal
            current_price: Decimal = Decimal(str(last_row['Close']))
            current_volume: Decimal = Decimal(str(last_row['Volume']))

            indicators_json = {
                "rsi": round(float(last_row['rsi']), 2),
                "atr": float(last_row['atr']),
                "vwap": float(last_row['vwap']),
                "ema_fast": float(last_row['ema_fast']),
                "ema_slow": float(last_row['ema_slow']),
                "macd": float(last_row['macd']),
                "macdh": float(last_row['macdh']),
                "macds": float(last_row['macds']),
                "adx": float(last_row['adx']),
                "dmp": float(last_row['dmp']),
                "dmn": float(last_row['dmn']),
                "stoch_k": float(last_row['stoch_k']),
                "stoch_d": float(last_row['stoch_d']),
                "vsa_signal": bool(last_row['is_big_guy']),
                "rsi_slope": float(last_row['rsi_slope']) if not pd.isna(last_row['rsi_slope']) else 0.0
            }

            # Zapis do bazy
            with engine.connect() as conn:
                query = text("""
                    INSERT INTO price_history (time, symbol, market_type, price, volume, indicators)
                    VALUES (NOW(), :sym, 'stocks', :pr, :vol, :ind)
                """)
                conn.execute(query, {
                    "sym": symbol,
                    "pr": current_price,
                    "vol": current_volume,
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

                vsa_count: int = conn.scalar(check_query, {"sym": symbol})

                recent_candles = df.tail(3)
                price_above_vwap: bool = (recent_candles['Close'] >= recent_candles['vwap']).all()
                price_below_vwap: bool = (recent_candles['Close'] <= recent_candles['vwap']).all()

                # --- LOGIKA LONG ---
                long_bullish_ema: bool = float(last_row['ema_fast']) > float(last_row['ema_slow'])
                long_macd_bullish: bool = last_row['macd'] > last_row['macds'] and last_row['macdh'] > 0
                long_adx_strong: bool = last_row['adx'] > ind_cfg['adx_threshold'] and last_row['dmp'] > last_row['dmn']
                long_rsi_healthy: bool = last_row['rsi'] > 45 and last_row['rsi_slope'] >= -1.0
                long_stoch_not_overbought: bool = last_row['stoch_k'] < 80

                # --- LOGIKA SHORT ---
                short_bearish_ema: bool = float(last_row['ema_fast']) < float(last_row['ema_slow'])
                short_macd_bearish: bool = last_row['macd'] < last_row['macds'] and last_row['macdh'] < 0
                short_adx_strong: bool = last_row['adx'] > ind_cfg['adx_threshold'] and last_row['dmn'] > last_row['dmp']
                short_rsi_weak: bool = last_row['rsi'] < 55 and last_row['rsi_slope'] <= 1.0
                short_stoch_not_oversold: bool = last_row['stoch_k'] > 20

                setup_detected = False
                direction = ""
                reasoning = ""

                if vsa_count >= mat_cfg['required_signals']:
                    if (price_above_vwap and long_bullish_ema and long_macd_bullish and
                        long_adx_strong and long_rsi_healthy and long_stoch_not_overbought):
                        setup_detected = True
                        direction = "LONG"
                        reasoning = "Wzrostowy (Cena nad VWAP, Trend Bullish, Stochastic nie wykupiony)"
                    elif (price_below_vwap and short_bearish_ema and short_macd_bearish and
                          short_adx_strong and short_rsi_weak and short_stoch_not_oversold):
                        setup_detected = True
                        direction = "SHORT"
                        reasoning = "Spadkowy (Cena pod VWAP, Trend Bearish, Stochastic nie wyprzedany)"

                if setup_detected:
                    direction_icon = "📈" if direction == "LONG" else "📉"
                    direction_text = "WZROST" if direction == "LONG" else "SPADEK"

                    msg = (
                        f"{direction_icon} <b>POTENCJALNY {direction_text} (Setup {direction}): {symbol}</b>\n\n"
                        f"🚀 <b>Dlaczego?</b>\n"
                        f"• Trend: <u>{reasoning}</u>\n"
                        f"• Aktywność Dużych Graczy (VSA): Tak ({vsa_count}x)\n"
                        f"• Siła Trendu (ADX): {float(last_row['adx']):.1f} (Silny)\n\n"
                        f"📊 <b>Dane techniczne:</b>\n"
                        f"• Cena: <code>{current_price:.2f}</code>\n"
                        f"• RSI: <code>{float(last_row['rsi']):.1f}</code>\n"
                        f"• Stochastic: <code>{float(last_row['stoch_k']):.1f}</code>\n\n"
                        f"⏳ <i>Przekazuję dane do eksperta AI w celu wyznaczenia SL/TP...</i>"
                    )
                    await send_tg(msg)
                    logger.info(f"Wykryto setup {direction} dla {symbol}")

        except Exception as e:
            logger.error(f"Błąd przetwarzania akcji {symbol}: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(scan_market())
