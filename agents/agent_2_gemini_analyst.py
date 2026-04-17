import os
import json
import pandas as pd
import asyncio
import logging
import requests
import html
import warnings
from datetime import datetime

# Całkowite wyciszenie ostrzeżeń
warnings.filterwarnings("ignore", category=FutureWarning)
os.environ["PYTHONWARNINGS"] = "ignore"

import google.generativeai as genai
from google.api_core import exceptions
from sqlalchemy import create_engine, text

# Konfiguracja Logów
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("Gemini_Analyst_Agent_Pro")

# ENV
DB_URL: str = os.getenv("DATABASE_URL", "")
TG_TOKEN: str = os.getenv("TELEGRAM_SIGNAL_TOKEN", "")
CHAT_ID: str = os.getenv("GROUP_CHAT_ID", "")
THREAD_ID: str = os.getenv("GEMINI_THREAD_ID", "5")
GEMINI_KEY: str = os.getenv("GEMINI_API_KEY", "")

engine = create_engine(DB_URL)

BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# --- NOWA FUNKCJA: Wczytywanie konfiguracji dla danego rynku ---
def load_market_config(market_type: str) -> dict:
    config_file = f'params_{market_type}.json'
    config_path = os.path.join(BASE_DIR, 'config', config_file)
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Nie można załadować konfiguracji {config_file}: {e}")
        return {}

def calculate_potential_rr(entry, sl, tp, direction):
    try:
        entry, sl, tp = float(entry), float(sl), float(tp)
        if direction == "LONG":
            risk = entry - sl
            reward = tp - entry
        else:
            risk = sl - entry
            reward = entry - tp

        if risk <= 0: return 0
        return reward / risk
    except Exception:
        return 0

def initialize_gemini():
    """Dynamicznie znajduje dostępny model flash lub fallbackuje do gemini-pro."""
    if not GEMINI_KEY:
        logger.error("Brak klucza GEMINI_API_KEY!")
        return None
    genai.configure(api_key=GEMINI_KEY)
    try:
        all_models = genai.list_models()
        available_for_content_generation = [
            m for m in all_models if 'generateContent' in m.supported_generation_methods
        ]

        flash_model_name = None
        for version in ['2.5', '2.0', '1.5', 'flash']:
            for m in available_for_content_generation:
                if version in m.name:
                    flash_model_name = m.name
                    break
            if flash_model_name: break

        if flash_model_name:
            logger.info(f"🚀 Inicjalizacja Gemini: {flash_model_name}")
            return genai.GenerativeModel(flash_model_name)
        else:
            if any('pro' in m.name for m in available_for_content_generation):
                logger.warning("Brak flash, używam gemini-pro")
                return genai.GenerativeModel('gemini-pro')
            return None

    except Exception as e:
        logger.error(f"Błąd inicjalizacji Gemini: {e}")
        return None

model = initialize_gemini()

async def send_tg_topic(message: str) -> None:
    if not TG_TOKEN or not CHAT_ID: return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "message_thread_id": int(THREAD_ID)
    }
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: requests.post(url, json=payload, timeout=10))
    except Exception as e:
        logger.error(f"Telegram Error: {e}")

def get_pending_setups():
    query = text("""
        WITH RecentSetups AS (
            SELECT symbol, MAX(time) as setup_time, market_type
            FROM price_history
            WHERE (indicators->>'vsa_signal')::boolean = true
              AND time > NOW() - INTERVAL '30 minutes'
            GROUP BY symbol, market_type
        )
        SELECT r.symbol, r.market_type
        FROM RecentSetups r
        LEFT JOIN signals_gemini s ON r.symbol = s.symbol AND s.created_at > NOW() - INTERVAL '4 hours'
        WHERE s.id IS NULL;
    """)
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        return [(row[0], row[1]) for row in result]

def get_market_context(symbol: str) -> pd.DataFrame:
    query = text("""
        SELECT time, price, volume, indicators
        FROM price_history
        WHERE symbol = :sym
        ORDER BY time DESC LIMIT 30
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"sym": symbol})
    df = df.sort_values(by='time')
    return df

# --- ZMIANA: Funkcja przyjmuje teraz min_rr ---
async def ask_gemini_with_backoff(symbol: str, df_context: pd.DataFrame, direction: str, sl: float, tp: float, min_rr: float) -> dict | None:
    if not model: return None

    last_row = df_context.iloc[-1]

    context_str = ""
    for _, row in df_context.iterrows():
        ind = row['indicators']
        if isinstance(ind, str): ind = json.loads(ind)
        context_str += (
            f"T: {row['time']}, P: {row['price']}, V: {row['volume']}, "
            f"VWAP: {ind.get('vwap', 0):.4f}, RSI: {ind.get('rsi', 0):.1f}\n"
        )

    # --- ZMIANA: Dynamiczny prompt z wartością min_rr ---
    prompt = f"""You are an elite Quant Trader/Senior Hedge Fund Analyst. Analyze 30 periods for {symbol}.
    STRICT RULE: Only approve trades with high technical conviction. Math check has been done: RR is {min_rr}+.

    CRITERIA:
    LONG: Price above VWAP, EMA Fast > Slow, MACD bullish.
    SHORT: Price below VWAP, EMA Fast < Slow, MACD bearish.

    TASK: Validate the setup. If trend alignment is weak, REJECT.
    Return ONLY a raw JSON object:
    {{
      "status": "APPROVED" | "REJECTED",
      "direction": "{direction}",
      "entry": {float(last_row['price'])},
      "sl": {sl},
      "tp": {tp},
      "reasoning": "string"
    }}"""

    for i in range(3):
        try:
            response = await asyncio.to_thread(model.generate_content, prompt + "\nData:\n" + context_str)
            result_text = response.text.strip()
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0].strip()

            result_text = result_text.replace("`", "").replace("json", "").strip()
            return json.loads(result_text)
        except exceptions.ResourceExhausted:
            await asyncio.sleep(10 * (i + 1))
        except Exception as e:
            logger.error(f"Gemini error for {symbol}: {e}")
            await asyncio.sleep(2)
    return None

async def main():
    if not model: return
    setups = get_pending_setups()
    if not setups: return

    logger.info(f"Gemini analizuje {len(setups)} nowych setupów...")

    for symbol, m_type in setups:
        df_context = get_market_context(symbol)
        if df_context.empty: continue

        # --- NOWOŚĆ: Pobieranie dynamicznego RR z pliku konfiguracyjnego ---
        config = load_market_config(m_type)
        min_rr = config.get('risk_management', {}).get('min_rr_ratio', 2.0)

        last_row = df_context.iloc[-1]
        indicators = last_row['indicators']
        if isinstance(indicators, str): indicators = json.loads(indicators)

        price = float(last_row['price'])
        vwap = float(indicators.get('vwap', 0))
        direction = "LONG" if price > vwap else "SHORT"

        sl_key = 'sl_hybrid_long' if direction == "LONG" else 'sl_hybrid_short'
        tp_key = 'tp_hybrid_long' if direction == "LONG" else 'tp_hybrid_short'

        sl_val = float(indicators.get(sl_key, 0))
        tp_val = float(indicators.get(tp_key, 0))

        rr_ratio = calculate_potential_rr(price, sl_val, tp_val, direction)

        # --- ZMIANA: Porównanie z dynamicznym min_rr ---
        if rr_ratio < min_rr:
            logger.info(f"Odrzucenie automatyczne (RR {rr_ratio:.2f} < wymagane {min_rr}) dla {symbol} - oszczędność API Gemini.")
            with engine.connect() as conn:
                query = text("""
                    INSERT INTO signals_gemini (symbol, entry_price, sl, tp, ai_verdict, status, direction, market_type, created_at)
                    VALUES (:sym, :en, :sl, :tp, :verdict, :status, :dir, :mt, NOW())
                """)
                conn.execute(query, {
                    "sym": symbol, "en": price, "sl": sl_val, "tp": tp_val,
                    "verdict": f"Python Filter: Odrzucono ze względu na niskie RR ({rr_ratio:.2f} < {min_rr})",
                    "status": "REJECTED", "dir": direction, "mt": m_type
                })
                conn.commit()
            continue # Przejdź dalej bez zapytania do API
        # --------------------------------------------------------

        logger.info(f"RR poprawne ({rr_ratio:.2f} >= {min_rr}). Pytam Gemini o {symbol}...")

        # Przekazujemy zmienną min_rr do funkcji AI
        ai_verdict = await ask_gemini_with_backoff(symbol, df_context, direction, sl_val, tp_val, min_rr)

        if not ai_verdict: continue

        # Zapis do tabeli signals_gemini
        with engine.connect() as conn:
            query = text("""
                INSERT INTO signals_gemini (symbol, entry_price, sl, tp, ai_verdict, status, direction, market_type, created_at)
                VALUES (:sym, :en, :sl, :tp, :verdict, :status, :dir, :mt, NOW())
            """)
            conn.execute(query, {
                "sym": symbol, "en": ai_verdict.get('entry', price),
                "sl": ai_verdict.get('sl', sl_val), "tp": ai_verdict.get('tp', tp_val),
                "verdict": ai_verdict.get('reasoning', ''),
                "status": "OPEN" if ai_verdict.get('status') == 'APPROVED' else "REJECTED",
                "dir": ai_verdict.get('direction', direction),
                "mt": m_type
            })
            conn.commit()

        if ai_verdict.get('status') == 'APPROVED':
            dir_icon = "🚀" if ai_verdict['direction'] == "LONG" else "📉"
            dir_text = "WZROST (LONG)" if ai_verdict['direction'] == "LONG" else "SPADEK (SHORT)"

            escaped_reasoning = html.escape(ai_verdict['reasoning'])
            msg = (
                f"♊ <b>GEMINI PRO ANALYST: {symbol}</b>\n\n"
                f"🎯 <b>Kierunek:</b> <u>{dir_text}</u> {dir_icon}\n"
                f"💰 <b>Wejście:</b> <code>{ai_verdict['entry']}</code>\n"
                f"🛑 <b>Stop Loss:</b> <code>{ai_verdict['sl']}</code>\n"
                f"✅ <b>Take Profit:</b> <code>{ai_verdict['tp']}</code>\n"
                f"📊 <b>Risk/Reward:</b> <code>1:{rr_ratio:.2f}</code> <i>(Min: {min_rr})</i>\n\n"
                f"📝 <b>Analiza:</b>\n<i>{escaped_reasoning}</i>"
            )
            await send_tg_topic(msg)
            logger.info(f"Gemini APPROVED {symbol} (RR: 1:{rr_ratio:.2f})")
            await asyncio.sleep(2) # Anty-spam

if __name__ == "__main__":
    asyncio.run(main())