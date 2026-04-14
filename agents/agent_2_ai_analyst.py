import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
import telegram
import asyncio
import logging
from anthropic import Anthropic

# Konfiguracja Logów
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("AI_Analyst_Agent")

# ENV
DB_URL = os.getenv("DATABASE_URL")
TG_TOKEN = os.getenv("TELEGRAM_SIGNAL_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")

engine = create_engine(DB_URL)
client = Anthropic(api_key=ANTHROPIC_KEY)

async def send_tg(message):
    if not TG_TOKEN or not CHAT_ID: return
    try:
        bot = telegram.Bot(token=TG_TOKEN)
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Telegram Error: {e}")

def get_pending_setups():
    """Znajduje symbole, które miały Ultimate Setup w ciągu ostatnich 30 min, ale nie zostały jeszcze przeanalizowane."""
    query = text("""
        WITH RecentSetups AS (
            SELECT symbol, MAX(time) as setup_time
            FROM price_history
            WHERE market_type = 'crypto'
              AND (indicators->>'vsa_signal')::boolean = true
              AND time > NOW() - INTERVAL '30 minutes'
            GROUP BY symbol
        )
        SELECT r.symbol
        FROM RecentSetups r
        LEFT JOIN signals s ON r.symbol = s.symbol AND s.created_at > NOW() - INTERVAL '4 hours'
        WHERE s.id IS NULL; -- Wykluczamy te, które już były analizowane ostatnio
    """)
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        return [row[0] for row in result]

def get_market_context(symbol):
    """Pobiera ostatnie 20 świec dla AI, by mogło ocenić strukturę ceny."""
    query = text("""
        SELECT time, price, volume, indicators
        FROM price_history
        WHERE symbol = :sym
        ORDER BY time DESC LIMIT 20
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"sym": symbol})

    # Sortujemy od najstarszej do najnowszej dla LLM
    df = df.sort_values(by='time')
    return df

def ask_claude_for_setup(symbol, df_context):
    """Wysyła dane do Claude  Sonnet i wymusza odpowiedź w JSON."""

    # Formatujemy dane do czytelnego stringa dla AI
    context_str = ""
    for _, row in df_context.iterrows():
        ind = row['indicators'] # dict, bo wczytane przez pandas/sqlalchemy z JSONB
        if isinstance(ind, str): ind = json.loads(ind) # Zabezpieczenie

        context_str += f"Cena: {row['price']}, Vol: {row['volume']}, VWAP: {ind.get('vwap', 0):.4f}, RSI: {ind.get('rsi', 0)}, BB_Squeeze: {ind.get('is_squeeze', False)}\n"

    system_prompt = """You are an elite Quant Trader and Risk Manager.
    Analyze the provided 5-minute interval crypto data (last 20 periods) for an asset that just triggered a Volume Spread Analysis (VSA) anomaly during a Bollinger Squeeze, above VWAP.

    Your task:
    1. Determine if this is a valid LONG entry. (If false breakout, return status 'REJECTED').
    2. If valid, provide exact prices for: Entry, Stop Loss (SL), and Take Profit (TP).
    3. Ensure Risk/Reward ratio is at least 1:2. SL must be placed below recent structure or VWAP.

    You MUST respond ONLY with a raw JSON object in the exact format below. No markdown formatting, no explanations outside JSON.
    {
      "status": "APPROVED" | "REJECTED",
      "entry": float,
      "sl": float,
      "tp": float,
      "reasoning": "string (max 3 sentences)"
    }"""

    user_prompt = f"Analyze the following recent price action for {symbol}:\n\n{context_str}"

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=300,
            temperature=0.2, # Niski temperature dla analitycznej precyzji
            system=system_prompt,
            messages=[
                {"role": "user", "content": user_prompt}
            ]
        )

        # Ekstrakcja czystego JSONa z odpowiedzi
        result_text = response.content[0].text.strip()

        # Jeśli AI doda bloki kodu pomimo zakazu, usuwamy je
        if result_text.startswith("```json"):
            result_text = result_text.replace("```json\n", "").replace("\n```", "")

        return json.loads(result_text)
    except Exception as e:
        logger.error(f"Błąd API Claude dla {symbol}: {e}")
        return None

async def main():
    if not ANTHROPIC_KEY:
        logger.error("Brak klucza ANTHROPIC_API_KEY. Zakończenie pracy.")
        return

    symbols_to_analyze = get_pending_setups()
    if not symbols_to_analyze:
        logger.info("Brak nowych sygnałów Ultimate Setup do analizy.")
        return

    logger.info(f"Znaleziono {len(symbols_to_analyze)} potencjalnych sygnałów. Rozpoczynam analizę AI...")

    for symbol in symbols_to_analyze:
        df_context = get_market_context(symbol)
        if df_context.empty: continue

        logger.info(f"Odpytuję Claude 4.6 Sonnet dla {symbol}...")
        ai_verdict = ask_claude_for_setup(symbol, df_context)

        if not ai_verdict: continue

        # Zapis do bazy danych
        with engine.connect() as conn:
            query = text("""
                INSERT INTO signals (symbol, entry_price, sl, tp, ai_verdict, status)
                VALUES (:sym, :en, :sl, :tp, :verdict, :status)
            """)
            conn.execute(query, {
                "sym": symbol,
                "en": ai_verdict.get('entry', 0.0),
                "sl": ai_verdict.get('sl', 0.0),
                "tp": ai_verdict.get('tp', 0.0),
                "verdict": ai_verdict.get('reasoning', ''),
                "status": "OPEN" if ai_verdict.get('status') == 'APPROVED' else "REJECTED"
            })
            conn.commit()

        # Powiadomienie Telegram, jeśli AI zatwierdziło sygnał
        if ai_verdict.get('status') == 'APPROVED':
            msg = (
                f"🧠 *AI TRADE APPROVED*: {symbol}\n\n"
                f"🎯 *Entry:* `{ai_verdict['entry']}`\n"
                f"🛑 *Stop Loss:* `{ai_verdict['sl']}`\n"
                f"✅ *Take Profit:* `{ai_verdict['tp']}`\n\n"
                f"📝 *AI Reasoning:*\n_{ai_verdict['reasoning']}_"
            )
            await send_tg(msg)
            logger.info(f"Wysłano zatwierdzony sygnał dla {symbol}")
        else:
            logger.info(f"AI odrzuciło sygnał dla {symbol}. Powód: {ai_verdict.get('reasoning')}")

if __name__ == "__main__":
    asyncio.run(main())
