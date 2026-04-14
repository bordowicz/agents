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
DB_URL: str = os.getenv("DATABASE_URL", "")
TG_TOKEN: str = os.getenv("TELEGRAM_SIGNAL_TOKEN", "")
CHAT_ID: str = os.getenv("CHAT_ID", "")
ANTHROPIC_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")

engine = create_engine(DB_URL)
client = Anthropic(api_key=ANTHROPIC_KEY)

async def send_tg(message: str) -> None:
    if not TG_TOKEN or not CHAT_ID: return
    try:
        bot = telegram.Bot(token=TG_TOKEN)
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Telegram Error: {e}")

def get_pending_setups():
    """Znajduje symbole z ostatnimi setupami do analizy."""
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
        LEFT JOIN signals s ON r.symbol = s.symbol AND s.created_at > NOW() - INTERVAL '4 hours'
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

def ask_claude_for_setup(symbol: str, df_context: pd.DataFrame) -> dict | None:
    context_str = ""
    for _, row in df_context.iterrows():
        ind = row['indicators']
        if isinstance(ind, str): ind = json.loads(ind)
        context_str += (
            f"T: {row['time']}, P: {row['price']}, V: {row['volume']}, "
            f"VWAP: {ind.get('vwap', 0):.4f}, RSI: {ind.get('rsi', 0):.1f}, "
            f"MACD: {ind.get('macd', 0):.4f}, ADX: {ind.get('adx', 0):.1f}, "
            f"EMA_F/S: {ind.get('ema_fast', 0):.2f}/{ind.get('ema_slow', 0):.2f}\n"
        )

    system_prompt = """You are an elite Quant Trader. Analyze 30 periods of data for an asset with a VSA anomaly.
    Task: Validate if this is a high-probability LONG or SHORT entry.

    CRITERIA FOR LONG: Price consistently above/bouncing off VWAP, EMA Fast > Slow, MACD bullish, RSI > 45.
    CRITERIA FOR SHORT: Price consistently below VWAP, EMA Fast < Slow, MACD bearish, RSI < 55.

    If valid, provide Entry, SL, and TP. RR ratio must be at least 1:2.
    SL for LONG: Below recent swing low or VWAP.
    SL for SHORT: Above recent swing high or VWAP.

    You MUST respond ONLY with a raw JSON object:
    {
      "status": "APPROVED" | "REJECTED",
      "direction": "LONG" | "SHORT",
      "entry": float,
      "sl": float,
      "tp": float,
      "reasoning": "string (max 2 sentences, explain why LONG or SHORT based on VWAP and trend)"
    }"""

    user_prompt = f"Analyze {symbol}:\n\n{context_str}"

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=500,
            temperature=0.1,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}]
        )
        result_text = response.content[0].text.strip()
        if result_text.startswith("```json"):
            result_text = result_text.replace("```json\n", "").replace("\n```", "")
        return json.loads(result_text)
    except Exception as e:
        logger.error(f"Błąd API Claude dla {symbol}: {e}")
        return None

async def main():
    if not ANTHROPIC_KEY: return
    setups = get_pending_setups()
    if not setups: return

    for symbol, m_type in setups:
        df_context = get_market_context(symbol)
        if df_context.empty: continue

        ai_verdict = ask_claude_for_setup(symbol, df_context)
        if not ai_verdict: continue

        with engine.connect() as conn:
            query = text("""
                INSERT INTO signals (symbol, entry_price, sl, tp, ai_verdict, status, direction, market_type)
                VALUES (:sym, :en, :sl, :tp, :verdict, :status, :dir, :mt)
            """)
            conn.execute(query, {
                "sym": symbol, "en": ai_verdict.get('entry', 0.0),
                "sl": ai_verdict.get('sl', 0.0), "tp": ai_verdict.get('tp', 0.0),
                "verdict": ai_verdict.get('reasoning', ''),
                "status": "OPEN" if ai_verdict.get('status') == 'APPROVED' else "REJECTED",
                "dir": ai_verdict.get('direction', 'LONG'),
                "mt": m_type
            })
            conn.commit()

        if ai_verdict.get('status') == 'APPROVED':
            dir_icon = "🚀" if ai_verdict['direction'] == "LONG" else "📉"
            dir_text = "WZROST (LONG)" if ai_verdict['direction'] == "LONG" else "SPADEK (SHORT)"

            msg = (
                f"🧠 <b>AI ZATWIERDZA TRANSAKCJĘ: {symbol}</b>\n\n"
                f"🎯 <b>Kierunek:</b> <u>{dir_text}</u> {dir_icon}\n"
                f"💰 <b>Wejście:</b> <code>{ai_verdict['entry']}</code>\n"
                f"🛑 <b>Stop Loss:</b> <code>{ai_verdict['sl']}</code>\n"
                f"✅ <b>Take Profit:</b> <code>{ai_verdict['tp']}</code>\n\n"
                f"📝 <b>Analiza ekspercka:</b>\n<i>{ai_verdict['reasoning']}</i>"
            )
            await send_tg(msg)

if __name__ == "__main__":
    asyncio.run(main())
