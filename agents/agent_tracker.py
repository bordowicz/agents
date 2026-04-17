import os
import requests
from sqlalchemy import create_engine, text
import asyncio
import logging
from decimal import Decimal
from datetime import datetime

# Konfiguracja Logów
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("Trade_Tracker")

# ENV
DB_URL: str = os.getenv("DATABASE_URL", "")
TG_TOKEN: str = os.getenv("TELEGRAM_SIGNAL_TOKEN", "")
CHAT_ID: str = os.getenv("GROUP_CHAT_ID", "")
CLAUDE_THREAD_ID: str = "22" # Topic dla sygnałów z Claude
GEMINI_THREAD_ID: str = "7"  # Topic dla sygnałów z Gemini

engine = create_engine(DB_URL)

async def send_tg_topic(message: str, thread_id: str) -> None:
    if not TG_TOKEN or not CHAT_ID or not thread_id: return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "message_thread_id": int(thread_id),
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: requests.post(url, json=payload, timeout=10))
    except Exception as e:
        logger.error(f"Telegram Error: {e}")

async def process_table(table_name: str, thread_id: str) -> None:
    """Rozlicza pozycje dla danej tabeli."""
    logger.info(f"--- Start skanowania tabeli: {table_name} ---")

    with engine.connect() as conn:
        query = text(f"SELECT id, symbol, created_at, entry_price, sl, tp, direction FROM {table_name} WHERE status = 'OPEN'")
        open_signals = conn.execute(query).fetchall()

        if not open_signals:
            logger.info(f"[{table_name}] Brak otwartych pozycji. Czekam na nowe sygnały.")
            return

        logger.info(f"[{table_name}] Aktywne pozycje do monitorowania: {len(open_signals)}")
        closed_count = 0

        for sig in open_signals:
            sig_id, symbol, created_at, entry, sl, tp, direction = sig
            sl_dec: Decimal = Decimal(str(sl))
            tp_dec: Decimal = Decimal(str(tp))

            price_query = text("""
                SELECT time, price FROM price_history
                WHERE symbol = :sym AND time >= :start_time
                ORDER BY time ASC
            """)
            price_history = conn.execute(price_query, {"sym": symbol, "start_time": created_at}).fetchall()

            if not price_history:
                continue

            new_status = None
            hit_price = Decimal('0')
            hit_time = None

            for record in price_history:
                current_p: Decimal = Decimal(str(record.price))
                if direction == 'LONG':
                    if current_p <= sl_dec:
                        new_status, hit_price, hit_time = 'HIT_SL', current_p, record.time
                        break
                    if current_p >= tp_dec:
                        new_status, hit_price, hit_time = 'HIT_TP', current_p, record.time
                        break
                elif direction == 'SHORT':
                    if current_p >= sl_dec:
                        new_status, hit_price, hit_time = 'HIT_SL', current_p, record.time
                        break
                    if current_p <= tp_dec:
                        new_status, hit_price, hit_time = 'HIT_TP', current_p, record.time
                        break

            if new_status:
                logger.info(f"[{table_name}] 🎯 AKCJA: Zamykam {symbol} -> {new_status} po cenie {hit_price}")

                update_query = text(f"UPDATE {table_name} SET status = :stat, closed_at = :now, exit_price = :ex_p WHERE id = :id")
                conn.execute(update_query, {"stat": new_status, "now": hit_time, "ex_p": float(hit_price), "id": sig_id})
                conn.commit()
                closed_count += 1

                source_name = "GEMINI (FREE)" if "gemini" in table_name else "CLAUDE (PRO)"
                icon = "🟢" if new_status == 'HIT_TP' else "🔴"

                # Obliczanie zysku/straty procentowej do podsumowania
                entry_f = float(entry)
                exit_f = float(hit_price)
                if entry_f > 0:
                    pct_change = ((exit_f - entry_f) / entry_f) * 100
                    if direction == 'SHORT':
                        pct_change = -pct_change
                else:
                    pct_change = 0.0

                msg = (
                    f"{icon} <b>ZAMKNIĘCIE POZYCJI ({source_name}): {symbol}</b>\n\n"
                    f"🏁 <b>Status:</b> {'ZYSK 💰' if new_status == 'HIT_TP' else 'STRATA 🛑'}\n"
                    f"📈 <b>Wynik:</b> <code>{pct_change:+.2f}%</code>\n"
                    f"💰 <b>Wejście:</b> <code>{entry}</code>\n"
                    f"🏁 <b>Wyjście:</b> <code>{hit_price}</code>\n"
                )
                await send_tg_topic(msg, thread_id)

        logger.info(f"[{table_name}] Cykl zakończony. Zamknięto {closed_count} z {len(open_signals)} pozycji.")

async def main():
    logger.info("Uruchamianie Agenta 2.5 (Tracker)...")
    if CLAUDE_THREAD_ID:
        await process_table("signals", CLAUDE_THREAD_ID)
    if GEMINI_THREAD_ID:
        await process_table("signals_gemini", GEMINI_THREAD_ID)
    logger.info("Zakończono działanie Agenta 2.5.")

if __name__ == "__main__":
    asyncio.run(main())