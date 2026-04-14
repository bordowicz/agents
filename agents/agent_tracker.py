import os
import pandas as pd
from sqlalchemy import create_engine, text
import telegram
import asyncio
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("Trade_Tracker")

DB_URL = os.getenv("DATABASE_URL")
TG_TOKEN = os.getenv("TELEGRAM_SIGNAL_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
engine = create_engine(DB_URL)

async def send_tg(message):
    if not TG_TOKEN or not CHAT_ID: return
    try:
        bot = telegram.Bot(token=TG_TOKEN)
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Telegram Error: {e}")

async def check_open_trades():
    with engine.connect() as conn:
        # Pobieramy wszystkie otwarte sygnały, które zostały zatwierdzone przez AI
        query = text("SELECT id, symbol, created_at, entry_price, sl, tp FROM signals WHERE status = 'OPEN'")
        open_signals = conn.execute(query).fetchall()

        if not open_signals:
            logger.info("Brak otwartych pozycji do rozliczenia.")
            return

        for sig in open_signals:
            sig_id, symbol, created_at, entry, sl, tp = sig

            # Pobieramy ceny dla tego symbolu OD MOMENTU wystawienia sygnału
            price_query = text("""
                SELECT MIN(price) as min_p, MAX(price) as max_p
                FROM price_history
                WHERE symbol = :sym AND time >= :start_time
            """)
            prices = conn.execute(price_query, {"sym": symbol, "start_time": created_at}).fetchone()

            if not prices or prices[0] is None:
                continue

            min_price, max_price = prices[0], prices[1]
            new_status = None
            msg = ""

            # Logika rozliczania (zakładamy pozycje LONG)
            if min_price <= sl:
                new_status = 'HIT_SL'
                msg = f"🔴 *STOP LOSS ZALICZONY*: {symbol}\nCena spadła do `{min_price}`. Strata zaksięgowana."
            elif max_price >= tp:
                new_status = 'HIT_TP'
                msg = f"🟢 *TAKE PROFIT ZALICZONY*: {symbol}\nCena wzrosła do `{max_price}`. Zysk zaksięgowany!"

            # Aktualizacja bazy, jeśli status uległ zmianie
            if new_status:
                update_query = text("UPDATE signals SET status = :stat WHERE id = :id")
                conn.execute(update_query, {"stat": new_status, "id": sig_id})
                conn.commit()
                await send_tg(msg)
                logger.info(f"Zaktualizowano sygnał {sig_id} ({symbol}) na {new_status}")

    engine.dispose()

if __name__ == "__main__":
    asyncio.run(check_open_trades())
