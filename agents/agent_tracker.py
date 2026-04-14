import os
import pandas as pd
from sqlalchemy import create_engine, text
import telegram
import asyncio
import logging
from decimal import Decimal

# Konfiguracja Logów
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("Trade_Tracker")

# ENV
DB_URL: str = os.getenv("DATABASE_URL", "")
TG_TOKEN: str = os.getenv("TELEGRAM_SIGNAL_TOKEN", "")
CHAT_ID: str = os.getenv("CHAT_ID", "")

engine = create_engine(DB_URL)

async def send_tg(message: str) -> None:
    if not TG_TOKEN or not CHAT_ID: return
    try:
        bot = telegram.Bot(token=TG_TOKEN)
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Telegram Error: {e}")

async def check_open_trades() -> None:
    """Sprawdza otwarte pozycje i rozlicza je na podstawie historii cen w czasie."""
    with engine.connect() as conn:
        # Pobieramy wszystkie otwarte sygnały (status 'OPEN')
        query = text("SELECT id, symbol, created_at, entry_price, sl, tp, direction FROM signals WHERE status = 'OPEN'")
        open_signals = conn.execute(query).fetchall()

        if not open_signals:
            logger.info("Brak otwartych pozycji do rozliczenia.")
            return

        for sig in open_signals:
            sig_id, symbol, created_at, entry, sl, tp, direction = sig

            # Konwersja na Decimal dla precyzji porównań
            sl_dec: Decimal = Decimal(str(sl))
            tp_dec: Decimal = Decimal(str(tp))

            # Pobieramy historię cen OD MOMENTU wystawienia sygnału, posortowaną chronologicznie
            price_query = text("""
                SELECT time, price
                FROM price_history
                WHERE symbol = :sym AND time >= :start_time
                ORDER BY time ASC
            """)
            price_history = conn.execute(price_query, {"sym": symbol, "start_time": created_at}).fetchall()

            if not price_history:
                continue

            new_status = None
            hit_price: Decimal = Decimal('0')
            hit_time = None

            for record in price_history:
                current_p: Decimal = Decimal(str(record.price))

                if direction == 'LONG':
                    if current_p <= sl_dec:
                        new_status = 'HIT_SL'
                        hit_price = current_p
                        hit_time = record.time
                        break
                    if current_p >= tp_dec:
                        new_status = 'HIT_TP'
                        hit_price = current_p
                        hit_time = record.time
                        break
                elif direction == 'SHORT':
                    if current_p >= sl_dec: # W shorcie SL jest powyżej wejścia
                        new_status = 'HIT_SL'
                        hit_price = current_p
                        hit_time = record.time
                        break
                    if current_p <= tp_dec: # W shorcie TP jest poniżej wejścia
                        new_status = 'HIT_TP'
                        hit_price = current_p
                        hit_time = record.time
                        break

            # Aktualizacja bazy, jeśli status uległ zmianie
            if new_status:
                try:
                    update_query = text("""
                        UPDATE signals
                        SET status = :stat,
                            closed_at = :now,
                            exit_price = :ex_p
                        WHERE id = :id
                    """)
                    conn.execute(update_query, {
                        "stat": new_status,
                        "now": hit_time,
                        "ex_p": float(hit_price),
                        "id": sig_id
                    })
                    conn.commit()

                    icon = "🟢" if new_status == 'HIT_TP' else "🔴"
                    action_text = "<b>ZYSK ZAKSIĘGOWANY</b> 💰" if new_status == 'HIT_TP' else "<b>STRATA ZAKSIĘGOWANA</b> 🛑"
                    dir_text = "WZROST (LONG)" if direction == "LONG" else "SPADEK (SHORT)"

                    msg = (
                        f"{icon} <b>POZYCJA ZAKOŃCZONA: {symbol}</b>\n\n"
                        f"📊 <b>Kierunek:</b> <u>{dir_text}</u>\n"
                        f"🏁 <b>Status:</b> {action_text}\n"
                        f"💰 <b>Wejście:</b> <code>{entry}</code>\n"
                        f"🏁 <b>Wyjście:</b> <code>{float(hit_price)}</code>\n\n"
                        f"⏲️ <b>Czas zamknięcia:</b> <i>{hit_time.strftime('%H:%M:%S') if hit_time else 'N/A'}</i>"
                    )
                    await send_tg(msg)
                    logger.info(f"Zaktualizowano sygnał {sig_id} ({symbol}) na {new_status}")
                except Exception as e:
                    logger.error(f"Błąd podczas aktualizacji statusu sygnału {sig_id}: {e}")
                    conn.rollback()

    engine.dispose()

if __name__ == "__main__":
    asyncio.run(check_open_trades())
