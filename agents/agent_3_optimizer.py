import os
import json
import subprocess
from sqlalchemy import create_engine, text
import telegram
import asyncio
import logging
from anthropic import Anthropic

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("Optimizer_Agent_3")

DB_URL = os.getenv("DATABASE_URL")
TG_TOKEN = os.getenv("TELEGRAM_SIGNAL_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")

engine = create_engine(DB_URL)
client = Anthropic(api_key=ANTHROPIC_KEY)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'params_crypto.json')

async def send_tg(message):
    if not TG_TOKEN or not CHAT_ID: return
    try:
        bot = telegram.Bot(token=TG_TOKEN)
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Telegram Error: {e}")

def get_performance_stats():
    """Pobiera statystyki zyskowności z bazy."""
    with engine.connect() as conn:
        query = text("""
            SELECT status, COUNT(*) as count
            FROM signals
            WHERE status IN ('HIT_TP', 'HIT_SL')
            GROUP BY status
        """)
        results = conn.execute(query).fetchall()

    stats = {'HIT_TP': 0, 'HIT_SL': 0}
    for row in results:
        stats[row[0]] = row[1]

    total = stats['HIT_TP'] + stats['HIT_SL']
    winrate = (stats['HIT_TP'] / total * 100) if total > 0 else 0
    return stats, winrate, total

def optimize_with_ai(current_config, stats, winrate):
    """Zleca AI modyfikację pliku konfiguracyjnego."""
    system_prompt = """You are a Senior Algorithmic Trading Engineer.
    Your system is using a config JSON to find trades based on VSA, VWAP, and Bollinger Bands.
    Your task is to analyze the current performance and return an updated JSON configuration to improve the winrate.
    Rules:
    1. If winrate is low (<50%), tighten the filters (e.g., increase vsa_volume_multiplier, lower bb_squeeze_threshold).
    2. Respond ONLY with a valid JSON matching the exact structure of the input JSON. No markdown, no explanations outside the JSON."""

    user_prompt = f"""
    Current Performance:
    - Total Closed Trades: {stats['HIT_TP'] + stats['HIT_SL']}
    - Wins (HIT_TP): {stats['HIT_TP']}
    - Losses (HIT_SL): {stats['HIT_SL']}
    - Winrate: {winrate:.2f}%

    Current Config:
    {json.dumps(current_config, indent=2)}

    Update the parameters logically to improve performance. Return ONLY JSON.
    """

    try:
        response = client.messages.create(
            model="claude-3-5-sonnet-20240620",
            max_tokens=500,
            temperature=0.3,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}]
        )

        result_text = response.content[0].text.strip()
        if result_text.startswith("```json"):
            result_text = result_text.replace("```json\n", "").replace("\n```", "")
        return json.loads(result_text)
    except Exception as e:
        logger.error(f"Błąd AI podczas optymalizacji: {e}")
        return None

def commit_changes(winrate):
    """Wykonuje komendy Git w celu zapisania nowej strategii."""
    try:
        # Przechodzimy do folderu workspace
        os.chdir(BASE_DIR)
        subprocess.run(['git', 'add', 'config/params_crypto.json'], check=True)
        commit_msg = f"Auto-Optimization: Updated strategy params. Previous Winrate: {winrate:.2f}%"
        subprocess.run(['git', 'commit', '-m', commit_msg], check=True)
        logger.info("Zmiany zapisane w repozytorium Git.")
    except Exception as e:
        logger.error(f"Błąd podczas operacji Git: {e}")

async def main():
    stats, winrate, total = get_performance_stats()

    if total < 10:
        logger.info(f"Za mało danych do optymalizacji ({total} transakcji). Wymagane minimum 10.")
        return

    logger.info(f"Rozpoczynam optymalizację. Obecny Winrate: {winrate:.2f}%")

    with open(CONFIG_PATH, 'r') as f:
        current_config = json.load(f)

    new_config = optimize_with_ai(current_config, stats, winrate)

    if new_config:
        # Zapisujemy nową konfigurację
        with open(CONFIG_PATH, 'w') as f:
            json.dump(new_config, f, indent=2)

        commit_changes(winrate)

        await send_tg(
            f"⚙️ *SYSTEM ZOPTYMALIZOWANY*\n\n"
            f"📊 *Poprzednie wyniki:*\n"
            f"Winrate: `{winrate:.2f}%` ({stats['HIT_TP']}W / {stats['HIT_SL']}L)\n\n"
            f"🤖 AI zaktualizowało parametry w `params_crypto.json` i zapisało zmiany w Git."
        )

    engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
