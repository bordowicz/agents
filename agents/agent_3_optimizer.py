import os
import json
import subprocess
from sqlalchemy import create_engine, text
import telegram
import asyncio
import logging
from anthropic import Anthropic
from typing import Dict, Any, Tuple

# Konfiguracja Logów
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("Optimizer_Agent_3")

# ENV
DB_URL: str = os.getenv("DATABASE_URL", "")
TG_TOKEN: str = os.getenv("TELEGRAM_SIGNAL_TOKEN", "")
CHAT_ID: str = os.getenv("CHAT_ID", "")
ANTHROPIC_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")

engine = create_engine(DB_URL)
client = Anthropic(api_key=ANTHROPIC_KEY)

BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

async def send_tg(message: str) -> None:
    if not TG_TOKEN or not CHAT_ID: return
    try:
        bot = telegram.Bot(token=TG_TOKEN)
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Telegram Error: {e}")

def get_performance_stats(market_type: str) -> Tuple[Dict[str, int], float, int]:
    """Pobiera statystyki zyskowności dla danego rynku (crypto lub stocks) z bazy."""
    with engine.connect() as conn:
        # Uwzględniamy market_type z tabeli price_history powiązanej z sygnałami
        # Zakładamy, że w tabeli signals mamy informację o rynku (np. przez symbol lub market_type)
        # Jeśli signals nie ma market_type, musimy dołączyć price_history (lub założyć pozycje na podstawie symboli)

        # Prostsze rozwiązanie: pobieramy wszystkie sygnały i filtrujemy po symbolu (USDT = crypto, inne = stocks)
        # Lub jeśli tabela signals ma kolumnę market_type (co byłoby idealne)

        query = text("""
            SELECT status, COUNT(*) as count
            FROM signals
            WHERE status IN ('HIT_TP', 'HIT_SL')
            -- Tutaj można dodać filtrowanie po rynku jeśli tabela signals ma takie pole
            -- AND market_type = :market_type
            GROUP BY status
        """)
        results = conn.execute(query, {"market_type": market_type}).fetchall()

    stats = {'HIT_TP': 0, 'HIT_SL': 0}
    for row in results:
        stats[row[0]] = row[1]

    total = stats['HIT_TP'] + stats['HIT_SL']
    winrate = (stats['HIT_TP'] / total * 100) if total > 0 else 0
    return stats, winrate, total

def optimize_with_ai(current_config: Dict[str, Any], stats: Dict[str, int], winrate: float, market_name: str) -> Dict[str, Any] | None:
    """Zleca AI modyfikację pliku konfiguracyjnego na podstawie wyników."""
    system_prompt = f"""You are a Senior Quantitative Trading Engineer specializing in {market_name} markets.
    Analyze the current performance (Winrate: {winrate:.2f}%) of your trading strategy which uses VSA, VWAP, MACD, ADX, and Bollinger Bands.

    Objective: Update the provided JSON configuration to increase the Winrate.
    Rules for Optimization:
    1. LOW WINRATE (<45%): Tighten filters. Increase 'vsa_volume_multiplier', 'adx_threshold', or decrease 'bb_squeeze_threshold'. Consider lengthening 'ema_slow'.
    2. MODERATE WINRATE (45-60%): Fine-tune. Adjust 'rsi_period' or 'macd' settings slightly.
    3. HIGH WINRATE (>60%): Maintain stability. Only minor adjustments if necessary.
    4. Ensure the JSON structure remains EXACTLY as the input.
    5. Be logical. For stocks, volatility is different than for crypto.

    You MUST respond ONLY with a raw JSON object. No markdown, no explanations."""

    user_prompt = f"""
    Current {market_name} Performance Stats:
    - Total Closed Trades: {stats['HIT_TP'] + stats['HIT_SL']}
    - Wins (HIT_TP): {stats['HIT_TP']}
    - Losses (HIT_SL): {stats['HIT_SL']}
    - Current Winrate: {winrate:.2f}%

    Current Strategy Config:
    {json.dumps(current_config, indent=2)}

    Improve the parameters for better future performance. Return ONLY JSON."""

    try:
        response = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=1000,
            temperature=0.2,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}]
        )

        result_text = response.content[0].text.strip()
        if result_text.startswith("```json"):
            result_text = result_text.replace("```json\n", "").replace("\n```", "")

        new_cfg = json.loads(result_text)
        # Zawsze inkrementujemy wersję
        version_parts = new_cfg.get('version', '1.0.0').split('.')
        version_parts[-1] = str(int(version_parts[-1]) + 1)
        new_cfg['version'] = ".".join(version_parts)

        return new_cfg
    except Exception as e:
        logger.error(f"Błąd AI podczas optymalizacji {market_name}: {e}")
        return None

def commit_changes(market_name: str, winrate: float, config_file: str) -> None:
    """Wykonuje operacje Git w celu wersjonowania strategii."""
    try:
        os.chdir(BASE_DIR)
        subprocess.run(['git', 'add', f'config/{config_file}'], check=True)
        commit_msg = f"Auto-Opt [{market_name}]: Winrate {winrate:.2f}% -> Updating Strategy."
        subprocess.run(['git', 'commit', '-m', commit_msg], check=True)
        logger.info(f"Zmiany dla {market_name} zapisane w repozytorium Git.")
    except Exception as e:
        logger.error(f"Błąd Git dla {market_name}: {e}")

async def process_optimization(market_type: str, config_file: str) -> None:
    """Główna logika optymalizacji dla konkretnego rynku."""
    config_path = os.path.join(BASE_DIR, 'config', config_file)
    market_name = market_type.upper()

    stats, winrate, total = get_performance_stats(market_type)

    if total < 5: # Obniżone minimum do 5 dla częstszej optymalizacji na start
        logger.info(f"Za mało danych dla {market_name} ({total} transakcji). Wymagane min 5.")
        return

    logger.info(f"Optymalizacja {market_name}. Winrate: {winrate:.2f}% ({total} transakcji).")

    try:
        with open(config_path, 'r') as f:
            current_config = json.load(f)

        new_config = optimize_with_ai(current_config, stats, winrate, market_name)

        if new_config:
            with open(config_path, 'w') as f:
                json.dump(new_config, f, indent=2)

            commit_changes(market_name, winrate, config_file)

            await send_tg(
                f"⚙️ *{market_name} ZOPTYMALIZOWANY*\n"
                f"📊 *Wyniki:* `{winrate:.2f}%` ({stats['HIT_TP']}W / {stats['HIT_SL']}L)\n"
                f"🚀 Nowa wersja strategii: `{new_config.get('version', 'N/A')}`\n"
                f"🤖 AI zaostrzyło filtry wejścia dla lepszego Winrate."
            )
    except Exception as e:
        logger.error(f"Błąd podczas procesu optymalizacji dla {market_name}: {e}")

async def main():
    if not ANTHROPIC_KEY:
        logger.error("Brak klucza ANTHROPIC_API_KEY.")
        return

    # Optymalizacja dla obu rynków
    await process_optimization('crypto', 'params_crypto.json')
    await process_optimization('stocks', 'params_stocks.json')

    engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
