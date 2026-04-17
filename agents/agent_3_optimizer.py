import os
import json
import asyncio
import logging
import requests
import html
import re
from sqlalchemy import create_engine, text
from anthropic import Anthropic
from typing import Dict, Any, Tuple

# Konfiguracja Logów
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("Optimizer_Agent_3")

# ENV
DB_URL: str = os.getenv("DATABASE_URL", "")
TG_TOKEN: str = os.getenv("TELEGRAM_SIGNAL_TOKEN", "")
CHAT_ID: str = os.getenv("GROUP_CHAT_ID", "")
OPTIMIZER_THREAD_ID: str = os.getenv("OPTIMIZER_THREAD_ID", "8")
ANTHROPIC_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")

engine = create_engine(DB_URL)
client = Anthropic(api_key=ANTHROPIC_KEY)

BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

async def send_tg_topic(message: str) -> None:
    if not TG_TOKEN or not CHAT_ID: return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "message_thread_id": int(OPTIMIZER_THREAD_ID)
    }
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: requests.post(url, json=payload, timeout=10))
    except Exception as e:
        logger.error(f"Telegram Error: {e}")

def get_performance_stats(table_name: str) -> Tuple[Dict[str, int], float, int]:
    """Pobiera statystyki zyskowności dla danej tabeli sygnałów."""
    try:
        with engine.connect() as conn:
            query = text(f"SELECT status, COUNT(*) as count FROM {table_name} WHERE status IN ('HIT_TP', 'HIT_SL') GROUP BY status")
            results = conn.execute(query).fetchall()

        stats = {'HIT_TP': 0, 'HIT_SL': 0}
        for row in results:
            stats[row[0]] = row[1]

        total = stats['HIT_TP'] + stats['HIT_SL']
        winrate = (stats['HIT_TP'] / total * 100) if total > 0 else 0
        return stats, winrate, total
    except Exception as e:
        logger.error(f"Błąd pobierania statystyk z {table_name}: {e}")
        return {'HIT_TP': 0, 'HIT_SL': 0}, 0.0, 0

def optimize_with_ai(current_config: Dict[str, Any], stats_combined: str, market_name: str) -> Dict[str, Any] | None:
    system_prompt = f"""You are a Senior Quantitative Trading Engineer.
    Analyze the performance of our AI models for the {market_name} market based on the provided stats.

    Your task is to modify the technical parameters in the JSON configuration to increase future profitability.
    Crucially, you have control over the "risk_management" section.
    - If Winrate is high but trade volume is low, you may LOWER "min_rr_ratio" (e.g., from 2.0 to 1.8) to capture more trades.
    - If Winrate is poor, you may INCREASE "min_rr_ratio" (e.g., to 2.2) to demand better setups, or tighten technical filters (volume multipliers, RSI periods).

    You MUST respond strictly with a JSON object in the following format:
    {{
        "rationale": "Explain clearly in 3-4 sentences what parameters (especially RR) you changed and why.",
        "new_config": {{ ... the complete updated configuration ... }}
    }}"""

    user_prompt = f"""
    Performance Summary for {market_name}:
    {stats_combined}

    Current Strategy Config:
    {json.dumps(current_config, indent=2)}
    """

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4000,
            temperature=0.2,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}]
        )
        result_text = response.content[0].text.strip()

        match = re.search(r'\{.*\}', result_text, re.DOTALL)
        if not match:
            logger.error("AI nie zwróciło poprawnego formatu JSON.")
            return None

        result_json = json.loads(match.group(0))
        new_cfg = result_json.get('new_config')
        rationale = result_json.get('rationale', 'Brak wyjaśnienia')

        if not new_cfg or 'indicators' not in new_cfg or 'risk_management' not in new_cfg:
            logger.error("AI zwróciło uszkodzoną strukturę konfiguracji. Odrzucam zmiany.")
            return None

        v = new_cfg.get('version', '3.0.0').split('.')
        v[-1] = str(int(v[-1]) + 1)
        new_cfg['version'] = ".".join(v)

        new_cfg['_ai_rationale'] = rationale
        return new_cfg

    except Exception as e:
        logger.error(f"Błąd API Claude podczas optymalizacji {market_name}: {e}")
        return None

async def main():
    if not ANTHROPIC_KEY: return

    for market_type in ['crypto', 'stocks']:
        config_file = f'params_{market_type}.json'
        config_path = os.path.join(BASE_DIR, 'config', config_file)

        if not os.path.exists(config_path): continue

        c_stats, c_win, c_tot = get_performance_stats("signals")
        g_stats, g_win, g_tot = get_performance_stats("signals_gemini")

        # Ustawione na 5 do celów testowych, produkcyjnie zalecane 20-30
        if (c_tot + g_tot) < 5:
            logger.info(f"[{market_type.upper()}] Zbyt mało zamkniętych transakcji. Wymagane 5.")
            continue

        stats_summary = (
            f"CLAUDE PRO: Winrate {c_win:.1f}% ({c_stats['HIT_TP']}W / {c_stats['HIT_SL']}L)\n"
            f"GEMINI FREE: Winrate {g_win:.1f}% ({g_stats['HIT_TP']}W / {g_stats['HIT_SL']}L)"
        )

        with open(config_path, 'r') as f:
            current_config = json.load(f)

        logger.info(f"[{market_type.upper()}] Zlecam analizę statystyk do AI...")
        new_config = optimize_with_ai(current_config, stats_summary, market_type.upper())

        if new_config:
            ai_rationale = new_config.pop('_ai_rationale', 'Parametry zostały zaktualizowane.')

            # ZAPIS DO PLIKU
            try:
                with open(config_path, 'w') as f:
                    json.dump(new_config, f, indent=4)
            except Exception as e:
                logger.error(f"Błąd zapisu pliku: {e}")
                continue

            # NOWOŚĆ: ZAPIS DO BAZY DANYCH (Wersjonowanie)
            try:
                with engine.connect() as conn:
                    query = text("""
                        INSERT INTO strategy_versions (market_type, version, parameters, ai_rationale)
                        VALUES (:mt, :ver, :params, :rat)
                    """)
                    conn.execute(query, {
                        "mt": market_type,
                        "ver": new_config['version'],
                        "params": json.dumps(new_config),
                        "rat": ai_rationale
                    })
                    conn.commit()
                logger.info(f"Zapisano wersję {new_config['version']} w bazie danych.")
            except Exception as e:
                logger.error(f"Błąd zapisu historii do bazy: {e}")

            winner = "CLAUDE PRO" if c_win > g_win else "GEMINI FREE"
            if c_win == g_win: winner = "REMIS"

            # Sprawdzanie różnicy RR (wizualny bajer na Telegram)
            old_rr = current_config.get('risk_management', {}).get('min_rr_ratio', 'N/A')
            new_rr = new_config.get('risk_management', {}).get('min_rr_ratio', 'N/A')
            rr_change_text = f"⚖️ <b>Zmiana RR:</b> <code>{old_rr}</code> ➡️ <code>{new_rr}</code>" if old_rr != new_rr else f"⚖️ <b>RR bez zmian:</b> <code>{new_rr}</code>"

            escaped_winner = html.escape(winner)
            escaped_rationale = html.escape(ai_rationale)

            msg = (
                f"⚙️ <b>OPTYMALIZACJA STRATEGII: {market_type.upper()}</b>\n\n"
                f"📊 <b>Wyniki (Ostatni cykl):</b>\n"
                f"• 🤖 Claude: <code>{c_win:.1f}%</code> | ♊ Gemini: <code>{g_win:.1f}%</code>\n\n"
                f"🚀 <b>Nowa wersja:</b> <code>{new_config.get('version')}</code>\n"
                f"{rr_change_text}\n\n"
                f"🧠 <b>Uzasadnienie AI:</b>\n"
                f"<i>{escaped_rationale}</i>"
            )
            await send_tg_topic(msg)

    engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())