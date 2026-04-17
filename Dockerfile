FROM python:3.12-slim

# 1. Instalacja zależności systemowych
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    libpq-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Aktualizacja narzędzi instalacyjnych
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# 3. Instalacja bibliotek
RUN pip install --no-cache-dir \
    "pandas<3.0.0" \
    ccxt \
    yfinance \
    sqlalchemy \
    psycopg2-binary \
    python-telegram-bot \
    anthropic \
    "google-generativeai>=0.8.3" \
    requests

# 4. Instalacja pandas-ta
RUN pip install --no-cache-dir pandas-ta

CMD ["tail", "-f", "/dev/null"]
