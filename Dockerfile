# Zmieniamy bazę na 3.12-slim, aby spełnić wymagania pandas-ta
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

# 3. Instalacja bibliotek (pandas zostawiamy w wersji < 3.0.0 dla stabilności pandas-ta)
RUN pip install --no-cache-dir \
    "pandas<3.0.0" \
    ccxt \
    yfinance \
    sqlalchemy \
    psycopg2-binary \
    python-telegram-bot \
    anthropic

# 4. Instalacja pandas-ta - teraz Python 3.12 pozwoli na instalację najnowszych wersji
RUN pip install --no-cache-dir pandas-ta

CMD ["tail", "-f", "/dev/null"]
