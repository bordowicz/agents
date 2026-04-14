-- Rozszerzenie dla danych czasowych
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Tabela historii cen
CREATE TABLE price_history (
                               time TIMESTAMPTZ NOT NULL,
                               symbol TEXT NOT NULL,
                               market_type TEXT NOT NULL, -- 'crypto' lub 'stocks'
                               price DOUBLE PRECISION,
                               volume DOUBLE PRECISION,
                               indicators JSONB -- Tu Python zapisze RSI, ATR itp.
);
SELECT create_hypertable('price_history', 'time');

-- Tabela sygnałów
CREATE TABLE signals (
                         id SERIAL PRIMARY KEY,
                         created_at TIMESTAMPTZ DEFAULT NOW(),
                         symbol TEXT,
                         entry_price DOUBLE PRECISION,
                         sl DOUBLE PRECISION,
                         tp DOUBLE PRECISION,
                         ai_verdict TEXT,
                         status TEXT DEFAULT 'PENDING' -- PENDING, HIT_TP, HIT_SL
);