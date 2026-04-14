-- MIGRACJA V1: POPRAWA PRECYZJI I ROZBUDOWA TRACKINGU

-- 1. Zmiana typów w tabeli price_history na NUMERIC dla zachowania pełnej precyzji (rekomendacja 1)
ALTER TABLE price_history
    ALTER COLUMN price TYPE NUMERIC(24, 10),
    ALTER COLUMN volume TYPE NUMERIC(32, 10);

-- 2. Zmiana typów w tabeli signals
ALTER TABLE signals
    ALTER COLUMN entry_price TYPE NUMERIC(24, 10),
    ALTER COLUMN sl TYPE NUMERIC(24, 10),
    ALTER COLUMN tp TYPE NUMERIC(24, 10);

-- 3. Dodanie brakujących kolumn do tabeli signals dla Agenta Trackera i Optymalizatora
ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS exit_price NUMERIC(24, 10),
    ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS market_type TEXT; -- 'crypto' lub 'stocks'

-- 4. Opcjonalne: Dodanie indeksu dla szybkości zapytań trackera
CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status);
CREATE INDEX IF NOT EXISTS idx_price_history_sym_time ON price_history(symbol, time DESC);
