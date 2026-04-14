-- MIGRACJA V2: OBSŁUGA POZYCJI SHORT I LEPSZA CZYTELNOŚĆ

-- 1. Dodanie kolumny direction do tabeli signals
ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS direction TEXT DEFAULT 'LONG'; -- 'LONG' lub 'SHORT'

-- 2. Dodanie indeksu dla kolumny direction
CREATE INDEX IF NOT EXISTS idx_signals_direction ON signals(direction);

-- 3. Opcjonalne: Dodanie kolumny market_type do tabeli signals (jeśli zapomniano w poprzedniej migracji)
ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS market_type TEXT; -- 'crypto' lub 'stocks'
