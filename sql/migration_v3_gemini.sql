-- Tabela dla sygnałów wygenerowanych przez darmowy model Gemini
CREATE TABLE IF NOT EXISTS signals_gemini (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    market_type VARCHAR(10) NOT NULL, -- 'crypto' lub 'stocks'
    direction VARCHAR(10) NOT NULL,   -- 'LONG' lub 'SHORT'
    entry_price DECIMAL NOT NULL,
    sl DECIMAL NOT NULL,
    tp DECIMAL NOT NULL,
    ai_verdict TEXT,
    status VARCHAR(20) DEFAULT 'OPEN', -- 'OPEN', 'CLOSED', 'REJECTED'
    pnl_percent DECIMAL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    closed_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_gemini_symbol ON signals_gemini(symbol);
CREATE INDEX idx_gemini_status ON signals_gemini(status);
