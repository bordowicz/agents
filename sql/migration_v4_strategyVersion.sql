CREATE TABLE IF NOT EXISTS strategy_versions (
                                                 id SERIAL PRIMARY KEY,
                                                 market_type TEXT,
                                                 version TEXT,
                                                 parameters JSONB,
                                                 ai_rationale TEXT,
                                                 created_at TIMESTAMPTZ DEFAULT NOW()
    );