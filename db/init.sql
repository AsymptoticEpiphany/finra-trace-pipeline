-- ---------------------------------------------------------------------------
-- FINRA TRACE Pipeline — Database Schema
-- ---------------------------------------------------------------------------
-- Automatically runs when the PostgreSQL container starts for the first time.
-- Creates the trades and issuer_info tables, and seeds issuer reference data.
-- ---------------------------------------------------------------------------

-- Trades table: stores ingested TRACE bond trade records
CREATE TABLE IF NOT EXISTS trades (
    id              SERIAL PRIMARY KEY,
    control_id      TEXT,
    coupon          DOUBLE PRECISION,
    cusip           TEXT,
    dealer_id       INTEGER,
    exec_time       TEXT,
    industry        TEXT,
    issuer          TEXT,
    maturity        TEXT,
    modifier3       TEXT,
    price           DOUBLE PRECISION,
    rating          TEXT,
    report_time     TEXT,
    reporting_capacity TEXT,
    side            TEXT,
    volume          BIGINT,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trades_cusip ON trades(cusip);
CREATE INDEX IF NOT EXISTS idx_trades_issuer ON trades(issuer);
CREATE INDEX IF NOT EXISTS idx_trades_exec_time ON trades(exec_time);

-- Issuer info table: reference data used to enrich trades
CREATE TABLE IF NOT EXISTS issuer_info (
    issuer      TEXT PRIMARY KEY,
    rating      TEXT NOT NULL,
    industry    TEXT NOT NULL
);

-- Seed issuer reference data matching the fake_trace_generator.py issuers
INSERT INTO issuer_info (issuer, rating, industry) VALUES
    ('3M',                      'A+',   'Industrials'),
    ('Amgen',                   'A',    'Healthcare'),
    ('Apple',                   'AA+',  'Technology'),
    ('American Express',        'A-',   'Financial Services'),
    ('Boeing',                  'BBB-', 'Aerospace'),
    ('Caterpillar',             'A',    'Industrials'),
    ('Chevron',                 'AA-',  'Energy'),
    ('Cisco Systems',           'AA-',  'Technology'),
    ('Coca-Cola',               'A+',   'Consumer Staples'),
    ('Disney',                  'A-',   'Communication Services'),
    ('Dow Inc.',                'BBB+', 'Materials'),
    ('Goldman Sachs',           'A+',   'Financial Services'),
    ('Home Depot',              'A',    'Consumer Discretionary'),
    ('Honeywell',               'A',    'Industrials'),
    ('IBM',                     'A-',   'Technology'),
    ('Intel',                   'A+',   'Technology'),
    ('Johnson & Johnson',       'AAA',  'Healthcare'),
    ('JPMorgan Chase',          'A+',   'Financial Services'),
    ('Merck',                   'A+',   'Healthcare'),
    ('Microsoft',               'AAA',  'Technology'),
    ('Nike',                    'AA-',  'Consumer Discretionary'),
    ('Procter & Gamble',        'AA-',  'Consumer Staples'),
    ('Salesforce',              'A+',   'Technology'),
    ('Travelers',               'A',    'Financial Services'),
    ('Verizon',                 'BBB+', 'Communication Services'),
    ('Visa',                    'AA-',  'Financial Services'),
    ('Walgreens Boots Alliance','BBB',  'Healthcare'),
    ('Walmart',                 'AA',   'Consumer Staples')
ON CONFLICT (issuer) DO NOTHING;
