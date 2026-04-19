CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS accounts (
    id           VARCHAR(50) PRIMARY KEY,
    owner_name   VARCHAR(100),
    balance      NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Insert test accounts (idempotent)
INSERT INTO accounts (id, owner_name, balance)
VALUES ('acc_123', 'Suvrat', 1000.00),
       ('acc_456', 'Acharya', 500.00)
ON CONFLICT (id) DO NOTHING;

-- Index for faster queries involving last_updated
CREATE INDEX IF NOT EXISTS idx_accounts_last_updated ON accounts(last_updated);

CREATE TABLE IF NOT EXISTS audit_logs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id  VARCHAR(50) NOT NULL,
    from_account    VARCHAR(50) NOT NULL,
    to_account      VARCHAR(50) NOT NULL,
    amount          NUMERIC(15, 2) NOT NULL,
    status          VARCHAR(20) NOT NULL,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_txn_id ON audit_logs(transaction_id);
CREATE INDEX IF NOT EXISTS idx_audit_created_at ON audit_logs(created_at);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id              SERIAL PRIMARY KEY,
    transaction_id  VARCHAR(50) NOT NULL,
    account_id      VARCHAR(50) NOT NULL,
    amount          NUMERIC(15,2) NOT NULL,
    entry_type      VARCHAR(10) NOT NULL CHECK (entry_type IN ('DEBIT', 'CREDIT')),
    metadata        JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT unique_ledger_entry 
        UNIQUE (transaction_id, account_id, entry_type)
);

CREATE INDEX IF NOT EXISTS idx_ledger_txn_id ON ledger_entries(transaction_id);
CREATE INDEX IF NOT EXISTS idx_ledger_account_id ON ledger_entries(account_id);

CREATE TABLE IF NOT EXISTS transaction_headers (
    transaction_id  VARCHAR(50) PRIMARY KEY,
    from_account    VARCHAR(50) NOT NULL,
    to_account      VARCHAR(50) NOT NULL,
    amount          NUMERIC(15,2) NOT NULL,
    status          VARCHAR(20) NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transaction_headers_created_at 
    ON transaction_headers(created_at);

UPDATE accounts 
SET last_updated = NOW() 
WHERE last_updated IS NULL;

CREATE TABLE IF NOT EXISTS outbox (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type   VARCHAR(50) NOT NULL,        -- "TRANSFER"
    aggregate_id     VARCHAR(50) NOT NULL,        -- transaction_id
    event_type       VARCHAR(100) NOT NULL,
    payload          JSONB NOT NULL,
    status           VARCHAR(20) DEFAULT 'PENDING',
    retry_count      INT DEFAULT 0,
    created_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at     TIMESTAMP WITH TIME ZONE,
    next_attempt_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending 
    ON outbox(status, next_attempt_at) 
    WHERE status = 'PENDING';