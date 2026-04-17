CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS accounts (
    id VARCHAR(50) PRIMARY KEY,
    owner_name VARCHAR(100),
    balance NUMERIC(15, 2) NOT NULL DEFAULT 0.00
);

INSERT INTO accounts (id, owner_name, balance) 
VALUES ('acc_123', 'Suvrat', 1000.00), ('acc_456', 'Acharya', 500.00)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id VARCHAR(50) NOT NULL,
    from_account VARCHAR(50) NOT NULL,
    to_account VARCHAR(50) NOT NULL,
    amount NUMERIC(15, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_txn_id ON audit_logs(transaction_id);
CREATE INDEX IF NOT EXISTS idx_audit_created_at ON audit_logs(created_at);

-- Double-entry ledger entries table for an auditable journal of debits/credits
CREATE TABLE IF NOT EXISTS ledger_entries (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL,
    account_id VARCHAR(50) NOT NULL,
    amount NUMERIC(15,2) NOT NULL,
    entry_type VARCHAR(10) NOT NULL,
    CONSTRAINT unique_ledger_entry UNIQUE (transaction_id, account_id, entry_type),
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ledger_txn_id ON ledger_entries(transaction_id);
CREATE INDEX IF NOT EXISTS idx_ledger_account_id ON ledger_entries(account_id);

-- Transaction header table used for idempotency and lightweight transaction metadata.
CREATE TABLE IF NOT EXISTS transaction_headers (
    transaction_id VARCHAR(50) PRIMARY KEY,
    from_account VARCHAR(50) NOT NULL,
    to_account VARCHAR(50) NOT NULL,
    amount NUMERIC(15,2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transaction_headers_created_at ON transaction_headers(created_at);