package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	pb "github.com/aegis-banking/ledger-core/internal/pb"
)

type AccountRepository struct {
	db *sql.DB
}

func NewAccountRepository(db *sql.DB) *AccountRepository {
	return &AccountRepository{db: db}
}

func (r *AccountRepository) ExecuteTransfer(ctx context.Context, transactionID, from, to string, amount float64, deviceID, ipAddress, userAgent string) error {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Idempotency: if transaction already recorded in transaction_headers, return nil (idempotent)
	var exists bool
	err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM transaction_headers WHERE transaction_id = $1)", transactionID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check existing transaction: %w", err)
	}
	if exists {
		// already applied
		return nil
	}

	// Lock accounts in a deterministic order to avoid deadlocks under concurrent transfers
	first, second := from, to
	if from > to {
		first, second = to, from
	}

	// Acquire FOR UPDATE locks in the same order every time
	if _, err := tx.ExecContext(ctx, "SELECT 1 FROM accounts WHERE id = $1 FOR UPDATE", first); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("account not found: %s", first)
		}
		return fmt.Errorf("failed to lock first account: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "SELECT 1 FROM accounts WHERE id = $1 FOR UPDATE", second); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("account not found: %s", second)
		}
		return fmt.Errorf("failed to lock second account: %w", err)
	}

	// Now read balances (rows are locked)
	var fromBalance float64
	err = tx.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE id = $1", from).Scan(&fromBalance)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("from account not found: %s", from)
		}
		return fmt.Errorf("failed to read from account balance: %w", err)
	}
	if fromBalance < amount {
		return fmt.Errorf("insufficient balance in %s: have %.2f, need %.2f", from, fromBalance, amount)
	}

	var toBalance float64
	err = tx.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE id = $1", to).Scan(&toBalance)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("to account not found: %s", to)
		}
		return fmt.Errorf("failed to read to account balance: %w", err)
	}

	// Apply balance updates
	_, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance - $1, last_updated = NOW() WHERE id = $2", amount, from)
	if err != nil {
		return fmt.Errorf("debit failed: %w", err)
	}
	_, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance + $1, last_updated = NOW() WHERE id = $2", amount, to)
	if err != nil {
		return fmt.Errorf("credit failed: %w", err)
	}

	// Record a transaction header (used for idempotency and simple searching)
	_, err = tx.ExecContext(ctx,
		`INSERT INTO transaction_headers (transaction_id, from_account, to_account, amount, status)
		 VALUES ($1, $2, $3, $4, 'COMPLETED')`,
		transactionID, from, to, amount)
	if err != nil {
		return fmt.Errorf("failed to insert transaction header: %w", err)
	}

	// Insert double-entry ledger rows: DEBIT on source (negative), CREDIT on destination (positive)
	_, err = tx.ExecContext(ctx,
		`INSERT INTO ledger_entries (transaction_id, account_id, amount, entry_type)
		 VALUES ($1, $2, $3, 'DEBIT')`,
		transactionID, from, -amount)
	if err != nil {
		return fmt.Errorf("failed to insert ledger debit entry: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO ledger_entries (transaction_id, account_id, amount, entry_type)
		 VALUES ($1, $2, $3, 'CREDIT')`,
		transactionID, to, amount)
	if err != nil {
		return fmt.Errorf("failed to insert ledger credit entry: %w", err)
	}

	// 5. Save rich audit log (ACID guaranteed)
	if err := r.SaveAuditLog(ctx, tx, transactionID, from, to, amount, deviceID, ipAddress, userAgent); err != nil {
		return fmt.Errorf("failed to save audit log: %w", err)
	}

	// Commit everything
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (r *AccountRepository) GetBalance(ctx context.Context, id string) (float64, string, string, error) {
	var balance float64
	var owner string
	var lastUpdated time.Time

	query := "SELECT balance, owner_name, last_updated FROM accounts WHERE id = $1"
	err := r.db.QueryRowContext(ctx, query, id).Scan(&balance, &owner, &lastUpdated)
	if err != nil {
		return 0, "", "", err
	}
	return balance, owner, lastUpdated.Format(time.RFC3339), nil
}

func (r *AccountRepository) GetHistory(ctx context.Context, id string, limit, offset int32) ([]*pb.TransactionEntry, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT transaction_id, ABS(amount), entry_type, COALESCE(description, ''), created_at 
		FROM ledger_entries 
		WHERE account_id = $1 
		ORDER BY created_at DESC 
		LIMIT $2 OFFSET $3`

	rows, err := r.db.QueryContext(ctx, query, id, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*pb.TransactionEntry
	for rows.Next() {
		var e pb.TransactionEntry
		var ts time.Time
		if err := rows.Scan(&e.TransactionId, &e.Amount, &e.EntryType, &e.Description, &ts); err != nil {	
			return nil, err
		}
		e.CreatedAt = ts.Format(time.RFC3339)
		entries = append(entries, &e)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

func (r *AccountRepository) SaveAuditLog(ctx context.Context, tx *sql.Tx, txnID, from, to string, amount float64, deviceID, ipAddress, userAgent string) error {
	payload := map[string]interface{}{
		"device_id":   deviceID,
		"ip_address":  ipAddress,
		"user_agent":  userAgent,
		"timestamp":   time.Now().Format(time.RFC3339),
		"environment": "development",
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal audit payload: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO audit_logs (transaction_id, from_account, to_account, amount, status, payload)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		txnID, from, to, amount, "COMPLETED", payloadBytes,
	)
	if err != nil {
		return fmt.Errorf("failed to insert into audit_logs: %w", err)
	}

	return nil
}
