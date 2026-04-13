package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
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

	// 1. Check balance with row lock
	var balance float64
	err = tx.QueryRowContext(ctx,
		"SELECT balance FROM accounts WHERE id = $1 FOR UPDATE", from).Scan(&balance)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("from account not found: %s", from)
		}
		return fmt.Errorf("failed to read balance: %w", err)
	}

	if balance < amount {
		return fmt.Errorf("insufficient balance in %s: have %.2f, need %.2f", from, balance, amount)
	}

	// 2. Debit
	_, err = tx.ExecContext(ctx,
		"UPDATE accounts SET balance = balance - $1 WHERE id = $2", amount, from)
	if err != nil {
		return fmt.Errorf("debit failed: %w", err)
	}

	// 3. Credit
	_, err = tx.ExecContext(ctx,
		"UPDATE accounts SET balance = balance + $1 WHERE id = $2", amount, to)
	if err != nil {
		return fmt.Errorf("credit failed: %w", err)
	}

	// 4. Log to transactions table
	_, err = tx.ExecContext(ctx,
		`INSERT INTO transactions (transaction_id, from_account, to_account, amount, status)
		 VALUES ($1, $2, $3, $4, 'COMPLETED')`,
		transactionID, from, to, amount)
	if err != nil {
		return fmt.Errorf("failed to log transaction: %w", err)
	}

	// 5. Save rich audit log (ACID guaranteed)
	if err := r.SaveAuditLog(ctx, tx, transactionID, from, to, amount, deviceID, ipAddress, userAgent); err != nil {
		return fmt.Errorf("failed to save audit log: %w", err)
	}

	// 6. Commit everything
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
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
