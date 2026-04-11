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

func (r *AccountRepository) ExecuteTransfer(ctx context.Context, from, to string, amount float64) error {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelLinearizable})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	transactionID := fmt.Sprintf("txn_%d", time.Now().UnixNano())

	// 1. Check balance
	var balance float64
	err = tx.QueryRowContext(ctx, 
		"SELECT balance FROM accounts WHERE id = $1 FOR UPDATE", from).Scan(&balance)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("from account not found: %s", from)
		}
		return fmt.Errorf("failed to get balance: %w", err)
	}

	if balance < amount {
		return fmt.Errorf("insufficient balance in account %s: have %.2f, need %.2f", from, balance, amount)
	}

	//2. Debit
	_, err = tx.ExecContext(ctx, 
		"UPDATE accounts SET balance = balance - $1 WHERE id = $2", amount, from)
	if err != nil {
		return fmt.Errorf("failed to debit account: %w", err)
	}

	// 3. Credit
	_, err = tx.ExecContext(ctx,
		"UPDATE accounts SET balance = balance + $1 WHERE id = $2", amount, to)
	if err != nil {
		return fmt.Errorf("failed to credit account: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO transactions (transaction_id, from_account, to_account, amount, status)
		 VALUES ($1, $2, $3, $4, 'COMPLETED')`,
		 transactionID, from, to, amount)
	if err != nil {
		return fmt.Errorf("failed to log transaction: %w", err)
	}

	// 4.5 Save audit log inside the same transaction so audit is ACID with the transfer
	if err := r.SaveAuditLog(ctx, tx, transactionID, from, to, amount); err != nil {
		return fmt.Errorf("failed to save audit log: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (r *AccountRepository) SaveAuditLog(ctx context.Context, tx *sql.Tx, txnID, from, to string, amount float64) error {
	payload := map[string]interface{}{
		"from_account": from,
		"to_account":   to,
		"amount":       amount,
		"currency":     "INR",
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
		return fmt.Errorf("failed to insert audit log: %w", err)
	}
	return nil
}