package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

type AccountRepository struct {
	db *sql.DB
}

func NewAccountRepository(db *sql.DB) *AccountRepository {
	return &AccountRepository{db: db}
}

// CreateAccount inserts the identity record and its CUSTOMER_CREATED outbox event
// in a single local transaction. The caller (internal/service/account.go) picks
// accountID and must have already registered it with ledger-core before calling
// this — if the ledger-core call fails, nothing should be written here at all.
func (r *AccountRepository) CreateAccount(ctx context.Context, accountID, ownerName, email, phone string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx,
		`INSERT INTO accounts (id, owner_name, email, phone)
		 VALUES ($1, $2, $3, $4)`,
		accountID, ownerName, email, phone)
	if err != nil {
		return fmt.Errorf("failed to insert account: %w", err)
	}

	payload := map[string]any{
		"account_id": accountID,
		"owner_name": ownerName,
		"email":      email,
		"phone":      phone,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal outbox payload: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
		 VALUES ('ACCOUNT', $1, 'CUSTOMER_CREATED', $2)`,
		accountID, payloadBytes)
	if err != nil {
		return fmt.Errorf("failed to insert into outbox: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (r *AccountRepository) GetAccount(ctx context.Context, accountID string) (ownerName, email, phone string, err error) {
	err = r.db.QueryRowContext(ctx,
		"SELECT owner_name, email, phone FROM accounts WHERE id = $1", accountID).
		Scan(&ownerName, &email, &phone)
	return
}
