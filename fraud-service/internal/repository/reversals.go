package repository

import (
	"context"
	"database/sql"
	"fmt"
)

type ReversalsRepository struct {
	db *sql.DB
}

func NewReversalsRepository(db *sql.DB) *ReversalsRepository {
	return &ReversalsRepository{db: db}
}

func (r *ReversalsRepository) TryClaim(ctx context.Context, originalTransactionID, reversalTransactionID string) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`INSERT INTO reversals (original_transaction_id, reversal_transaction_id, status)
		 VALUES ($1, $2, 'ISSUED')
		 ON CONFLICT (original_transaction_id) DO NOTHING`,
		originalTransactionID, reversalTransactionID)
	if err != nil {
		return false, fmt.Errorf("failed to claim reversal: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to check rows affected: %w", err)
	}
	return rows > 0, nil
}

func (r *ReversalsRepository) MarkFailed(ctx context.Context, originalTransactionID string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE reversals SET status = 'FAILED' WHERE original_transaction_id = $1`,
		originalTransactionID)
	if err != nil {
		return fmt.Errorf("failed to mark reversal failed: %w", err)
	}
	return nil
}
