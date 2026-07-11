package repository

import (
	"context"
	"database/sql"
	"fmt"
)

type ScoredRepository struct {
	db *sql.DB
}

func NewScoredRepository(db *sql.DB) *ScoredRepository {
	return &ScoredRepository{db: db}
}

func (r *ScoredRepository) TryRecord(ctx context.Context, transactionID, fromAccount, toAccount string, amount float64) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`INSERT INTO scored_transfers (transaction_id, from_account, to_account, amount)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (transaction_id) DO NOTHING`,
		transactionID, fromAccount, toAccount, amount)
	if err != nil {
		return false, fmt.Errorf("failed to record scored transfer: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to check rows affected: %w", err)
	}
	return rows > 0, nil
}

func (r *ScoredRepository) CumulativeAmount(ctx context.Context, fromAccount string, windowSeconds int) (float64, error) {
	var total sql.NullFloat64
	err := r.db.QueryRowContext(ctx,
		`SELECT SUM(amount) FROM scored_transfers
		 WHERE from_account = $1
		   AND scored_at >= NOW() - make_interval(secs => $2::int)`,
		fromAccount, windowSeconds).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("failed to sum cumulative amount: %w", err)
	}
	if !total.Valid {
		return 0, nil
	}
	return total.Float64, nil
}

func (r *ScoredRepository) MarkFlagged(ctx context.Context, transactionID string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE scored_transfers SET flagged = TRUE WHERE transaction_id = $1`, transactionID)
	if err != nil {
		return fmt.Errorf("failed to mark transfer flagged: %w", err)
	}
	return nil
}
