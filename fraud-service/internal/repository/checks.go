package repository

import (
	"context"
	"database/sql"
	"fmt"
)

type ChecksRepository struct {
	db *sql.DB
}

func NewChecksRepository(db *sql.DB) *ChecksRepository {
	return &ChecksRepository{db: db}
}

func (r *ChecksRepository) RecentTransferCount(ctx context.Context, fromAccount string, windowSeconds int) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM risk_checks
		 WHERE from_account = $1
		   AND checked_at >= NOW() - make_interval(secs => $2::int)`,
		fromAccount, windowSeconds).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count recent transfers: %w", err)
	}
	return count, nil
}

func (r *ChecksRepository) RecordCheck(ctx context.Context, transactionID, fromAccount, toAccount string, amount float64, verdict, reason string) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO risk_checks (transaction_id, from_account, to_account, amount, verdict, reason)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		transactionID, fromAccount, toAccount, amount, verdict, reason)
	if err != nil {
		return fmt.Errorf("failed to record risk check: %w", err)
	}
	return nil
}
