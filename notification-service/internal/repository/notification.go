package repository

import (
	"context"
	"database/sql"
)

type NotificationRepository struct {
	db *sql.DB
}

func NewNotificationRepository(db *sql.DB) *NotificationRepository {
	return &NotificationRepository{db: db}
}

// Exists reports whether a notification was already recorded for this
// (transaction, account) pair. ledger-core publishes to audit_events via two
// paths (the outbox relay and a direct fire-and-forget call), so the same
// transfer can arrive twice — callers should check this before sending to
// avoid double-notifying. The DB's UNIQUE(transaction_id, account_id)
// constraint is the backstop if this check ever races.
func (r *NotificationRepository) Exists(ctx context.Context, transactionID, accountID string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM notifications WHERE transaction_id = $1 AND account_id = $2)",
		transactionID, accountID).Scan(&exists)
	return exists, err
}

// Insert records a notification attempt regardless of outcome (SENT,
// SKIPPED_NO_CONTACT, or FAILED) — the point is a durable record that we did
// (or didn't) notify someone, not just a log line. Treats a unique-constraint
// conflict as "already handled" rather than an error.
func (r *NotificationRepository) Insert(ctx context.Context, transactionID, accountID, channel, recipient, message, status string) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO notifications (transaction_id, account_id, channel, recipient, message, status)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT ON CONSTRAINT unique_notification_per_transaction_account DO NOTHING`,
		transactionID, accountID, channel, recipient, message, status)
	return err
}
