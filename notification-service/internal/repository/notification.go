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

func (r *NotificationRepository) Exists(ctx context.Context, transactionID, accountID string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM notifications WHERE transaction_id = $1 AND account_id = $2)",
		transactionID, accountID).Scan(&exists)
	return exists, err
}

func (r *NotificationRepository) Insert(ctx context.Context, transactionID, accountID, channel, recipient, message, status string) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO notifications (transaction_id, account_id, channel, recipient, message, status)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT ON CONSTRAINT unique_notification_per_transaction_account DO NOTHING`,
		transactionID, accountID, channel, recipient, message, status)
	return err
}
