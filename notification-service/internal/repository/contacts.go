package repository

import (
	"context"
	"database/sql"
)

type ContactsRepository struct {
	db *sql.DB
}

func NewContactsRepository(db *sql.DB) *ContactsRepository {
	return &ContactsRepository{db: db}
}

// Upsert records/refreshes an account's contact info, fed by CUSTOMER_CREATED
// events consumed off the account_events topic.
func (r *ContactsRepository) Upsert(ctx context.Context, accountID, ownerName, email, phone string) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO account_contacts (account_id, owner_name, email, phone, updated_at)
		 VALUES ($1, $2, $3, $4, NOW())
		 ON CONFLICT (account_id) DO UPDATE
		 SET owner_name = EXCLUDED.owner_name,
		     email      = EXCLUDED.email,
		     phone      = EXCLUDED.phone,
		     updated_at = NOW()`,
		accountID, ownerName, email, phone)
	return err
}

// Lookup returns the cached contact info for an account, or sql.ErrNoRows if
// this service hasn't seen a CUSTOMER_CREATED event for it yet (e.g. an account
// that was never created through account-service).
func (r *ContactsRepository) Lookup(ctx context.Context, accountID string) (ownerName, email, phone string, err error) {
	err = r.db.QueryRowContext(ctx,
		"SELECT owner_name, email, phone FROM account_contacts WHERE account_id = $1", accountID).
		Scan(&ownerName, &email, &phone)
	return
}
