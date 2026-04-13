package queue

import "time"

type AuditEvent struct {
	TransactionID string    `json:"transaction_id"`
	FromAccount   string    `json:"from_account"`
	ToAccount     string    `json:"to_account"`
	Amount        float64   `json:"amount"`
	Status        string    `json:"status"`
	Timestamp     time.Time `json:"timestamp"`
	Message       string    `json:"message,omitempty"`
	Currency      string    `json:"currency,omitempty"`
}