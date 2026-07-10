package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/aegis-banking/notification-service/internal/mailer"
	"github.com/aegis-banking/notification-service/internal/observability"
	"github.com/aegis-banking/notification-service/internal/repository"
	"github.com/segmentio/kafka-go"
)

const (
	AccountEventsTopic = "account_events"
	AuditEventsTopic   = "audit_events"
	ConsumerGroupID    = "notification-service"
)

type Consumer struct {
	reader   *kafka.Reader
	contacts *repository.ContactsRepository
	notifs   *repository.NotificationRepository
	mailer   *mailer.Mailer
}

func New(brokers string, contacts *repository.ContactsRepository, notifs *repository.NotificationRepository, m *mailer.Mailer) *Consumer {
	addrs := strings.Split(brokers, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     addrs,
		GroupID:     ConsumerGroupID,
		GroupTopics: []string{AccountEventsTopic, AuditEventsTopic},
	})

	return &Consumer{reader: reader, contacts: contacts, notifs: notifs, mailer: m}
}

func (c *Consumer) Start(ctx context.Context) {
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Consumer: context cancelled, stopping")
				return
			}
			log.Printf("Consumer: read failed: %v", err)
			continue
		}

		switch msg.Topic {
		case AccountEventsTopic:
			c.handleAccountEvent(ctx, msg.Value)
		case AuditEventsTopic:
			c.handleAuditEvent(ctx, msg.Value)
		default:
			log.Printf("Consumer: unexpected topic %s, skipping", msg.Topic)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

type accountEvent struct {
	AccountID string `json:"account_id"`
	OwnerName string `json:"owner_name"`
	Email     string `json:"email"`
	Phone     string `json:"phone"`
	EventType string `json:"event_type"`
}

func (c *Consumer) handleAccountEvent(ctx context.Context, payload []byte) {
	var ev accountEvent
	if err := json.Unmarshal(payload, &ev); err != nil {
		log.Printf("Consumer: failed to parse account event: %v", err)
		return
	}

	if ev.EventType != "CUSTOMER_CREATED" {
		log.Printf("Consumer: ignoring unknown account event_type=%s", ev.EventType)
		return
	}

	if err := c.contacts.Upsert(ctx, ev.AccountID, ev.OwnerName, ev.Email, ev.Phone); err != nil {
		log.Printf("Consumer: failed to upsert contact for account_id=%s: %v", ev.AccountID, err)
		return
	}
	log.Printf("Contact cached: account_id=%s email=%s", ev.AccountID, ev.Email)
}

type auditEvent struct {
	TransactionID string  `json:"transaction_id"`
	FromAccount   string  `json:"from_account"`
	ToAccount     string  `json:"to_account"`
	Amount        float64 `json:"amount"`
	EventType     string  `json:"event_type"`
}

func (c *Consumer) handleAuditEvent(ctx context.Context, payload []byte) {
	var ev auditEvent
	if err := json.Unmarshal(payload, &ev); err != nil {
		log.Printf("Consumer: failed to parse audit event: %v", err)
		return
	}

	if ev.EventType != "TRANSFER_COMPLETED" {
		log.Printf("Consumer: ignoring unknown audit event_type=%s", ev.EventType)
		return
	}

	c.notifyParty(ctx, ev.TransactionID, ev.FromAccount, "debited", ev.Amount)
	c.notifyParty(ctx, ev.TransactionID, ev.ToAccount, "credited", ev.Amount)
}

func (c *Consumer) notifyParty(ctx context.Context, transactionID, accountID, verb string, amount float64) {
	exists, err := c.notifs.Exists(ctx, transactionID, accountID)
	if err != nil {
		log.Printf("Consumer: failed to check existing notification (txn=%s, account=%s): %v", transactionID, accountID, err)
		return
	}
	if exists {
		log.Printf("Consumer: duplicate delivery, already notified txn=%s account=%s", transactionID, accountID)
		return
	}

	_, email, _, err := c.contacts.Lookup(ctx, accountID)
	message := fmt.Sprintf("Your account %s was %s ₹%.2f (transaction %s)", accountID, verb, amount, transactionID)

	if err != nil || email == "" {
		observability.NotificationsSkippedTotal.Inc()
		if insertErr := c.notifs.Insert(ctx, transactionID, accountID, "EMAIL", "", message, "SKIPPED_NO_CONTACT"); insertErr != nil {
			log.Printf("Consumer: failed to record skipped notification: %v", insertErr)
		}
		log.Printf("Notification skipped (no contact info): account=%s txn=%s", accountID, transactionID)
		return
	}

	subject := "Aegis Banking: transaction notification"
	if sendErr := c.mailer.Send(email, subject, message); sendErr != nil {
		observability.NotificationsFailedTotal.Inc()
		if insertErr := c.notifs.Insert(ctx, transactionID, accountID, "EMAIL", email, message, "FAILED"); insertErr != nil {
			log.Printf("Consumer: failed to record failed notification: %v", insertErr)
		}
		log.Printf("Notification send failed: account=%s txn=%s err=%v", accountID, transactionID, sendErr)
		return
	}

	observability.NotificationsSentTotal.Inc()
	if err := c.notifs.Insert(ctx, transactionID, accountID, "EMAIL", email, message, "SENT"); err != nil {
		log.Printf("Consumer: failed to record sent notification: %v", err)
	}
	log.Printf("📧 Notification sent: account=%s email=%s txn=%s", accountID, email, transactionID)
}
