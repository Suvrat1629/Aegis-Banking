package consumer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"strings"

	"github.com/aegis-banking/fraud-service/internal/grpcclient"
	"github.com/aegis-banking/fraud-service/internal/observability"
	"github.com/aegis-banking/fraud-service/internal/repository"
	"github.com/aegis-banking/fraud-service/internal/rules"
	"github.com/segmentio/kafka-go"
)

const (
	AuditEventsTopic = "audit_events"
	ConsumerGroupID  = "fraud-service"
)

type Consumer struct {
	reader   *kafka.Reader
	scored   *repository.ScoredRepository
	reversal *repository.ReversalsRepository
	ledger   *grpcclient.LedgerClient
}

func New(brokers string, scored *repository.ScoredRepository, reversal *repository.ReversalsRepository, ledger *grpcclient.LedgerClient) *Consumer {
	addrs := strings.Split(brokers, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     addrs,
		GroupID:     ConsumerGroupID,
		GroupTopics: []string{AuditEventsTopic},
	})

	return &Consumer{reader: reader, scored: scored, reversal: reversal, ledger: ledger}
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
		c.handleAuditEvent(ctx, msg.Value)
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

type auditEvent struct {
	TransactionID          string  `json:"transaction_id"`
	FromAccount            string  `json:"from_account"`
	ToAccount              string  `json:"to_account"`
	Amount                 float64 `json:"amount"`
	EventType              string  `json:"event_type"`
	ReferenceTransactionID string  `json:"reference_transaction_id"`
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

	if ev.ReferenceTransactionID != "" {
		log.Printf("Consumer: skipping reversal event txn=%s (references %s)", ev.TransactionID, ev.ReferenceTransactionID)
		return
	}

	first, err := c.scored.TryRecord(ctx, ev.TransactionID, ev.FromAccount, ev.ToAccount, ev.Amount)
	if err != nil {
		log.Printf("Consumer: failed to record scored transfer txn=%s: %v", ev.TransactionID, err)
		return
	}
	if !first {
		log.Printf("Consumer: duplicate delivery, already scored txn=%s", ev.TransactionID)
		return
	}

	observability.FraudRescoredTotal.Inc()

	cumulative, err := c.scored.CumulativeAmount(ctx, ev.FromAccount, rules.AsyncWindowSeconds)
	if err != nil {
		log.Printf("Consumer: failed to compute cumulative amount for %s: %v (skipping rescore)", ev.FromAccount, err)
		return
	}

	if !rules.EvaluateAsync(cumulative) {
		return
	}

	log.Printf("Async rule flagged from_account=%s cumulative=%.2f (window=%ds) txn=%s", ev.FromAccount, cumulative, rules.AsyncWindowSeconds, ev.TransactionID)
	if err := c.scored.MarkFlagged(ctx, ev.TransactionID); err != nil {
		log.Printf("Consumer: failed to mark txn=%s flagged: %v", ev.TransactionID, err)
	}

	c.issueReversal(ctx, ev.TransactionID, ev.FromAccount, ev.ToAccount, ev.Amount)
}

func (c *Consumer) issueReversal(ctx context.Context, originalTxnID, from, to string, amount float64) {
	reversalTxnID := "rev_" + generateSuffix()

	claimed, err := c.reversal.TryClaim(ctx, originalTxnID, reversalTxnID)
	if err != nil {
		log.Printf("Consumer: failed to claim reversal for txn=%s: %v", originalTxnID, err)
		return
	}
	if !claimed {
		log.Printf("Consumer: reversal already claimed for txn=%s, skipping", originalTxnID)
		return
	}

	// Reversed direction: the original recipient (to) pays back the original
	// sender (from). reference_transaction_id=originalTxnID is what lets our
	// own consumer recognize this event's eventual audit_events message as a
	// reversal (loop guard above) and lets a human auditing ledger_entries
	// see which transfer this compensates.
	if err := c.ledger.ExecuteTransfer(ctx, reversalTxnID, to, from, amount, originalTxnID); err != nil {
		observability.FraudReversalsTotal.WithLabelValues("FAILED").Inc()
		if markErr := c.reversal.MarkFailed(ctx, originalTxnID); markErr != nil {
			log.Printf("Consumer: failed to mark reversal failed for txn=%s: %v", originalTxnID, markErr)
		}
		// Documented, not solved: if the flagged account already spent the
		// funds, this fails with insufficient balance. A real system would
		// escalate to a human/case-management workflow here; we log + metric it.
		log.Printf("Consumer: reversal FAILED for original txn=%s (reversal=%s): %v", originalTxnID, reversalTxnID, err)
		return
	}

	observability.FraudReversalsTotal.WithLabelValues("ISSUED").Inc()
	log.Printf("Reversal issued: original=%s reversal=%s %s→%s ₹%.2f", originalTxnID, reversalTxnID, to, from, amount)
}

func generateSuffix() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
