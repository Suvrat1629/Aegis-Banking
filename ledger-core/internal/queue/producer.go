package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const AuditTopic = "audit_events"

type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer(brokers string) (*KafkaProducer, error) {
	addrs := strings.Split(brokers, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(addrs...),
		Topic:                  AuditTopic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafka.RequireAll,
	}

	return &KafkaProducer{writer: writer}, nil
}

func (p *KafkaProducer) PublishAudit(txnID, from, to string, amount float64, refTxnID string) error {
	event := AuditEvent{
		TransactionID:          txnID,
		FromAccount:            from,
		ToAccount:              to,
		Amount:                 amount,
		Status:                 "COMPLETED",
		Timestamp:              time.Now(),
		Message:                "Transfer completed successfully",
		Currency:               "INR",
		EventType:              "TRANSFER_COMPLETED",
		AggregateType:          "TRANSFER",
		ReferenceTransactionID: refTxnID,
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal audit event: %w", err)
	}

	err = p.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(txnID),
		Value: body,
	})
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	log.Printf("📤 Audit published: txn=%s %s→%s ₹%.2f", txnID, from, to, amount)
	return nil
}

func (p *KafkaProducer) PublishAuditFromPayload(payload []byte) error {
	if p == nil || p.writer == nil {
		return fmt.Errorf("kafka producer not initialized")
	}

	var key []byte
	var fields struct {
		TransactionID string `json:"transaction_id"`
	}
	if err := json.Unmarshal(payload, &fields); err == nil && fields.TransactionID != "" {
		key = []byte(fields.TransactionID)
	}

	err := p.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   key,
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("failed to publish raw audit payload: %w", err)
	}

	log.Printf("Audit published (raw payload), %d bytes", len(payload))
	return nil
}

func (p *KafkaProducer) Close() {
	if p.writer != nil {
		p.writer.Close()
	}
}
