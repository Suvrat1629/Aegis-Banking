package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const AuditQueueName = "audit_queue"

type RabbitMQProducer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewRabbitMQProducer(url string) (*RabbitMQProducer, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	_, err = ch.QueueDeclare(AuditQueueName, true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	return &RabbitMQProducer{conn: conn, channel: ch}, nil
}

func (p *RabbitMQProducer) PublishAudit(txnID, from, to string, amount float64) error {
	event := AuditEvent{
		TransactionID: txnID,
		FromAccount:   from,
		ToAccount:     to,
		Amount:        amount,
		Status:        "COMPLETED",
		Timestamp:     time.Now(),
		Message:       "Transfer completed successfully",
		Currency:      "INR",
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal audit event: %w", err)
	}

	err = p.channel.PublishWithContext(
		context.Background(),
		"",
		AuditQueueName,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		},
	)

	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	log.Printf("📤 Audit published: txn=%s %s→%s ₹%.2f", txnID, from, to, amount)
	return nil
}

func (p *RabbitMQProducer) PublishAuditFromPayload(payload []byte) error {
	if p == nil || p.channel == nil {
		return fmt.Errorf("rabbitmq producer not initialized")
	}

	err := p.channel.PublishWithContext(
		context.Background(),
		"",
		AuditQueueName,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         payload,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish raw audit payload: %w", err)
	}

	log.Printf("Audit published (raw payload), %d bytes", len(payload))
	return nil
}

func (p *RabbitMQProducer) Close() {
	if p.channel != nil {
		p.channel.Close()
	}
	if p.conn != nil {
		p.conn.Close()
	}
}