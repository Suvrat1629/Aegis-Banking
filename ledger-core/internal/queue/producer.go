package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	AuditQueueName = "audit_queue"
)

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

	_, err = ch.QueueDeclare(
		AuditQueueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	return &RabbitMQProducer{
		conn: 	 conn,
		channel: ch,
	}, nil
}

func (p *RabbitMQProducer) PublishAudit(from, to string, amount float64) error {
	event := AuditEvent{
		TransactionID: fmt.Sprintf("txn_%d", time.Now().UnixNano()),
		FromAccount:   from,
		ToAccount:     to,
		Amount:        amount,
		Status:        "COMPLETED",
		Timestamp:     time.Now(),
		Message:       "Transfer completed successfully",
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal audit event: %w", err)
	}

	err = p.channel.PublishWithContext(
		context.Background(),
		"",                // exchange (default exchange)
		AuditQueueName,    // routing key = queue name
		false,             // mandatory
		false,             // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		},
	)

	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	log.Printf("Published audit event: %s → %s | ₹%.2f", from, to, amount)
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