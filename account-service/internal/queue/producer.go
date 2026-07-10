package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/segmentio/kafka-go"
)

const AccountTopic = "account_events"

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
		Topic:                  AccountTopic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafka.RequireAll,
	}

	return &KafkaProducer{writer: writer}, nil
}

func (p *KafkaProducer) PublishAccountEventFromPayload(payload []byte) error {
	if p == nil || p.writer == nil {
		return fmt.Errorf("kafka producer not initialized")
	}

	var key []byte
	var fields struct {
		AccountID string `json:"account_id"`
	}
	if err := json.Unmarshal(payload, &fields); err == nil && fields.AccountID != "" {
		key = []byte(fields.AccountID)
	}

	err := p.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   key,
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("failed to publish account event payload: %w", err)
	}

	log.Printf("Account event published, %d bytes", len(payload))
	return nil
}

func (p *KafkaProducer) Close() {
	if p.writer != nil {
		p.writer.Close()
	}
}
