package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	AccountsCreatedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "aegis_account_created_total",
		Help: "Total number of accounts successfully created",
	})

	AccountCreationFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "aegis_account_creation_failures_total",
		Help: "Total number of failed account creation attempts",
	})

	KafkaPublishTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "aegis_account_kafka_publish_total",
		Help: "Total messages published to Kafka",
	})

	KafkaPublishFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "aegis_account_kafka_publish_failures_total",
		Help: "Failed Kafka publishes",
	})
)
