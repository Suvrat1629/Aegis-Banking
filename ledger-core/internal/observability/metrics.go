package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	TransferRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "aegis_ledger_transfer_requests_total",
		Help: "Total number of transfer requests received",
	})

	TransferSuccessTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "aegis_ledger_transfer_success_total",
		Help: "Total number of successful transfers",
	})

	TransferFailureTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "aegis_ledger_transfer_failure_total",
		Help: "Total number of failed transfers",
	}, []string{"reason"})

	TransferDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "aegis_ledger_transfer_duration_seconds",
		Help:    "Time spent processing transfers",
		Buckets: prometheus.DefBuckets,
	})

	DatabaseConnectionPoolSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "aegis_ledger_db_pool_size",
		Help: "Current database connection pool size",
	})

	KafkaPublishTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "aegis_ledger_kafka_publish_total",
		Help: "Total messages published to Kafka",
	})

	KafkaPublishFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "aegis_ledger_kafka_publish_failures_total",
		Help: "Failed Kafka publishes",
	})
)