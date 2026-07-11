package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	FraudChecksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "aegis_fraud_checks_total",
		Help: "Total synchronous fraud checks performed, by verdict",
	}, []string{"verdict"})

	FraudRescoredTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "aegis_fraud_rescored_total",
		Help: "Total completed transfers re-scored asynchronously",
	})

	FraudReversalsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "aegis_fraud_reversals_total",
		Help: "Total compensating reversal transactions attempted, by status",
	}, []string{"status"})
)
