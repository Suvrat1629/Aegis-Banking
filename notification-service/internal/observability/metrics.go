package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	NotificationsSentTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "aegis_notification_sent_total",
		Help: "Total number of notifications successfully sent",
	})

	NotificationsSkippedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "aegis_notification_skipped_total",
		Help: "Total number of notifications skipped due to missing contact info",
	})

	NotificationsFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "aegis_notification_failed_total",
		Help: "Total number of notification send failures",
	})
)
