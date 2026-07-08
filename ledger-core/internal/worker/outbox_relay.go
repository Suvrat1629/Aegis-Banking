package worker

import (
    "context"
    "database/sql"
    "encoding/json"
    "log"
    "time"

    "github.com/aegis-banking/ledger-core/internal/observability"
    "github.com/aegis-banking/ledger-core/internal/queue"
)

type OutboxRelay struct {
    db         *sql.DB
    publisher  *queue.KafkaProducer
    maxRetries int
}

func NewOutboxRelay(db *sql.DB, publisher *queue.KafkaProducer) *OutboxRelay {
    return &OutboxRelay{
        db:         db,
        publisher:  publisher,
        maxRetries: 10,
    }
}

func (r *OutboxRelay) Start(ctx context.Context) {
    ticker := time.NewTicker(3 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            log.Println("OutboxRelay: context cancelled, stopping")
            return
        case <-ticker.C:
            r.processBatch(ctx)
        }
    }
}

func (r *OutboxRelay) processBatch(ctx context.Context) {
    tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
    if err != nil {
        log.Printf("OutboxRelay: begin tx failed: %v", err)
        return
    }
    defer tx.Rollback()

    rows, err := tx.QueryContext(ctx, `
        SELECT id, payload, retry_count, event_type, aggregate_type, aggregate_id
        FROM outbox
        WHERE status = 'PENDING'
          AND next_attempt_at <= NOW()
        ORDER BY created_at ASC
        LIMIT 10
        FOR UPDATE SKIP LOCKED`)
    if err != nil {
        _ = tx.Rollback()
        log.Printf("OutboxRelay: select failed: %v", err)
        return
    }

    type outboxRow struct {
        id            string
        payload       []byte
        retryCount    int
        eventType     string
        aggregateType string
        aggregateID   string
    }

    var batch []outboxRow
    for rows.Next() {
        var id string
        var payload []byte
        var retryCount int
        var eventType, aggregateType, aggregateID string

        if err := rows.Scan(&id, &payload, &retryCount, &eventType, &aggregateType, &aggregateID); err != nil {
            log.Printf("OutboxRelay: row scan failed: %v", err)
            continue
        }
        batch = append(batch, outboxRow{
            id: id, payload: payload, retryCount: retryCount,
            eventType: eventType, aggregateType: aggregateType, aggregateID: aggregateID,
        })
    }

    // Close rows before executing additional queries on the same transaction/connection
    rows.Close()

    for _, item := range batch {
        id := item.id
        retryCount := item.retryCount

        var payloadFields map[string]any
        if err := json.Unmarshal(item.payload, &payloadFields); err != nil {
            log.Printf("OutboxRelay: failed to parse payload for id=%s: %v", id, err)
            continue
        }
        payloadFields["event_type"] = item.eventType
        payloadFields["aggregate_type"] = item.aggregateType
        payloadFields["aggregate_id"] = item.aggregateID

        enrichedPayload, err := json.Marshal(payloadFields)
        if err != nil {
            log.Printf("OutboxRelay: failed to build enriched payload for id=%s: %v", id, err)
            continue
        }

        err = r.publisher.PublishAuditFromPayload(enrichedPayload)
        if err == nil {
            observability.KafkaPublishTotal.Inc()
            if _, err := tx.ExecContext(ctx, `
                UPDATE outbox
                SET status = 'PROCESSED', processed_at = NOW()
                WHERE id = $1`, id); err != nil {
                log.Printf("OutboxRelay: failed to mark processed id=%s: %v", id, err)
            }
        } else {
            observability.KafkaPublishFailures.Inc()
            log.Printf("OutboxRelay: publish failed for id=%s: %v", id, err)
            if retryCount >= r.maxRetries {
                if _, err := tx.ExecContext(ctx, `UPDATE outbox SET status = 'FAILED' WHERE id = $1`, id); err != nil {
                    log.Printf("OutboxRelay: failed to mark failed id=%s: %v", id, err)
                }
            } else {
                backoffSecs := (retryCount + 1) * 10
                if _, err := tx.ExecContext(ctx, `
                    UPDATE outbox
                    SET retry_count = retry_count + 1,
                        next_attempt_at = NOW() + make_interval(secs => $2::int)
                    WHERE id = $1`, id, backoffSecs); err != nil {
                    log.Printf("OutboxRelay: failed to update retry for id=%s: %v", id, err)
                }
            }
        }
    }

    if err := tx.Commit(); err != nil {
        log.Printf("OutboxRelay: commit failed: %v", err)
    }
}