package service

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/aegis-banking/ledger-core/internal/pb"
	"github.com/aegis-banking/ledger-core/internal/queue"
	"github.com/aegis-banking/ledger-core/internal/repository"
)
type LedgerService struct {
	pb.UnimplementedLedgerServiceServer
	repo 	 *repository.AccountRepository
	producer *queue.RabbitMQProducer
}

func NewLedgerService(repo *repository.AccountRepository, producer *queue.RabbitMQProducer) *LedgerService {
	return &LedgerService{
		repo: 	  repo,
		producer: producer,
	}
}

func (s *LedgerService) ExecuteTransfer(ctx context.Context, req *pb.TransferRequest) (*pb.TransferResponse, error) {
	if req.Amount <= 0 {
		return &pb.TransferResponse{
			Success:       false,
			Message:       "amount must be greater than zero",
			TransactionId: req.GetTransactionId(),
		}, nil
	}

	txnID := req.GetTransactionId()
	if txnID == "" {
		txnID = fmt.Sprintf("txn_%d", time.Now().UnixNano())
	}

	err := s.repo.ExecuteTransfer(ctx, txnID, req.GetFromAccount(), req.GetToAccount(), req.GetAmount(), req.GetDeviceId(), req.GetIpAddress(), req.GetUserAgent())
	if err != nil {
		log.Printf("Transfer failed: %v", err)
		return &pb.TransferResponse{
			Success:       false,
			Message:       err.Error(),
			TransactionId: txnID,
		}, nil
	}

	// Publish to RabbitMQ for Logstash → Elasticsearch
	if s.producer != nil {
		if pubErr := s.producer.PublishAudit(txnID, req.GetFromAccount(), req.GetToAccount(), req.GetAmount()); pubErr != nil {
			log.Printf("Warning: Failed to publish audit event: %v", pubErr)
		}
	}

	log.Printf("✅ Transfer successful: %s → %s | ₹%.2f | txn=%s", req.GetFromAccount(), req.GetToAccount(), req.GetAmount(), txnID)

	return &pb.TransferResponse{
		Success:       true,
		Message:       "Transfer completed successfully",
		TransactionId: txnID,
	}, nil
}