package service

import (
	"context"
	"log"

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
			Success: false,
			Message: "amount must be greater than zero",
		}, nil
	}
	
	err := s.repo.ExecuteTransfer(ctx, req.FromAccount, req.ToAccount, req.Amount)
	if err != nil {
		log.Printf("Transfer failed: %v", err)
		return &pb.TransferResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	if s.producer != nil {
		if pubErr := s.producer.PublishAudit(req.FromAccount, req.ToAccount, req.Amount); pubErr != nil {
			log.Printf("Warning: Failed to publish audit event: %v", pubErr)
		}
	}

	log.Printf("Transfer completed: %s → %s | ₹%.2f", req.FromAccount, req.ToAccount, req.Amount)

	return &pb.TransferResponse{
		Success: true,
		Message: "Transfer completed successfully",
	}, nil
}