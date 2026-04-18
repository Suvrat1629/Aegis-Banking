package service

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	pb "github.com/aegis-banking/ledger-core/internal/pb"
	"github.com/aegis-banking/ledger-core/internal/queue"
	"github.com/aegis-banking/ledger-core/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func (s *LedgerService) GetAccountBalance(ctx context.Context, req *pb.BalanceRequest) (*pb.BalanceResponse, error) {
	if req == nil || req.GetAccountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}

	bal, owner, lastUpdated, err := s.repo.GetBalance(ctx, req.GetAccountId())
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Error(codes.NotFound, "account not found")
		}
		return nil, status.Error(codes.Internal, "failed to fetch balance")
	}

	return &pb.BalanceResponse{
		AccountId:   req.GetAccountId(),
		OwnerName:   owner,
		Balance:     bal,
		LastUpdated: lastUpdated,
	}, nil
}

func (s *LedgerService) GetAccountHistory(ctx context.Context, req *pb.HistoryRequest) (*pb.HistoryResponse, error) {
	if req == nil || req.GetAccountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}

	limit := req.GetLimit()
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	offset := req.GetOffset()
	if offset < 0 {
		offset = 0
	}

	entries, err := s.repo.GetHistory(ctx, req.GetAccountId(), limit, offset)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to fetch history")
	}
	return &pb.HistoryResponse{Entries: entries}, nil
}