package service

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aegis-banking/ledger-core/internal/observability"
	pb "github.com/aegis-banking/ledger-core/internal/pb"
	"github.com/aegis-banking/ledger-core/internal/queue"
	"github.com/aegis-banking/ledger-core/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)
type LedgerService struct {
	pb.UnimplementedLedgerServiceServer
	repo 	 *repository.AccountRepository
	producer *queue.KafkaProducer
}

func NewLedgerService(repo *repository.AccountRepository, producer *queue.KafkaProducer) *LedgerService {
	return &LedgerService{
		repo: 	  repo,
		producer: producer,
	}
}

func (s *LedgerService) ExecuteTransfer(ctx context.Context, req *pb.TransferRequest) (*pb.TransferResponse, error) {
	observability.TransferRequestsTotal.Inc()
	start := time.Now()
	defer func() {
		observability.TransferDuration.Observe(time.Since(start).Seconds())
	}()

	if req.Amount <= 0 {
		observability.TransferFailureTotal.WithLabelValues("invalid_amount").Inc()
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
		observability.TransferFailureTotal.WithLabelValues(classifyTransferError(err)).Inc()
		return &pb.TransferResponse{
			Success:       false,
			Message:       err.Error(),
			TransactionId: txnID,
		}, nil
	}

	// Publish to Kafka for Logstash → Elasticsearch
	if s.producer != nil {
		if pubErr := s.producer.PublishAudit(txnID, req.GetFromAccount(), req.GetToAccount(), req.GetAmount()); pubErr != nil {
			log.Printf("Warning: Failed to publish audit event: %v", pubErr)
		}
	}

	log.Printf("✅ Transfer successful: %s → %s | ₹%.2f | txn=%s", req.GetFromAccount(), req.GetToAccount(), req.GetAmount(), txnID)

	observability.TransferSuccessTotal.Inc()

	return &pb.TransferResponse{
		Success:       true,
		Message:       "Transfer completed successfully",
		TransactionId: txnID,
	}, nil
}

// classifyTransferError buckets repository errors into a small, bounded set of
// Prometheus label values. The raw error text (which embeds account IDs/amounts)
// must never be used as a label value directly — that would blow up cardinality.
func classifyTransferError(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "insufficient balance"):
		return "insufficient_balance"
	case strings.Contains(msg, "account not found"):
		return "account_not_found"
	default:
		return "internal_error"
	}
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

// RegisterAccount is called by account-service, as part of customer creation, to
// create the ledger-side row that can hold a balance. Idempotent (see
// AccountRepository.RegisterAccount) so it's safe for account-service to retry.
func (s *LedgerService) RegisterAccount(ctx context.Context, req *pb.RegisterAccountRequest) (*pb.RegisterAccountResponse, error) {
	if req == nil || req.GetAccountId() == "" || req.GetOwnerName() == "" {
		return nil, status.Error(codes.InvalidArgument, "account_id and owner_name are required")
	}

	if err := s.repo.RegisterAccount(ctx, req.GetAccountId(), req.GetOwnerName()); err != nil {
		log.Printf("RegisterAccount failed: %v", err)
		return nil, status.Error(codes.Internal, "failed to register account")
	}

	return &pb.RegisterAccountResponse{
		Success: true,
		Message: "account registered",
	}, nil
}