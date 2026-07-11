package service

import (
	"context"
	"log"

	"github.com/aegis-banking/fraud-service/internal/observability"
	pb "github.com/aegis-banking/fraud-service/internal/pb/fraud"
	"github.com/aegis-banking/fraud-service/internal/repository"
	"github.com/aegis-banking/fraud-service/internal/rules"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FraudService struct {
	pb.UnimplementedFraudServiceServer
	checks *repository.ChecksRepository
}

func NewFraudService(checks *repository.ChecksRepository) *FraudService {
	return &FraudService{checks: checks}
}

func (s *FraudService) CheckTransfer(ctx context.Context, req *pb.FraudCheckRequest) (*pb.FraudCheckResponse, error) {
	if req == nil || req.GetFromAccount() == "" || req.GetToAccount() == "" {
		return nil, status.Error(codes.InvalidArgument, "from_account and to_account are required")
	}

	recentCount, err := s.checks.RecentTransferCount(ctx, req.GetFromAccount(), rules.SyncVelocityWindowSeconds)
	if err != nil {
		log.Printf("CheckTransfer: failed to read recent transfer count for %s: %v (failing open)", req.GetFromAccount(), err)
		recentCount = 0
	}

	result := rules.EvaluateSync(rules.SyncCheckInput{
		Amount:              req.GetAmount(),
		RecentTransferCount: recentCount,
	})

	observability.FraudChecksTotal.WithLabelValues(result.Verdict).Inc()

	if recErr := s.checks.RecordCheck(ctx, req.GetTransactionId(), req.GetFromAccount(), req.GetToAccount(), req.GetAmount(), result.Verdict, result.Reason); recErr != nil {
		log.Printf("CheckTransfer: failed to record check for txn=%s: %v", req.GetTransactionId(), recErr)
	}

	if result.Verdict == rules.VerdictBlock {
		log.Printf("Fraud check BLOCK: txn=%s from=%s amount=%.2f reason=%s", req.GetTransactionId(), req.GetFromAccount(), req.GetAmount(), result.Reason)
	}

	score := 0.0
	if result.Verdict == rules.VerdictBlock {
		score = 1.0
	}

	return &pb.FraudCheckResponse{
		Verdict: result.Verdict,
		Score:   score,
		Reason:  result.Reason,
	}, nil
}
