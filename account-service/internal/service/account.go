package service

import (
	"context"
	"log"

	"github.com/aegis-banking/account-service/internal/grpcclient"
	"github.com/aegis-banking/account-service/internal/observability"
	accountpb "github.com/aegis-banking/account-service/internal/pb/account"
	"github.com/aegis-banking/account-service/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AccountService struct {
	accountpb.UnimplementedAccountServiceServer
	repo         *repository.AccountRepository
	ledgerClient *grpcclient.LedgerClient
}

func NewAccountService(repo *repository.AccountRepository, ledgerClient *grpcclient.LedgerClient) *AccountService {
	return &AccountService{
		repo:         repo,
		ledgerClient: ledgerClient,
	}
}

// CreateAccount registers the account with ledger-core FIRST, then commits the
// local identity record + outbox event only if that succeeds. This ordering is
// deliberate: it avoids ever having a local account-service record that points
// at a ledger-core account that doesn't exist. See the plan's "ordering note"
// for the one edge case this doesn't fully close (a crash between the two
// steps) and why it's an acceptable simplification for this phase.
func (s *AccountService) CreateAccount(ctx context.Context, req *accountpb.CreateAccountRequest) (*accountpb.CreateAccountResponse, error) {
	if req == nil || req.GetOwnerName() == "" {
		return nil, status.Error(codes.InvalidArgument, "owner_name is required")
	}

	// We need an account_id before calling ledger-core, but the DB generates it
	// on insert. Resolve this by inserting locally first inside its own
	// transaction is not an option (that's the ordering we're avoiding) — so we
	// generate the account_id here in the service layer instead of leaving it to
	// the DB default, and pass it explicitly to both ledger-core and the insert.
	accountID := generateAccountID()

	if err := s.ledgerClient.RegisterAccount(ctx, accountID, req.GetOwnerName()); err != nil {
		observability.AccountCreationFailuresTotal.Inc()
		log.Printf("CreateAccount: ledger-core registration failed: %v", err)
		return nil, status.Error(codes.Internal, "failed to register account with ledger")
	}

	if err := s.repo.CreateAccount(ctx, accountID, req.GetOwnerName(), req.GetEmail(), req.GetPhone()); err != nil {
		observability.AccountCreationFailuresTotal.Inc()
		log.Printf("CreateAccount: local persist failed after ledger-core succeeded (account_id=%s): %v", accountID, err)
		return nil, status.Error(codes.Internal, "failed to persist account")
	}

	observability.AccountsCreatedTotal.Inc()
	log.Printf("✅ Account created: id=%s owner=%s", accountID, req.GetOwnerName())

	return &accountpb.CreateAccountResponse{
		AccountId: accountID,
		OwnerName: req.GetOwnerName(),
		Email:     req.GetEmail(),
		Phone:     req.GetPhone(),
	}, nil
}

func (s *AccountService) GetAccount(ctx context.Context, req *accountpb.GetAccountRequest) (*accountpb.GetAccountResponse, error) {
	if req == nil || req.GetAccountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}

	ownerName, email, phone, err := s.repo.GetAccount(ctx, req.GetAccountId())
	if err != nil {
		return nil, status.Error(codes.NotFound, "account not found")
	}

	return &accountpb.GetAccountResponse{
		AccountId: req.GetAccountId(),
		OwnerName: ownerName,
		Email:     email,
		Phone:     phone,
	}, nil
}
