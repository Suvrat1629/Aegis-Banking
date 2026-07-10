package grpcclient

import (
	"context"
	"fmt"

	ledgerpb "github.com/aegis-banking/account-service/internal/pb/ledger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type LedgerClient struct {
	conn   *grpc.ClientConn
	client ledgerpb.LedgerServiceClient
}

func NewLedgerClient(addr string) (*LedgerClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial ledger-core at %s: %w", addr, err)
	}

	return &LedgerClient{
		conn:   conn,
		client: ledgerpb.NewLedgerServiceClient(conn),
	}, nil
}

func (c *LedgerClient) RegisterAccount(ctx context.Context, accountID, ownerName string) error {
	resp, err := c.client.RegisterAccount(ctx, &ledgerpb.RegisterAccountRequest{
		AccountId: accountID,
		OwnerName: ownerName,
	})
	if err != nil {
		return fmt.Errorf("ledger-core RegisterAccount call failed: %w", err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("ledger-core RegisterAccount rejected: %s", resp.GetMessage())
	}
	return nil
}

func (c *LedgerClient) Close() error {
	return c.conn.Close()
}
