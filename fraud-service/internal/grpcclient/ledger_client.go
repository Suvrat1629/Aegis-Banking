package grpcclient

import (
	"context"
	"fmt"

	ledgerpb "github.com/aegis-banking/fraud-service/internal/pb/ledger"
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

func (c *LedgerClient) ExecuteTransfer(ctx context.Context, reversalTxnID, from, to string, amount float64, referenceTxnID string) error {
	resp, err := c.client.ExecuteTransfer(ctx, &ledgerpb.TransferRequest{
		TransactionId:          reversalTxnID,
		FromAccount:            from,
		ToAccount:              to,
		Amount:                 amount,
		ReferenceTransactionId: referenceTxnID,
	})
	if err != nil {
		return fmt.Errorf("ledger-core ExecuteTransfer call failed: %w", err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("ledger-core ExecuteTransfer rejected: %s", resp.GetMessage())
	}
	return nil
}

func (c *LedgerClient) Close() error {
	return c.conn.Close()
}
