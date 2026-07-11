package rules

import "testing"

func TestEvaluateSync_AllowsNormalTransfer(t *testing.T) {
	result := EvaluateSync(SyncCheckInput{Amount: 500, RecentTransferCount: 0})
	if result.Verdict != VerdictAllow {
		t.Fatalf("expected ALLOW, got %s (reason=%s)", result.Verdict, result.Reason)
	}
}

func TestEvaluateSync_BlocksOverThresholdAmount(t *testing.T) {
	result := EvaluateSync(SyncCheckInput{Amount: SyncAmountThreshold + 0.01, RecentTransferCount: 0})
	if result.Verdict != VerdictBlock || result.Reason != ReasonAmountOverThreshold {
		t.Fatalf("expected BLOCK/%s, got %s/%s", ReasonAmountOverThreshold, result.Verdict, result.Reason)
	}
}

func TestEvaluateSync_AllowsExactlyAtThreshold(t *testing.T) {
	// Boundary check: threshold itself should still be allowed ("over", not "at or over").
	result := EvaluateSync(SyncCheckInput{Amount: SyncAmountThreshold, RecentTransferCount: 0})
	if result.Verdict != VerdictAllow {
		t.Fatalf("expected ALLOW at exact threshold, got %s", result.Verdict)
	}
}

func TestEvaluateSync_BlocksOnVelocity(t *testing.T) {
	result := EvaluateSync(SyncCheckInput{Amount: 100, RecentTransferCount: SyncVelocityLimit})
	if result.Verdict != VerdictBlock || result.Reason != ReasonVelocity {
		t.Fatalf("expected BLOCK/%s, got %s/%s", ReasonVelocity, result.Verdict, result.Reason)
	}
}

func TestEvaluateSync_AllowsJustBelowVelocityLimit(t *testing.T) {
	result := EvaluateSync(SyncCheckInput{Amount: 100, RecentTransferCount: SyncVelocityLimit - 1})
	if result.Verdict != VerdictAllow {
		t.Fatalf("expected ALLOW just below velocity limit, got %s", result.Verdict)
	}
}

func TestEvaluateSync_AmountRuleTakesPrecedenceOverVelocity(t *testing.T) {
	// Both conditions true at once: amount reason should win since it's checked first.
	result := EvaluateSync(SyncCheckInput{Amount: SyncAmountThreshold + 1, RecentTransferCount: SyncVelocityLimit})
	if result.Reason != ReasonAmountOverThreshold {
		t.Fatalf("expected amount reason to take precedence, got %s", result.Reason)
	}
}

func TestEvaluateAsync_FlagsOverThreshold(t *testing.T) {
	if !EvaluateAsync(AsyncWindowAmountThreshold + 0.01) {
		t.Fatal("expected flag when cumulative amount exceeds threshold")
	}
}

func TestEvaluateAsync_DoesNotFlagAtOrBelowThreshold(t *testing.T) {
	if EvaluateAsync(AsyncWindowAmountThreshold) {
		t.Fatal("did not expect flag at exact threshold")
	}
}
