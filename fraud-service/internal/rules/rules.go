package rules

const (
	// SyncAmountThreshold blocks any single transfer above this amount outright.
	// This is the simplest possible rule: fast, deterministic, no history needed.
	SyncAmountThreshold = 10000.0

	// SyncVelocityLimit blocks a transfer if the source account has already made
	// this many transfers within SyncVelocityWindowSeconds.
	SyncVelocityLimit         = 5
	SyncVelocityWindowSeconds = 60

	// AsyncWindowAmountThreshold flags an account (post-settlement) if its
	// cumulative outgoing amount within AsyncWindowSeconds crosses this total.
	AsyncWindowAmountThreshold = 20000.0
	AsyncWindowSeconds         = 600
)

// Verdict values returned by CheckTransfer.
const (
	VerdictAllow = "ALLOW"
	VerdictBlock = "BLOCK"
)

const (
	ReasonAmountOverThreshold = "amount_over_threshold"
	ReasonVelocity            = "velocity"
)

type SyncCheckInput struct {
	Amount              float64
	RecentTransferCount int // count of transfers already made by from_account in the velocity window
}

// SyncCheckResult is the outcome of evaluating the synchronous rules.
type SyncCheckResult struct {
	Verdict string
	Reason  string
}

func EvaluateSync(in SyncCheckInput) SyncCheckResult {
	if in.Amount > SyncAmountThreshold {
		return SyncCheckResult{Verdict: VerdictBlock, Reason: ReasonAmountOverThreshold}
	}
	if in.RecentTransferCount >= SyncVelocityLimit {
		return SyncCheckResult{Verdict: VerdictBlock, Reason: ReasonVelocity}
	}
	return SyncCheckResult{Verdict: VerdictAllow, Reason: ""}
}

func EvaluateAsync(cumulativeAmount float64) bool {
	return cumulativeAmount > AsyncWindowAmountThreshold
}
