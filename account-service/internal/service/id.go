package service

import (
	"crypto/rand"
	"encoding/hex"
)

// generateAccountID mirrors the format the DB schema itself used to default to
// ('acc_' || substr(gen_random_uuid()::text, 1, 8)) — generated here instead of
// left to a DB default because the caller needs the ID before the local insert
// happens (it's passed to ledger-core first). See CreateAccount's ordering note.
func generateAccountID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return "acc_" + hex.EncodeToString(b)
}
