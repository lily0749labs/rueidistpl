package rueidistpl

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func newOwnershipToken() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("create ownership token: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}
