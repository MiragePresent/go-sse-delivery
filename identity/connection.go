package identity

import (
	"crypto/rand"
	"fmt"
	"net/http"
)

// ConnectionIDResolver extracts a connection ID from an HTTP request.
// Returns empty string if no connection ID is present.
type ConnectionIDResolver func(*http.Request) string

// ConnectionIDGenerator generates a new unique connection ID.
type ConnectionIDGenerator func() string

// DefaultConnectionIDResolver reads connection ID from connectionId query parameter.
func DefaultConnectionIDResolver(r *http.Request) string {
	return r.URL.Query().Get("connectionId")
}

// DefaultConnectionIDGenerator generates a UUID v4 connection ID.
func DefaultConnectionIDGenerator() string {
	b := make([]byte, 16)
	rand.Read(b)

	// Set version (4) and variant (RFC 4122) bits
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC 4122

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
