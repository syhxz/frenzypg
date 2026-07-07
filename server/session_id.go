package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
)

// sessionIDAttrKey is the key used to store the stable session ID in wire session attributes
const sessionIDAttrKey = "frenzy_session_id"

var globalSessionCounter uint64

// generateSessionID creates a unique session ID
func generateSessionID() string {
	id := atomic.AddUint64(&globalSessionCounter, 1)
	return fmt.Sprintf("session_%d_%d", time.Now().UnixNano(), id)
}

// getOrCreateSessionID retrieves or creates a stable session ID from the psql-wire session context.
// This ensures that all queries within the same client connection share the same session ID.
func getOrCreateSessionID(ctx context.Context) string {
	session, ok := wire.GetSession(ctx)
	if !ok || session == nil {
		// Fallback: generate a new ID (should not happen in normal flow)
		return generateSessionID()
	}

	// Check if we already have a session ID stored
	if existingID, exists := session.Attributes[sessionIDAttrKey]; exists {
		if id, ok := existingID.(string); ok && id != "" {
			return id
		}
	}

	// Generate and store a new session ID
	newID := generateSessionID()
	session.Attributes[sessionIDAttrKey] = newID
	return newID
}
