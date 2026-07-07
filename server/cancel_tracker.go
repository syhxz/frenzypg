package server

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

// MirrorCancelTracker tracks active mirror operations per session so they can be
// cancelled when the client cancels a query.
type MirrorCancelTracker struct {
	activeCancels map[string][]cancelEntry
	nextID        uint64
	mutex         sync.Mutex
	logger        *zap.Logger
}

type cancelEntry struct {
	id     uint64
	cancel context.CancelFunc
}

func NewMirrorCancelTracker(logger *zap.Logger) *MirrorCancelTracker {
	return &MirrorCancelTracker{
		activeCancels: make(map[string][]cancelEntry),
		logger:        logger,
	}
}

// Register registers a cancel function for a session's mirror operations.
// Returns an ID that can be used to remove this specific registration.
func (mct *MirrorCancelTracker) Register(sessionID string, cancel context.CancelFunc) uint64 {
	mct.mutex.Lock()
	defer mct.mutex.Unlock()
	mct.nextID++
	id := mct.nextID
	mct.activeCancels[sessionID] = append(mct.activeCancels[sessionID], cancelEntry{id: id, cancel: cancel})
	return id
}

// CancelAll cancels all active mirror operations for a session.
func (mct *MirrorCancelTracker) CancelAll(sessionID string) {
	mct.mutex.Lock()
	defer mct.mutex.Unlock()

	if entries, exists := mct.activeCancels[sessionID]; exists {
		for _, entry := range entries {
			entry.cancel()
		}
		delete(mct.activeCancels, sessionID)
		mct.logger.Debug("Cancelled mirror operations for session",
			zap.String("session", sessionID),
			zap.Int("cancelled", len(entries)))
	}
}

// Remove removes a specific cancel registration by ID (called when mirror operation completes normally).
func (mct *MirrorCancelTracker) Remove(sessionID string, id uint64) {
	mct.mutex.Lock()
	defer mct.mutex.Unlock()

	entries := mct.activeCancels[sessionID]
	for i, entry := range entries {
		if entry.id == id {
			mct.activeCancels[sessionID] = append(entries[:i], entries[i+1:]...)
			break
		}
	}

	// Clean up empty entries
	if len(mct.activeCancels[sessionID]) == 0 {
		delete(mct.activeCancels, sessionID)
	}
}

// Cleanup removes all entries for a session (called on session disconnect).
func (mct *MirrorCancelTracker) Cleanup(sessionID string) {
	mct.CancelAll(sessionID)
}
