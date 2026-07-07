package server

import (
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// TransactionState represents the current transaction state
type TransactionState int

const (
	TransactionIdle TransactionState = iota
	TransactionInProgress
	TransactionFailed
)

// Session represents a client session with transaction state
type Session struct {
	ID               string
	TransactionState TransactionState
	LastUsed         time.Time
	mutex            sync.RWMutex
	logger           *zap.Logger
}

// SessionManager manages client sessions and their transaction states
type SessionManager struct {
	sessions      map[string]*Session
	mutex         sync.RWMutex
	logger        *zap.Logger
	cleanupTicker *time.Ticker
	stopCleanup   chan bool
}

func NewSessionManager(logger *zap.Logger) *SessionManager {
	sm := &SessionManager{
		sessions:      make(map[string]*Session),
		logger:        logger,
		cleanupTicker: time.NewTicker(5 * time.Minute),
		stopCleanup:   make(chan bool),
	}
	
	// Start cleanup goroutine
	go sm.cleanupExpiredSessions()
	
	return sm
}

func (sm *SessionManager) GetOrCreateSession(sessionID string) *Session {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if session, exists := sm.sessions[sessionID]; exists {
		session.LastUsed = time.Now()
		return session
	}

	session := &Session{
		ID:               sessionID,
		TransactionState: TransactionIdle,
		LastUsed:         time.Now(),
		logger:           sm.logger,
	}
	sm.sessions[sessionID] = session
	return session
}

func (sm *SessionManager) RemoveSession(sessionID string) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	
	if session, exists := sm.sessions[sessionID]; exists {
		session.Cleanup()
		delete(sm.sessions, sessionID)
	}
}

// IsTransactionCommand checks if the query is a transaction command
func IsTransactionCommand(query string) (bool, string) {
	trimmed := strings.TrimSpace(strings.ToUpper(query))
	
	if strings.HasPrefix(trimmed, "BEGIN") || strings.HasPrefix(trimmed, "START TRANSACTION") {
		return true, "BEGIN"
	}
	if strings.HasPrefix(trimmed, "COMMIT") || trimmed == "END" {
		return true, "COMMIT"
	}
	if strings.HasPrefix(trimmed, "ROLLBACK TO SAVEPOINT") || strings.HasPrefix(trimmed, "ROLLBACK TO ") {
		// ROLLBACK TO SAVEPOINT does NOT end the transaction — it's a savepoint operation
		return true, "ROLLBACK_TO_SAVEPOINT"
	}
	if strings.HasPrefix(trimmed, "ROLLBACK") {
		return true, "ROLLBACK"
	}
	if strings.HasPrefix(trimmed, "SAVEPOINT ") {
		return true, "SAVEPOINT"
	}
	if strings.HasPrefix(trimmed, "RELEASE SAVEPOINT") || strings.HasPrefix(trimmed, "RELEASE ") {
		return true, "RELEASE_SAVEPOINT"
	}
	
	return false, ""
}

// UpdateTransactionState updates the session's transaction state based on the command
func (s *Session) UpdateTransactionState(command string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	switch command {
	case "BEGIN":
		s.TransactionState = TransactionInProgress
		s.logger.Debug("Transaction started", zap.String("session", s.ID))
	case "COMMIT":
		s.TransactionState = TransactionIdle
		s.logger.Debug("Transaction committed", zap.String("session", s.ID))
	case "ROLLBACK":
		s.TransactionState = TransactionIdle
		s.logger.Debug("Transaction rolled back", zap.String("session", s.ID))
	}
}

// IsInTransaction returns true if the session is currently in a transaction
func (s *Session) IsInTransaction() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.TransactionState == TransactionInProgress
}

// Cleanup resets session state
func (s *Session) Cleanup() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.TransactionState = TransactionIdle
}

// Close stops the cleanup goroutine and cleans up all sessions
func (sm *SessionManager) Close() {
	if sm.stopCleanup != nil {
		close(sm.stopCleanup)
	}
	if sm.cleanupTicker != nil {
		sm.cleanupTicker.Stop()
	}

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	// Clean up all sessions
	for sessionID, session := range sm.sessions {
		session.Cleanup()
		delete(sm.sessions, sessionID)
	}
}

// cleanupExpiredSessions runs periodically to clean up expired sessions
func (sm *SessionManager) cleanupExpiredSessions() {
	for {
		select {
		case <-sm.cleanupTicker.C:
			sm.performCleanup()
		case <-sm.stopCleanup:
			return
		}
	}
}

// performCleanup removes expired sessions that are not in active transactions
func (sm *SessionManager) performCleanup() {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	now := time.Now()
	expiredSessions := make([]string, 0)

	for sessionID, session := range sm.sessions {
		// Clean up sessions idle for more than 30 minutes
		if now.Sub(session.LastUsed) > 30*time.Minute {
			// Only clean up if not in active transaction
			if session.TransactionState != TransactionInProgress {
				expiredSessions = append(expiredSessions, sessionID)
			}
		}
	}

	cleanedCount := 0
	for _, sessionID := range expiredSessions {
		if session, exists := sm.sessions[sessionID]; exists {
			session.Cleanup()
			delete(sm.sessions, sessionID)
			cleanedCount++
		}
	}

	if cleanedCount > 0 {
		sm.logger.Info("Cleaned up expired sessions", zap.Int("cleaned_sessions", cleanedCount))
	}
}
