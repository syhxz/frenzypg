package server

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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
	ID              string
	TransactionState TransactionState
	PrimaryConn     *pgxpool.Conn
	MirrorConns     []*pgxpool.Conn
	LastUsed        time.Time
	mutex           sync.RWMutex
	logger          *zap.Logger
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
	if strings.HasPrefix(trimmed, "ROLLBACK") {
		return true, "ROLLBACK"
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

// SetPrimaryConnection sets the dedicated primary connection for this session
func (s *Session) SetPrimaryConnection(conn *pgxpool.Conn) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.PrimaryConn = conn
}

// SetMirrorConnections sets the dedicated mirror connections for this session
func (s *Session) SetMirrorConnections(conns []*pgxpool.Conn) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.MirrorConns = conns
}

// Cleanup releases all connections associated with this session
func (s *Session) Cleanup() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.PrimaryConn != nil {
		// If in transaction, rollback before releasing
		if s.TransactionState == TransactionInProgress {
			s.logger.Warn("Rolling back uncommitted transaction on session cleanup", zap.String("session", s.ID))
			_, err := s.PrimaryConn.Exec(context.Background(), "ROLLBACK")
			if err != nil {
				s.logger.Error("Failed to rollback transaction during cleanup", zap.Error(err))
			}
		}
		s.PrimaryConn.Release()
		s.PrimaryConn = nil
	}

	for _, mirrorConn := range s.MirrorConns {
		if mirrorConn != nil {
			if s.TransactionState == TransactionInProgress {
				_, err := mirrorConn.Exec(context.Background(), "ROLLBACK")
				if err != nil {
					s.logger.Error("Failed to rollback mirror transaction during cleanup", zap.Error(err))
				}
			}
			mirrorConn.Release()
		}
	}
	s.MirrorConns = nil
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
