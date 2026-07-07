package server

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// CircuitState represents the state of a circuit breaker.
type CircuitState int32

const (
	CircuitClosed   CircuitState = iota // Normal operation
	CircuitOpen                         // Blocking requests
	CircuitHalfOpen                     // Testing if recovered
)

// CircuitBreaker implements a simple circuit breaker for mirror connections.
type CircuitBreaker struct {
	state           atomic.Int32
	failureCount    atomic.Int64
	successCount    atomic.Int64
	lastFailureTime atomic.Int64 // UnixNano

	// Configuration
	failureThreshold int64         // Number of failures before opening
	recoveryTimeout  time.Duration // Time to wait before half-open
	successThreshold int64         // Successes needed in half-open to close

	name   string
	logger *zap.Logger
}

func NewCircuitBreaker(name string, failureThreshold int64, recoveryTimeout time.Duration, logger *zap.Logger) *CircuitBreaker {
	cb := &CircuitBreaker{
		failureThreshold: failureThreshold,
		recoveryTimeout:  recoveryTimeout,
		successThreshold: 3,
		name:             name,
		logger:           logger,
	}
	cb.state.Store(int32(CircuitClosed))
	return cb
}

// Allow checks if a request should be allowed through the circuit breaker.
func (cb *CircuitBreaker) Allow() bool {
	state := CircuitState(cb.state.Load())

	switch state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		// Check if recovery timeout has elapsed
		lastFail := time.Unix(0, cb.lastFailureTime.Load())
		if time.Since(lastFail) > cb.recoveryTimeout {
			// Transition to half-open
			if cb.state.CompareAndSwap(int32(CircuitOpen), int32(CircuitHalfOpen)) {
				cb.successCount.Store(0)
				cb.logger.Info("Circuit breaker half-open, testing recovery",
					zap.String("mirror", cb.name))
			}
			return true
		}
		return false
	case CircuitHalfOpen:
		return true
	}
	return true
}

// RecordSuccess records a successful request.
func (cb *CircuitBreaker) RecordSuccess() {
	state := CircuitState(cb.state.Load())

	switch state {
	case CircuitClosed:
		// Reset failure count on success
		cb.failureCount.Store(0)
	case CircuitHalfOpen:
		count := cb.successCount.Add(1)
		if count >= cb.successThreshold {
			cb.state.Store(int32(CircuitClosed))
			cb.failureCount.Store(0)
			cb.logger.Info("Circuit breaker closed, mirror recovered",
				zap.String("mirror", cb.name))
		}
	}
}

// RecordFailure records a failed request.
func (cb *CircuitBreaker) RecordFailure() {
	cb.lastFailureTime.Store(time.Now().UnixNano())
	state := CircuitState(cb.state.Load())

	switch state {
	case CircuitClosed:
		count := cb.failureCount.Add(1)
		if count >= cb.failureThreshold {
			cb.state.Store(int32(CircuitOpen))
			cb.logger.Warn("Circuit breaker opened, mirror isolated",
				zap.String("mirror", cb.name),
				zap.Int64("failures", count))
		}
	case CircuitHalfOpen:
		// Back to open
		cb.state.Store(int32(CircuitOpen))
		cb.logger.Warn("Circuit breaker re-opened after half-open failure",
			zap.String("mirror", cb.name))
	}
}

// State returns the current circuit state.
func (cb *CircuitBreaker) State() CircuitState {
	return CircuitState(cb.state.Load())
}

// MirrorCircuitBreakers manages circuit breakers for all mirrors.
type MirrorCircuitBreakers struct {
	breakers []*CircuitBreaker
	mutex    sync.RWMutex
}

func NewMirrorCircuitBreakers(mirrorCount int, failureThreshold int64, recoveryTimeout time.Duration, logger *zap.Logger) *MirrorCircuitBreakers {
	mcb := &MirrorCircuitBreakers{
		breakers: make([]*CircuitBreaker, mirrorCount),
	}
	for i := 0; i < mirrorCount; i++ {
		name := fmt.Sprintf("mirror_%d", i)
		mcb.breakers[i] = NewCircuitBreaker(name, failureThreshold, recoveryTimeout, logger)
	}
	return mcb
}

// Allow checks if the mirror at index is allowed.
func (mcb *MirrorCircuitBreakers) Allow(index int) bool {
	mcb.mutex.RLock()
	defer mcb.mutex.RUnlock()
	if index < 0 || index >= len(mcb.breakers) {
		return false
	}
	return mcb.breakers[index].Allow()
}

// RecordSuccess records a success for the mirror at index.
func (mcb *MirrorCircuitBreakers) RecordSuccess(index int) {
	mcb.mutex.RLock()
	defer mcb.mutex.RUnlock()
	if index >= 0 && index < len(mcb.breakers) {
		mcb.breakers[index].RecordSuccess()
	}
}

// RecordFailure records a failure for the mirror at index.
func (mcb *MirrorCircuitBreakers) RecordFailure(index int) {
	mcb.mutex.RLock()
	defer mcb.mutex.RUnlock()
	if index >= 0 && index < len(mcb.breakers) {
		mcb.breakers[index].RecordFailure()
	}
}
