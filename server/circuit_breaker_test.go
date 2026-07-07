package server

import (
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestCircuitBreaker_ClosedAllowsRequests(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	cb := NewCircuitBreaker("test", 3, 1*time.Second, logger)

	if !cb.Allow() {
		t.Error("closed circuit should allow requests")
	}
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	cb := NewCircuitBreaker("test", 3, 1*time.Second, logger)

	cb.RecordFailure()
	cb.RecordFailure()
	if !cb.Allow() {
		t.Error("should still allow after 2 failures (threshold is 3)")
	}

	cb.RecordFailure()
	if cb.Allow() {
		t.Error("should NOT allow after 3 failures (circuit should be open)")
	}

	if cb.State() != CircuitOpen {
		t.Errorf("expected CircuitOpen, got %d", cb.State())
	}
}

func TestCircuitBreaker_SuccessResetsFailureCount(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	cb := NewCircuitBreaker("test", 3, 1*time.Second, logger)

	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordSuccess() // should reset

	cb.RecordFailure()
	cb.RecordFailure()
	// Only 2 failures since last success, should still be closed
	if !cb.Allow() {
		t.Error("should allow — success should have reset failure count")
	}
}

func TestCircuitBreaker_HalfOpenAfterTimeout(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	cb := NewCircuitBreaker("test", 2, 50*time.Millisecond, logger)

	cb.RecordFailure()
	cb.RecordFailure()
	// Now open
	if cb.Allow() {
		t.Error("should be open immediately after threshold")
	}

	// Wait for recovery timeout
	time.Sleep(60 * time.Millisecond)

	// Should transition to half-open and allow
	if !cb.Allow() {
		t.Error("should allow after recovery timeout (half-open)")
	}

	if cb.State() != CircuitHalfOpen {
		t.Errorf("expected CircuitHalfOpen, got %d", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenRecovery(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	cb := NewCircuitBreaker("test", 2, 50*time.Millisecond, logger)
	cb.successThreshold = 2 // need 2 successes to close

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait and transition to half-open
	time.Sleep(60 * time.Millisecond)
	cb.Allow() // triggers half-open

	// First success
	cb.RecordSuccess()
	if cb.State() != CircuitHalfOpen {
		t.Error("should still be half-open after 1 success (need 2)")
	}

	// Second success — should close
	cb.RecordSuccess()
	if cb.State() != CircuitClosed {
		t.Errorf("expected CircuitClosed after 2 successes, got %d", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	cb := NewCircuitBreaker("test", 2, 50*time.Millisecond, logger)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait and transition to half-open
	time.Sleep(60 * time.Millisecond)
	cb.Allow()

	// Failure in half-open — should go back to open
	cb.RecordFailure()
	if cb.State() != CircuitOpen {
		t.Errorf("expected CircuitOpen after half-open failure, got %d", cb.State())
	}
}

func TestMirrorCircuitBreakers_IndexBounds(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	mcb := NewMirrorCircuitBreakers(3, 5, 30*time.Second, logger)

	// Valid indices
	if !mcb.Allow(0) || !mcb.Allow(1) || !mcb.Allow(2) {
		t.Error("valid indices should be allowed")
	}

	// Out of bounds
	if mcb.Allow(3) {
		t.Error("out of bounds index should not be allowed")
	}
	if mcb.Allow(-1) {
		t.Error("negative index should not be allowed")
	}
}
