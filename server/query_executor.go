package server

import (
	"context"
	"fmt"
	"strings"

	wire "github.com/jeroenrinzema/psql-wire"
	"go.uber.org/zap"
)

// executeQuery is the main entry point for query execution - routes to primary and mirrors.
func (server *ProxyServerV15) executeQuery(ctx context.Context, query string, writer wire.DataWriter, parameters []wire.Parameter) error {
	server.logger.Debug("Executing query", zap.String("query", truncateQuery(query, 500)))

	sessionID := getSessionIDFromContext(ctx)
	_, cmdType := IsTransactionCommand(query)
	pgxArgs := server.convertParameters(parameters)

	// Handle COPY statements specially
	if isCopyQuery(query) {
		return server.handleCopyQuery(ctx, query, writer, pgxArgs, sessionID)
	}

	// Execute on primary
	err := server.executePrimaryQuery(ctx, query, writer, pgxArgs, sessionID)
	if err != nil {
		server.logger.Error("Primary query failed", zap.String("query", truncateQuery(query, 200)), zap.Error(err))
		return err
	}

	// Dispatch to mirror routing
	server.dispatchToMirrors(ctx, sessionID, query, cmdType, pgxArgs, parameters)
	return nil
}

// handleCopyQuery routes COPY commands appropriately.
func (server *ProxyServerV15) handleCopyQuery(ctx context.Context, query string, writer wire.DataWriter, args []any, sessionID string) error {
	if isCopyFromQuery(query) {
		// COPY ... FROM STDIN — special protocol handling
		return server.executeCopyFrom(ctx, query, writer, sessionID)
	}
	if isCopyToQuery(query) {
		// COPY ... TO STDOUT — read from primary, send to client (no mirror needed)
		return server.executeCopyTo(ctx, query, writer, sessionID)
	}

	// File-based COPY — execute on primary
	err := server.executePrimaryQuery(ctx, query, writer, args, sessionID)
	if err != nil {
		return err
	}

	// Only mirror COPY FROM (data loading), not COPY TO (data export)
	trimmed := strings.ToUpper(strings.TrimSpace(query))
	if strings.Contains(trimmed, " FROM ") {
		_, cmdType := IsTransactionCommand(query)
		server.dispatchToMirrors(ctx, sessionID, query, cmdType, args, nil)
	}
	return nil
}

// convertParameters converts wire.Parameter slice to pgx-compatible []any arguments.
func (server *ProxyServerV15) convertParameters(parameters []wire.Parameter) []any {
	if len(parameters) == 0 {
		return nil
	}
	args := make([]any, len(parameters))
	for i, p := range parameters {
		if p.Value() == nil {
			args[i] = nil
		} else {
			args[i] = string(p.Value())
		}
	}
	return args
}

// executePrimaryQuery executes a query on the primary.
// Uses session affinity only during active transactions, otherwise executes directly from pool.
func (server *ProxyServerV15) executePrimaryQuery(ctx context.Context, query string, writer wire.DataWriter, args []any, sessionID string) error {
	if server.primary == nil || server.primary.pool == nil {
		return fmt.Errorf("primary connection not available")
	}

	affinity := server.primary.affinity

	// Fast path: if not in a transaction and not starting one, skip affinity entirely
	_, cmdType := IsTransactionCommand(query)
	if affinity != nil && (affinity.IsInTransaction(sessionID) || cmdType == "BEGIN") {
		// Transaction path: use affinity for connection pinning
		return server.executePrimaryQueryWithAffinity(ctx, query, writer, args, sessionID, cmdType)
	}

	// Non-transaction path: direct pool execution (no mutex overhead)
	return server.executePrimaryQueryDirect(ctx, query, writer, args)
}

// executePrimaryQueryWithAffinity handles queries within transactions using pinned connections.
func (server *ProxyServerV15) executePrimaryQueryWithAffinity(ctx context.Context, query string, writer wire.DataWriter, args []any, sessionID string, cmdType string) error {
	affinity := server.primary.affinity

	conn, err := affinity.GetConnection(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("failed to get session connection: %w", err)
	}

	// Execute the query
	var execErr error
	if isResultReturningQuery(query) && writer != nil {
		rows, err := conn.Query(ctx, query, args...)
		if err != nil {
			execErr = fmt.Errorf("query execution failed: %w", err)
		} else {
			execErr = writeRowsToWire(rows, writer)
			rows.Close()
		}
	} else {
		cmdTag, err := conn.Exec(ctx, query, args...)
		if err != nil {
			execErr = fmt.Errorf("exec failed: %w", err)
		} else if writer != nil {
			execErr = writer.Complete(cmdTag.String())
		}
	}

	// Update transaction state and release if done (atomic check)
	if affinity.UpdateAndCheckRelease(sessionID, query) {
		affinity.ReleaseConnection(sessionID)
	}

	return execErr
}

// executePrimaryQueryDirect executes directly from pool without session affinity.
func (server *ProxyServerV15) executePrimaryQueryDirect(ctx context.Context, query string, writer wire.DataWriter, args []any) error {
	conn, err := server.primary.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	if isResultReturningQuery(query) && writer != nil {
		rows, err := conn.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("query execution failed: %w", err)
		}
		defer rows.Close()
		return writeRowsToWire(rows, writer)
	}

	cmdTag, err := conn.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}
	if writer != nil {
		return writer.Complete(cmdTag.String())
	}
	return nil
}

// writeRowsToWire streams pgx rows to the wire protocol writer.
func writeRowsToWire(rows interface {
	Next() bool
	Values() ([]any, error)
	Err() error
}, writer wire.DataWriter) error {
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("query error: %w", err)
		}
		return writer.Complete("SELECT 0")
	}

	rowCount := uint64(0)
	for {
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		if err := writer.Row(values); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
		rowCount++
		if !rows.Next() {
			break
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}
	return writer.Complete(fmt.Sprintf("SELECT %d", rowCount))
}

// isResultReturningQuery checks if a query returns result rows.
func isResultReturningQuery(query string) bool {
	trimmed := strings.ToUpper(strings.TrimSpace(query))
	for _, prefix := range []string{"SELECT", "WITH", "VALUES", "TABLE", "SHOW", "EXPLAIN"} {
		if strings.HasPrefix(trimmed, prefix) {
			return true
		}
	}
	// INSERT/UPDATE/DELETE ... RETURNING also returns rows
	// Check for RETURNING keyword (with space before to avoid matching column names)
	if strings.Contains(trimmed, " RETURNING") {
		return true
	}
	return false
}

// Helper function to get session ID from context using psql-wire session.
func getSessionIDFromContext(ctx context.Context) string {
	return getOrCreateSessionID(ctx)
}
