package server

import (
	"context"
	"fmt"
	"io"
	"strings"

	wire "github.com/jeroenrinzema/psql-wire"
	"go.uber.org/zap"
)

// isCopyFromQuery checks if the query is a COPY ... FROM STDIN statement.
func isCopyFromQuery(query string) bool {
	trimmed := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(trimmed, "COPY ") && strings.Contains(trimmed, "FROM STDIN")
}

// isCopyToQuery checks if the query is a COPY ... TO STDOUT statement.
func isCopyToQuery(query string) bool {
	trimmed := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(trimmed, "COPY ") && strings.Contains(trimmed, "TO STDOUT")
}

// isCopyQuery checks if the query is any COPY statement.
func isCopyQuery(query string) bool {
	trimmed := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(trimmed, "COPY ")
}

// copyReaderAdapter bridges psql-wire's CopyReader to io.Reader for pgx's CopyFrom.
// It reads CopyData messages from the client and exposes them as a sequential byte stream.
type copyReaderAdapter struct {
	copyReader *wire.CopyReader
	buf        []byte // remaining bytes from current chunk
	done       bool
}

func newCopyReaderAdapter(cr *wire.CopyReader) *copyReaderAdapter {
	return &copyReaderAdapter{copyReader: cr}
}

// Read implements io.Reader. It reads the next chunk of COPY data from the client.
func (a *copyReaderAdapter) Read(p []byte) (int, error) {
	// If we have leftover bytes from a previous chunk, serve those first
	if len(a.buf) > 0 {
		n := copy(p, a.buf)
		a.buf = a.buf[n:]
		return n, nil
	}

	if a.done {
		return 0, io.EOF
	}

	// Read next CopyData message from client
	err := a.copyReader.Read()
	if err == io.EOF {
		a.done = true
		return 0, io.EOF
	}
	if err != nil {
		return 0, err
	}

	// copyReader.Msg contains the raw CopyData payload
	data := a.copyReader.Msg
	n := copy(p, data)
	if n < len(data) {
		// Store remainder for next Read call
		a.buf = make([]byte, len(data)-n)
		copy(a.buf, data[n:])
	}
	return n, nil
}

// executeCopyFrom handles COPY ... FROM STDIN by:
// 1. Initiating CopyIn on the client connection (psql-wire) to receive data
// 2. Piping the data stream to primary via pgx's PgConn().CopyFrom()
// 3. Logging a warning that mirror replication is not possible for STDIN data
func (server *ProxyServerV15) executeCopyFrom(ctx context.Context, query string, writer wire.DataWriter, sessionID string) error {
	tableName := extractCopyTableName(query)
	server.logger.Info("Handling COPY FROM STDIN",
		zap.String("table", tableName))

	if server.primary == nil || server.primary.pool == nil {
		return fmt.Errorf("primary connection not available")
	}

	// Step 1: Initiate CopyIn on client side to receive data stream
	// CopyIn requires columns to be defined on the writer. We use TextFormat since
	// most COPY FROM STDIN uses text/CSV format.
	copyReader, err := writer.CopyIn(wire.TextFormat)
	if err != nil {
		return fmt.Errorf("failed to initiate COPY protocol with client: %w", err)
	}

	// Step 2: Create an io.Reader adapter that reads from the client's CopyData stream
	reader := newCopyReaderAdapter(copyReader)

	// Step 3: Acquire primary connection and pipe data
	conn, err := server.primary.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire primary connection for COPY: %w", err)
	}
	defer conn.Release()

	// PgConn().CopyFrom reads from the io.Reader and sends data to PostgreSQL
	cmdTag, err := conn.Conn().PgConn().CopyFrom(ctx, reader, query)
	if err != nil {
		server.logger.Error("COPY FROM STDIN failed on primary",
			zap.String("table", tableName),
			zap.Error(err))
		return fmt.Errorf("COPY FROM STDIN failed: %w", err)
	}

	rowCount := cmdTag.RowsAffected()
	server.logger.Info("COPY FROM STDIN completed on primary",
		zap.String("table", tableName),
		zap.Int64("rows", rowCount))

	// Step 4: Log mirror warning
	if len(server.mirrors) > 0 {
		server.logger.Warn("COPY FROM STDIN: data not replicated to mirror",
			zap.String("table", tableName),
			zap.Int64("primary_rows", rowCount),
			zap.String("hint", "use COPY FROM '/path/file.csv' (server-side file) for mirror replication"))
	}

	return writer.Complete(fmt.Sprintf("COPY %d", rowCount))
}

// executeCopyTo handles COPY ... TO STDOUT.
// LIMITATION: COPY TO STDOUT requires streaming CopyData messages back to the client
// through the wire protocol. The current implementation cannot fully support this because
// pgx's Exec() will consume the COPY data internally. The client will receive a COPY count
// but not the actual data rows. Clients should use COPY TO '/file' or SELECT instead.
func (server *ProxyServerV15) executeCopyTo(ctx context.Context, query string, writer wire.DataWriter, sessionID string) error {
	tableName := extractCopyTableName(query)
	server.logger.Warn("COPY TO STDOUT has limited support - data cannot be streamed to client through proxy",
		zap.String("table", tableName),
		zap.String("hint", "use COPY TO '/path/file.csv' (server-side file) or SELECT instead"))

	return fmt.Errorf("COPY TO STDOUT is not fully supported through this proxy. Use COPY TO '/path/file.csv' or a SELECT query instead. Table: %s", tableName)
}

// extractCopyTableName extracts the table name from a COPY statement.
// e.g. "COPY myschema.mytable (col1, col2) FROM STDIN" → "myschema.mytable"
func extractCopyTableName(query string) string {
	trimmed := strings.TrimSpace(query)
	// Remove "COPY " prefix (case-insensitive)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "COPY ") {
		return "unknown"
	}
	rest := strings.TrimSpace(trimmed[5:])

	// Table name is the next token (may include schema prefix)
	// Stop at whitespace or opening parenthesis
	var tableName strings.Builder
	for _, ch := range rest {
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '(' {
			break
		}
		tableName.WriteRune(ch)
	}

	name := tableName.String()
	if name == "" {
		return "unknown"
	}
	return name
}
