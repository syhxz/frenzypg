package server

import (
	"strings"
	"testing"
)

func TestSplitCommands_Basic(t *testing.T) {
	server := &ProxyServerV15{}

	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single command",
			input:    "SELECT 1",
			expected: []string{"SELECT 1"},
		},
		{
			name:     "two commands",
			input:    "SELECT 1; SELECT 2",
			expected: []string{"SELECT 1", "SELECT 2"},
		},
		{
			name:     "trailing semicolon",
			input:    "SELECT 1;",
			expected: []string{"SELECT 1"},
		},
		{
			name:     "empty input",
			input:    "",
			expected: nil,
		},
		{
			name:     "only whitespace",
			input:    "   \n\t  ",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := server.splitCommands(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d commands, got %d: %v", len(tt.expected), len(result), result)
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("command[%d]: expected %q, got %q", i, tt.expected[i], result[i])
				}
			}
		})
	}
}

func TestSplitCommands_SingleQuotes(t *testing.T) {
	server := &ProxyServerV15{}

	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "semicolon in single quotes",
			input:    "SELECT 'hello;world'",
			expected: []string{"SELECT 'hello;world'"},
		},
		{
			name:     "escaped quote (double single quote)",
			input:    "SELECT 'it''s a test'; SELECT 2",
			expected: []string{"SELECT 'it''s a test'", "SELECT 2"},
		},
		{
			name:     "backslash escape in E string",
			input:    `SELECT E'hello\';world'; SELECT 2`,
			expected: []string{`SELECT E'hello\';world'`, "SELECT 2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := server.splitCommands(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d commands, got %d: %v", len(tt.expected), len(result), result)
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("command[%d]: expected %q, got %q", i, tt.expected[i], result[i])
				}
			}
		})
	}
}

func TestSplitCommands_DoubleQuotes(t *testing.T) {
	server := &ProxyServerV15{}

	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "semicolon in double quotes",
			input:    `SELECT "col;name" FROM t`,
			expected: []string{`SELECT "col;name" FROM t`},
		},
		{
			name:     "escaped double quote",
			input:    `SELECT "col""name" FROM t; SELECT 2`,
			expected: []string{`SELECT "col""name" FROM t`, "SELECT 2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := server.splitCommands(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d commands, got %d: %v", len(tt.expected), len(result), result)
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("command[%d]: expected %q, got %q", i, tt.expected[i], result[i])
				}
			}
		})
	}
}

func TestSplitCommands_DollarQuotes(t *testing.T) {
	server := &ProxyServerV15{}

	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "dollar quote with semicolon inside",
			input:    "CREATE FUNCTION f() RETURNS void AS $$ BEGIN RETURN; END; $$ LANGUAGE plpgsql",
			expected: []string{"CREATE FUNCTION f() RETURNS void AS $$ BEGIN RETURN; END; $$ LANGUAGE plpgsql"},
		},
		{
			name:     "tagged dollar quote",
			input:    "CREATE FUNCTION f() RETURNS void AS $body$ BEGIN RETURN; END; $body$ LANGUAGE plpgsql; SELECT 1",
			expected: []string{"CREATE FUNCTION f() RETURNS void AS $body$ BEGIN RETURN; END; $body$ LANGUAGE plpgsql", "SELECT 1"},
		},
		{
			name:     "nested dollar quotes",
			input:    "DO $$ BEGIN EXECUTE $inner$ SELECT 1; $inner$; END $$; SELECT 2",
			expected: []string{"DO $$ BEGIN EXECUTE $inner$ SELECT 1; $inner$; END $$", "SELECT 2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := server.splitCommands(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d commands, got %d: %v", len(tt.expected), len(result), result)
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("command[%d]:\nexpected: %q\n     got: %q", i, tt.expected[i], result[i])
				}
			}
		})
	}
}

func TestSplitCommands_Comments(t *testing.T) {
	server := &ProxyServerV15{}

	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single line comment with semicolon",
			input:    "SELECT 1 -- this is; a comment\n; SELECT 2",
			expected: []string{"SELECT 1 -- this is; a comment", "SELECT 2"},
		},
		{
			name:     "multi-line comment with semicolon",
			input:    "SELECT /* ; */ 1; SELECT 2",
			expected: []string{"SELECT /* ; */ 1", "SELECT 2"},
		},
		{
			name:     "nested multi-line comments",
			input:    "SELECT /* outer /* inner ; */ still comment ; */ 1; SELECT 2",
			expected: []string{"SELECT /* outer /* inner ; */ still comment ; */ 1", "SELECT 2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := server.splitCommands(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d commands, got %d: %v", len(tt.expected), len(result), result)
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("command[%d]:\nexpected: %q\n     got: %q", i, tt.expected[i], result[i])
				}
			}
		})
	}
}

func TestShouldMirrorQuery(t *testing.T) {
	server := NewTestProxyServer(nil) // all defaults: no MirrorAll, no MirrorSelect

	tests := []struct {
		query    string
		expected bool
	}{
		// DDL - should mirror by default
		{"CREATE TABLE t (id int)", true},
		{"ALTER TABLE t ADD COLUMN name text", true},
		{"DROP TABLE t", true},
		{"TRUNCATE t", true},
		{"CREATE FUNCTION f() RETURNS void AS $$ BEGIN END $$ LANGUAGE plpgsql", true},
		{"CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()", true},
		{"CREATE SEQUENCE myseq", true},

		// DML - should mirror by default
		{"INSERT INTO t VALUES (1)", true},
		{"UPDATE t SET name = 'x' WHERE id = 1", true},
		{"DELETE FROM t WHERE id = 1", true},
		{"MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name", true},

		// Transaction commands - always mirror
		{"BEGIN", true},
		{"COMMIT", true},
		{"ROLLBACK", true},
		{"START TRANSACTION", true},

		// Savepoints - always mirror
		{"SAVEPOINT sp1", true},
		{"RELEASE SAVEPOINT sp1", true},
		{"ROLLBACK TO SAVEPOINT sp1", true},
		{"ROLLBACK TO sp1", true},

		// Session commands - always mirror
		{"SET search_path TO myschema", true},
		{"SET timezone TO 'UTC'", true},
		{"RESET ALL", true},
		{"DISCARD ALL", true},

		// PREPARE/EXECUTE/DEALLOCATE - always mirror
		{"PREPARE myplan AS SELECT * FROM t WHERE id = $1", true},
		{"EXECUTE myplan(1)", true},
		{"DEALLOCATE myplan", true},
		{"DEALLOCATE ALL", true},

		// CALL / DO - should mirror
		{"CALL my_procedure(1, 2)", true},
		{"DO $$ BEGIN PERFORM 1; END $$", true},

		// COPY FROM - should mirror
		{"COPY t FROM '/tmp/data.csv'", true},

		// COPY TO - should NOT mirror (read-only)
		{"COPY t TO '/tmp/data.csv'", false},
		{"COPY t TO STDOUT", false},

		// SELECT - should NOT mirror by default
		{"SELECT * FROM t", false},
		{"WITH cte AS (SELECT 1) SELECT * FROM cte", false},

		// Sequence manipulation - always mirror
		{"SELECT setval('myseq', 100)", true},
		{"SELECT nextval('myseq')", true},

		// GRANT/REVOKE - should mirror
		{"GRANT SELECT ON t TO myuser", true},
		{"REVOKE ALL ON t FROM public", true},

		// LISTEN/NOTIFY - not mirror by default
		{"LISTEN mychannel", false},
		{"NOTIFY mychannel, 'hello'", false},

		// Maintenance - not mirror by default
		{"VACUUM t", false},
		{"ANALYZE t", false},
		{"REINDEX TABLE t", false},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			result := server.shouldMirrorQuery(tt.query)
			if result != tt.expected {
				t.Errorf("shouldMirrorQuery(%q) = %v, want %v", tt.query, result, tt.expected)
			}
		})
	}
}

func TestShouldMirrorQuery_MirrorAllQueries(t *testing.T) {
	server := NewTestProxyServer(&QueryFilterConfig{MirrorAllQueries: true})

	queries := []string{
		"SELECT 1",
		"LISTEN mychannel",
		"VACUUM t",
		"ANALYZE t",
	}

	for _, q := range queries {
		if !server.shouldMirrorQuery(q) {
			t.Errorf("with MirrorAllQueries, shouldMirrorQuery(%q) should be true", q)
		}
	}
}

func TestShouldMirrorQuery_MirrorSelectQueries(t *testing.T) {
	server := NewTestProxyServer(&QueryFilterConfig{MirrorSelectQueries: true})

	if !server.shouldMirrorQuery("SELECT * FROM t") {
		t.Error("with MirrorSelectQueries, SELECT should be mirrored")
	}
	if !server.shouldMirrorQuery("WITH cte AS (SELECT 1) SELECT * FROM cte") {
		t.Error("with MirrorSelectQueries, WITH should be mirrored")
	}
}

func TestShouldMirrorQuery_DdlOnly(t *testing.T) {
	server := NewTestProxyServer(&QueryFilterConfig{MirrorDdlOnly: true})

	if !server.shouldMirrorQuery("CREATE TABLE t (id int)") {
		t.Error("with MirrorDdlOnly, DDL should be mirrored")
	}
	if server.shouldMirrorQuery("INSERT INTO t VALUES (1)") {
		t.Error("with MirrorDdlOnly, DML should NOT be mirrored")
	}
	if server.shouldMirrorQuery("CALL my_proc()") {
		t.Error("with MirrorDdlOnly, CALL should NOT be mirrored")
	}
}

func TestShouldMirrorQuery_DmlOnly(t *testing.T) {
	server := NewTestProxyServer(&QueryFilterConfig{MirrorDmlOnly: true})

	if server.shouldMirrorQuery("CREATE TABLE t (id int)") {
		t.Error("with MirrorDmlOnly, DDL should NOT be mirrored")
	}
	if !server.shouldMirrorQuery("INSERT INTO t VALUES (1)") {
		t.Error("with MirrorDmlOnly, DML should be mirrored")
	}
}

func TestIsTransactionCommand(t *testing.T) {
	tests := []struct {
		query       string
		isTx        bool
		expectedCmd string
	}{
		{"BEGIN", true, "BEGIN"},
		{"begin", true, "BEGIN"},
		{"START TRANSACTION", true, "BEGIN"},
		{"COMMIT", true, "COMMIT"},
		{"END", true, "COMMIT"},
		{"ROLLBACK", true, "ROLLBACK"},
		{"ROLLBACK TO SAVEPOINT sp1", true, "ROLLBACK_TO_SAVEPOINT"},
		{"ROLLBACK TO sp1", true, "ROLLBACK_TO_SAVEPOINT"},
		{"SAVEPOINT sp1", true, "SAVEPOINT"},
		{"RELEASE SAVEPOINT sp1", true, "RELEASE_SAVEPOINT"},
		{"RELEASE sp1", true, "RELEASE_SAVEPOINT"},
		{"SELECT 1", false, ""},
		{"INSERT INTO t VALUES (1)", false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			isTx, cmd := IsTransactionCommand(tt.query)
			if isTx != tt.isTx {
				t.Errorf("IsTransactionCommand(%q): isTx = %v, want %v", tt.query, isTx, tt.isTx)
			}
			if cmd != tt.expectedCmd {
				t.Errorf("IsTransactionCommand(%q): cmd = %q, want %q", tt.query, cmd, tt.expectedCmd)
			}
		})
	}
}

func TestIsResultReturningQuery(t *testing.T) {
	tests := []struct {
		query    string
		expected bool
	}{
		{"SELECT 1", true},
		{"select * from t", true},
		{"WITH cte AS (SELECT 1) SELECT * FROM cte", true},
		{"VALUES (1, 2)", true},
		{"TABLE t", true},
		{"SHOW search_path", true},
		{"EXPLAIN SELECT 1", true},
		{"INSERT INTO t VALUES (1) RETURNING id", true},
		{"UPDATE t SET x = 1 RETURNING *", true},
		{"DELETE FROM t WHERE id = 1 RETURNING id, name", true},
		{"INSERT INTO t VALUES (1)", false},
		{"UPDATE t SET x = 1", false},
		{"DELETE FROM t WHERE id = 1", false},
		{"CREATE TABLE t (id int)", false},
		{"BEGIN", false},
		{"COMMIT", false},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			result := isResultReturningQuery(tt.query)
			if result != tt.expected {
				t.Errorf("isResultReturningQuery(%q) = %v, want %v", tt.query, result, tt.expected)
			}
		})
	}
}

func TestExtractCopyTableName(t *testing.T) {
	tests := []struct {
		query    string
		expected string
	}{
		{"COPY mytable FROM STDIN", "mytable"},
		{"COPY myschema.mytable FROM STDIN", "myschema.mytable"},
		{"COPY mytable (col1, col2) FROM STDIN", "mytable"},
		{"copy mytable FROM '/tmp/file.csv'", "mytable"},
		{"COPY mytable TO STDOUT", "mytable"},
		{"not a copy", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			result := extractCopyTableName(tt.query)
			if result != tt.expected {
				t.Errorf("extractCopyTableName(%q) = %q, want %q", tt.query, result, tt.expected)
			}
		})
	}
}

func TestTruncateQuery(t *testing.T) {
	short := "SELECT 1"
	if truncateQuery(short, 100) != short {
		t.Error("short query should not be truncated")
	}

	long := strings.Repeat("x", 300)
	result := truncateQuery(long, 100)
	if len(result) != 103 { // 100 + "..."
		t.Errorf("expected truncated length 103, got %d", len(result))
	}
	if !strings.HasSuffix(result, "...") {
		t.Error("truncated query should end with ...")
	}
}

// NewTestProxyServer creates a ProxyServerV15 with optional filter config for testing.
func NewTestProxyServer(filterConfig *QueryFilterConfig) *ProxyServerV15 {
	server := &ProxyServerV15{
		txBuffers:        make(map[string]*TransactionBuffer),
		poolConfig:       DefaultPoolConfig(),
		performanceConfig: DefaultPerformanceConfig(),
		stopCleanup:      make(chan struct{}),
	}
	if filterConfig == nil {
		filterConfig = &QueryFilterConfig{}
	}
	server.queryFilterConfig.Store(filterConfig)
	return server
}
