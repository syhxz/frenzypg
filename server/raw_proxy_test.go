package server

import (
	"testing"
)

func TestSplitStatements_Basic(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single statement",
			input:    "SELECT 1",
			expected: []string{"SELECT 1"},
		},
		{
			name:     "two statements",
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
			result := splitStatements(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d statements, got %d: %v", len(tt.expected), len(result), result)
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("stmt[%d]: expected %q, got %q", i, tt.expected[i], result[i])
				}
			}
		})
	}
}

func TestSplitStatements_SingleQuotes(t *testing.T) {
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
			result := splitStatements(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d statements, got %d: %v", len(tt.expected), len(result), result)
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("stmt[%d]: expected %q, got %q", i, tt.expected[i], result[i])
				}
			}
		})
	}
}

func TestSplitStatements_DoubleQuotes(t *testing.T) {
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
			result := splitStatements(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d statements, got %d: %v", len(tt.expected), len(result), result)
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("stmt[%d]: expected %q, got %q", i, tt.expected[i], result[i])
				}
			}
		})
	}
}

func TestSplitStatements_DollarQuotes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "dollar quote with semicolons inside",
			input:    "CREATE FUNCTION foo() RETURNS void AS $$ BEGIN RAISE NOTICE 'done;'; END; $$ LANGUAGE plpgsql",
			expected: []string{"CREATE FUNCTION foo() RETURNS void AS $$ BEGIN RAISE NOTICE 'done;'; END; $$ LANGUAGE plpgsql"},
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
			result := splitStatements(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d statements, got %d: %v", len(tt.expected), len(result), result)
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("stmt[%d]:\nexpected: %q\n     got: %q", i, tt.expected[i], result[i])
				}
			}
		})
	}
}

func TestSplitStatements_Comments(t *testing.T) {
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
			result := splitStatements(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d statements, got %d: %v", len(tt.expected), len(result), result)
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("stmt[%d]:\nexpected: %q\n     got: %q", i, tt.expected[i], result[i])
				}
			}
		})
	}
}

func TestReplaceParamsOutsideQuotes_Basic(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		replacements []string
		expected     string
	}{
		{
			name:         "simple replacement",
			sql:          "SELECT $1, $2",
			replacements: []string{"$$hello$$", "42"},
			expected:     "SELECT $$hello$$, 42",
		},
		{
			name:         "no placeholders",
			sql:          "SELECT 1",
			replacements: []string{"unused"},
			expected:     "SELECT 1",
		},
		{
			name:         "placeholder in single quotes not replaced",
			sql:          "SELECT '$1' || $1",
			replacements: []string{"$$val$$"},
			expected:     "SELECT '$1' || $$val$$",
		},
		{
			name:         "placeholder in double quotes not replaced",
			sql:          `SELECT "$1" FROM t WHERE x = $1`,
			replacements: []string{"$$val$$"},
			expected:     `SELECT "$1" FROM t WHERE x = $$val$$`,
		},
		{
			name:         "placeholder in dollar-quoted string not replaced",
			sql:          "SELECT $tag$ $1 is here $tag$ || $1",
			replacements: []string{"$$val$$"},
			expected:     "SELECT $tag$ $1 is here $tag$ || $$val$$",
		},
		{
			name:         "multi-digit placeholder",
			sql:          "SELECT $10, $1",
			replacements: []string{"$$a$$", "$$b$$", "$$c$$", "$$d$$", "$$e$$", "$$f$$", "$$g$$", "$$h$$", "$$i$$", "$$j$$"},
			expected:     "SELECT $$j$$, $$a$$",
		},
		{
			name:         "placeholder with escaped single quote",
			sql:          "SELECT 'it''s $1' || $1",
			replacements: []string{"$$val$$"},
			expected:     "SELECT 'it''s $1' || $$val$$",
		},
		{
			name:         "dollar sign not followed by digit",
			sql:          "SELECT $0, $abc, $1",
			replacements: []string{"$$val$$"},
			expected:     "SELECT $0, $abc, $$val$$",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replaceParamsOutsideQuotes(tt.sql, tt.replacements)
			if result != tt.expected {
				t.Errorf("expected: %q\n     got: %q", tt.expected, result)
			}
		})
	}
}
