package cryo

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

// fixtureMeta is the per-batch metadata every fixture decode uses, fixed so
// golden assertions do not move with the clock.
var fixtureMeta = rowMeta{
	network: "mainnet",
	updated: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
}

// fixtureTable decodes a committed cryo parquet fixture.
func fixtureTable(t *testing.T, block, dataset string) *decode.Table {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join("testdata", block, fmt.Sprintf("ethereum__%s__*.parquet", dataset)))
	require.NoError(t, err)
	require.Len(t, matches, 1, "expected exactly one %s fixture in %s", dataset, block)

	table, err := decode.File(matches[0])
	require.NoError(t, err)

	return table
}

// decodeFixture runs a dataset mapping over its committed fixture.
func decodeFixture[R any](t *testing.T, block, dataset string, fn decodeFunc[R]) ([]R, error) {
	t.Helper()

	return fn(fixtureTable(t, block, dataset), fixtureMeta)
}

var (
	ddlTableRe  = regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS (\w+)\s*\((.*?)\n\)`)
	ddlColumnRe = regexp.MustCompile("^\\s*`([^`]+)`\\s+(.+?)(?:\\s+COMMENT\\s|\\s+CODEC\\(|,?$)")
)

// schemaDDL parses the committed target DDL into table -> column -> type.
func schemaDDL(t *testing.T) map[string]map[string]string {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("testdata", "schema.sql"))
	require.NoError(t, err)

	tables := make(map[string]map[string]string)

	for _, match := range ddlTableRe.FindAllStringSubmatch(string(raw), -1) {
		cols := make(map[string]string)

		for line := range splitLines(match[2]) {
			parts := ddlColumnRe.FindStringSubmatch(line)
			if parts == nil {
				continue
			}

			cols[parts[1]] = trimTrailingComma(parts[2])
		}

		tables[match[1]] = cols
	}

	require.Len(t, tables, 16, "expected 16 tables in testdata/schema.sql")

	return tables
}

func splitLines(s string) func(func(string) bool) {
	return func(yield func(string) bool) {
		start := 0

		for i := range len(s) {
			if s[i] != '\n' {
				continue
			}

			if !yield(s[start:i]) {
				return
			}

			start = i + 1
		}

		if start < len(s) {
			yield(s[start:])
		}
	}
}

func trimTrailingComma(s string) string {
	for len(s) > 0 && (s[len(s)-1] == ',' || s[len(s)-1] == ' ') {
		s = s[:len(s)-1]
	}

	return s
}

// requireInputMatchesDDL asserts that a dataset's column block writes exactly
// the columns its target table declares, with exactly matching types. This is
// the compile-time-adjacent guard on the mappings: a wrong width or a missed
// Nullable would otherwise only surface as a ClickHouse insert error.
func requireInputMatchesDDL(t *testing.T, table string, input proto.Input) {
	t.Helper()

	want, ok := schemaDDL(t)[table]
	require.True(t, ok, "table %s missing from testdata/schema.sql", table)

	got := make(map[string]string, len(input))

	for _, col := range input {
		got[col.Name] = string(col.Data.Type())
	}

	require.Equal(t, want, got, "column mapping for %s does not match its DDL", table)
}
