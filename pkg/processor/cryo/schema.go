package cryo

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
)

// describeTable returns the column name to ClickHouse type mapping of a table.
func describeTable(ctx context.Context, client clickhouse.ClientInterface, table string) (map[string]string, error) {
	database, name, err := splitTableName(table)
	if err != nil {
		return nil, err
	}

	queryCtx, cancel := context.WithTimeout(ctx, tracker.DefaultClickHouseTimeout)
	defer cancel()

	var (
		names proto.ColStr
		types proto.ColStr
		out   = make(map[string]string)
	)

	body := fmt.Sprintf(
		"SELECT name, type FROM system.columns WHERE database = %s AND table = '%s'",
		database, name,
	)

	err = client.Do(queryCtx, ch.Query{
		Body: body,
		Result: proto.Results{
			{Name: "name", Data: &names},
			{Name: "type", Data: &types},
		},
		OnResult: func(_ context.Context, _ proto.Block) error {
			for i := range names.Rows() {
				out[names.Row(i)] = types.Row(i)
			}

			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("describe table %s: %w", table, err)
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("table %s does not exist or has no columns", table)
	}

	return out, nil
}

// splitTableName resolves a configured table into the database expression and
// table name to look up, rejecting anything that is not a plain identifier so
// the values can be interpolated into the lookup.
func splitTableName(table string) (database, name string, err error) {
	database, name = "currentDatabase()", table

	if db, rest, found := strings.Cut(table, "."); found {
		if !isIdentifier(db) {
			return "", "", fmt.Errorf("table %q has an invalid database name", table)
		}

		database, name = "'"+db+"'", rest
	}

	if !isIdentifier(name) {
		return "", "", fmt.Errorf("table %q is not a plain identifier", table)
	}

	return database, name, nil
}

func isIdentifier(s string) bool {
	if s == "" {
		return false
	}

	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
		default:
			return false
		}
	}

	return true
}
