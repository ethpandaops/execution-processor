package cryo

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
)

// integrationDSN names a ClickHouse holding the tables in testdata/schema.sql.
// The insert path cannot be proved without one: UInt256 limb order,
// FixedString padding and LowCardinality encoding are all wire-format
// properties that only a real server validates.
const integrationDSNEnv = "CRYO_TEST_CLICKHOUSE"

func integrationClient(t *testing.T) clickhouse.ClientInterface {
	t.Helper()

	addr := os.Getenv(integrationDSNEnv)
	if addr == "" {
		t.Skipf("set %s to addr/database (e.g. localhost:9000/cryo_e2e) to run", integrationDSNEnv)
	}

	host, database := addr, "default"
	for i := range len(addr) {
		if addr[i] == '/' {
			host, database = addr[:i], addr[i+1:]

			break
		}
	}

	cfg := &clickhouse.Config{Addr: host, Database: database, Processor: "cryo_test", Network: "mainnet"}
	cfg.SetDefaults()

	client, err := clickhouse.New(cfg)
	require.NoError(t, err)
	require.NoError(t, client.Start())

	t.Cleanup(func() { _ = client.Stop() })

	return client
}

// TestInsertEveryDataset runs every dataset's real column mapping into a real
// ClickHouse and reads the row count back, which is the only way to catch a
// column block that encodes to something the server rejects.
func TestInsertEveryDataset(t *testing.T) {
	client := integrationClient(t)
	ctx := t.Context()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	for _, ds := range datasets {
		t.Run(ds.Name, func(t *testing.T) {
			s := ds.newSink(sinkDeps{
				log:                 log,
				clickhouse:          client,
				dataset:             ds,
				table:               ds.Table,
				network:             "mainnet",
				processor:           "cryo_test",
				bufferMaxRows:       1,
				bufferFlushInterval: time.Hour,
			})

			require.NoError(t, s.ValidateSchema(ctx), "startup schema check must accept the real table")

			require.NoError(t, s.Start(ctx))
			t.Cleanup(func() { _ = s.Stop(ctx) })

			var expected int

			for _, block := range []string{"b23000000", "b23000026", "b15000051"} {
				table := fixtureTable(t, block, ds.Name)

				rows, err := s.Consume(ctx, table, fixtureMeta)
				require.NoError(t, err)
				require.Equal(t, table.Rows(), rows, "every parquet row must reach the buffer")

				expected += rows
			}

			require.Equal(t, expected, countRows(t, client, ds.Table))
		})
	}
}

// TestUint256RoundTrip proves the limb order of the UInt256 encoding against
// ClickHouse itself rather than against the code that produced it.
func TestUint256RoundTrip(t *testing.T) {
	client := integrationClient(t)
	ctx := t.Context()

	values := []string{
		"0",
		"1",
		"18446744073709551615",
		"18446744073709551616",
		"82520860000000000000",
		"2227376088180124203790135",
		"340282366920938463463374607431768211455",
		"340282366920938463463374607431768211456",
		"90200608809960061854851247237396347024452973173411525954377011279129548947580",
		"115792089237316195423570985008687907853269984665640564039457584007913129639935",
	}

	var col proto.ColUInt256

	for _, v := range values {
		parsed, err := uint256(v)
		require.NoError(t, err, "value %s", v)

		col.Append(parsed)
	}

	table := "cryo_uint256_roundtrip"
	require.NoError(t, client.Execute(ctx, "DROP TABLE IF EXISTS "+table))
	require.NoError(t, client.Execute(ctx, "CREATE TABLE "+table+" (v UInt256) ENGINE = Memory"))

	t.Cleanup(func() { _ = client.Execute(context.Background(), "DROP TABLE IF EXISTS "+table) })

	input := proto.Input{{Name: "v", Data: &col}}
	require.NoError(t, client.Do(ctx, ch.Query{Body: input.Into(table), Input: input}))

	var readBack proto.ColStr

	got := make([]string, 0, len(values))

	require.NoError(t, client.Do(ctx, ch.Query{
		Body:   "SELECT toString(v) AS v FROM " + table + " ORDER BY v",
		Result: proto.Results{{Name: "v", Data: &readBack}},
		OnResult: func(_ context.Context, _ proto.Block) error {
			for i := range readBack.Rows() {
				got = append(got, readBack.Row(i))
			}

			return nil
		},
	}))

	require.ElementsMatch(t, values, got)
}

// countRows counts only the rows this test wrote, identified by the fixed
// fixture timestamp. Counting the whole table would break against leftovers
// from an earlier run and against any processor writing concurrently, and
// truncating a shared database to avoid that is worse than either.
func countRows(t *testing.T, client clickhouse.ClientInterface, table string) int {
	t.Helper()

	query := fmt.Sprintf("SELECT count() AS c FROM %s WHERE updated_date_time = '%s'",
		table, fixtureMeta.updated.Format("2006-01-02 15:04:05"))

	count, err := client.QueryUInt64(t.Context(), query, "c")
	require.NoError(t, err)
	require.NotNil(t, count)

	return int(*count)
}
