package cryo

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// fakeCryo writes an executable stub in place of cryo. The body receives cryo's
// arguments and is responsible for producing whatever output the test needs.
//
// Callers must not run in parallel: exec'ing a freshly written file races with
// any concurrent fork, which inherits the write descriptor and makes exec fail
// with ETXTBSY.
func fakeCryo(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "cryo")
	script := "#!/bin/sh\n" + body + "\n"

	require.NoError(t, os.WriteFile(path, []byte(script), 0o755))

	return path
}

func testFetcher(t *testing.T, binary string) *execFetcher {
	t.Helper()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cfg := &Config{BinaryPath: binary, TempDir: t.TempDir()}
	cfg.Addr = "localhost:9000"
	cfg.Enabled = true
	cfg.Groups = []GroupConfig{{Name: "t", Enabled: true, Datatypes: []string{"blocks"}}}
	require.NoError(t, cfg.Validate())

	return newExecFetcher(log, cfg, func() (string, error) { return "https://user:secret@node.example/", nil })
}

func testGroup(t *testing.T, datatypes ...string) *Group {
	t.Helper()

	g, err := newGroup(&GroupConfig{Name: "t", Enabled: true, Datatypes: datatypes})
	require.NoError(t, err)

	return g
}

// TestFetchPassesEndpointThroughEnvironment is the security-relevant one: the
// endpoint carries basic-auth credentials, and anything in argv is readable
// from /proc by every process sharing the PID namespace.
func TestFetchPassesEndpointThroughEnvironment(t *testing.T) {
	binary := fakeCryo(t, `
for arg in "$@"; do
  case "$arg" in
    *secret*) echo "credential leaked into argv: $arg" >&2; exit 3 ;;
  esac
done
[ "$ETH_RPC_URL" = "https://user:secret@node.example/" ] || { echo "env not set: $ETH_RPC_URL" >&2; exit 4; }
exit 9
`)

	_, err := testFetcher(t, binary).Fetch(t.Context(), testGroup(t, "blocks"), 100, 100)

	require.Error(t, err)
	require.NotContains(t, err.Error(), "leaked into argv")
	require.NotContains(t, err.Error(), "env not set")
	require.Contains(t, err.Error(), "exit status 9")
}

func TestFetchBuildsArguments(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	binary := fakeCryo(t, `printf '%s\n' "$@" > `+argsFile+`; exit 1`)

	group := testGroup(t, "blocks", "state_diffs")

	_, err := testFetcher(t, binary).Fetch(t.Context(), group, 100, 100)
	require.Error(t, err)

	raw, readErr := os.ReadFile(argsFile)
	require.NoError(t, readErr)

	args := strings.Split(strings.TrimSpace(string(raw)), "\n")

	require.Equal(t, "blocks", args[0])
	require.Equal(t, "state_diffs", args[1])
	require.Contains(t, args, "--hex")
	require.Contains(t, args, "--u256-types")
	require.Contains(t, args, "--no-report")

	// Derived from the datasets rather than taken from config, so it cannot be
	// configured away.
	require.Contains(t, args, "--include-columns")
	require.Contains(t, args, "gas_limit")

	// cryo's range end is exclusive, so one block is N:N+1.
	blocks := args[indexOf(t, args, "--blocks")+1]
	require.Equal(t, "100:101", blocks)
}

func TestFetchDecodesEveryDatasetInTheGroup(t *testing.T) {
	// state_diffs is one argument yielding three datasets, so a group naming it
	// must come back with all three decoded.
	src, err := filepath.Abs(filepath.Join("testdata", "b23000000"))
	require.NoError(t, err)

	binary := fakeCryo(t, `
out=""
while [ $# -gt 0 ]; do
  case "$1" in --output-dir) out="$2"; shift 2 ;; *) shift ;; esac
done
for f in `+src+`/*.parquet; do
  base=$(basename "$f")
  ds=$(echo "$base" | sed 's/^ethereum__//; s/__.*//')
  cp "$f" "$out/ethereum__${ds}__100_to_100.parquet"
done
`)

	tables, err := testFetcher(t, binary).Fetch(t.Context(), testGroup(t, "blocks", "state_diffs"), 100, 100)
	require.NoError(t, err)

	require.Len(t, tables, 4)
	require.Equal(t, 1, tables["blocks"].Rows())
	require.Equal(t, 377, tables["balance_diffs"].Rows())
	require.Equal(t, 139, tables["nonce_diffs"].Rows())
	require.Equal(t, 359, tables["storage_diffs"].Rows())
}

// TestFetchFailsWhenDatasetMissing covers cryo exiting successfully having
// written nothing, which must not be mistaken for a block with no rows.
func TestFetchFailsWhenDatasetMissing(t *testing.T) {
	binary := fakeCryo(t, "exit 0")

	_, err := testFetcher(t, binary).Fetch(t.Context(), testGroup(t, "blocks"), 100, 100)

	require.Error(t, err)
	require.Contains(t, err.Error(), "no parquet for dataset blocks")
}

func TestFetchRemovesTempDirOnEveryPath(t *testing.T) {
	parent := t.TempDir()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cfg := &Config{BinaryPath: fakeCryo(t, "echo boom >&2; exit 1"), TempDir: parent}
	cfg.Addr = "localhost:9000"
	cfg.Enabled = true
	cfg.Groups = []GroupConfig{{Name: "t", Enabled: true, Datatypes: []string{"blocks"}}}
	require.NoError(t, cfg.Validate())

	f := newExecFetcher(log, cfg, func() (string, error) { return "http://node", nil })

	for range 3 {
		_, err := f.Fetch(t.Context(), testGroup(t, "blocks"), 100, 100)
		require.Error(t, err)
	}

	entries, err := os.ReadDir(parent)
	require.NoError(t, err)
	require.Empty(t, entries, "every failed fetch must remove its scratch directory")
}

func TestFetchKillsHungSubprocess(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cfg := &Config{BinaryPath: fakeCryo(t, "sleep 120"), TempDir: t.TempDir(), FetchTimeout: 300 * time.Millisecond}
	cfg.Addr = "localhost:9000"
	cfg.Enabled = true
	cfg.Groups = []GroupConfig{{Name: "t", Enabled: true, Datatypes: []string{"blocks"}}}
	require.NoError(t, cfg.Validate())

	f := newExecFetcher(log, cfg, func() (string, error) { return "http://node", nil })

	start := time.Now()
	_, err := f.Fetch(t.Context(), testGroup(t, "blocks"), 100, 100)

	require.Error(t, err)
	require.Less(t, time.Since(start), 30*time.Second, "a hung cryo must not hold the worker")
}

// TestFetchStopsOnContextCancel covers pod drain: an in-flight fetch must
// unblock rather than run to its own timeout.
func TestFetchStopsOnContextCancel(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cfg := &Config{BinaryPath: fakeCryo(t, "sleep 120"), TempDir: t.TempDir()}
	cfg.Addr = "localhost:9000"
	cfg.Enabled = true
	cfg.Groups = []GroupConfig{{Name: "t", Enabled: true, Datatypes: []string{"blocks"}}}
	require.NoError(t, cfg.Validate())

	f := newExecFetcher(log, cfg, func() (string, error) { return "http://node", nil })

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := f.Fetch(ctx, testGroup(t, "blocks"), 100, 100)

	require.Error(t, err)
	require.Less(t, time.Since(start), 30*time.Second)
}

func TestFetchStderrIsBounded(t *testing.T) {
	binary := fakeCryo(t, `i=0; while [ $i -lt 4000 ]; do echo "noisy failure line $i"; i=$((i+1)); done >&2; exit 1`)

	_, err := testFetcher(t, binary).Fetch(t.Context(), testGroup(t, "blocks"), 100, 100)

	require.Error(t, err)
	require.Less(t, len(err.Error()), 3*stderrLimit, "a chatty failure must not balloon the error")
}

func TestSweepTempDirsSparesRecentDirectories(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()

	live := filepath.Join(parent, "cryo-live")
	require.NoError(t, os.Mkdir(live, 0o750))

	orphan := filepath.Join(parent, "cryo-orphan")
	require.NoError(t, os.Mkdir(orphan, 0o750))

	old := time.Now().Add(-2 * sweepMinAge)
	require.NoError(t, os.Chtimes(orphan, old, old))

	unrelated := filepath.Join(parent, "something-else")
	require.NoError(t, os.Mkdir(unrelated, 0o750))
	require.NoError(t, os.Chtimes(unrelated, old, old))

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	sweepTempDirs(log, parent)

	require.DirExists(t, live, "another replica's in-flight directory must survive")
	require.NoDirExists(t, orphan)
	require.DirExists(t, unrelated, "the sweep must only touch its own directories")
}

func TestVersionReportsBinary(t *testing.T) {
	version, err := testFetcher(t, fakeCryo(t, `echo "cryo 0.3.2-37-g559b654"`)).Version(t.Context())

	require.NoError(t, err)
	require.Equal(t, "cryo 0.3.2-37-g559b654", version)
}

func TestVersionFailsWhenBinaryMissing(t *testing.T) {
	t.Parallel()

	_, err := testFetcher(t, "/nonexistent/cryo").Version(t.Context())

	require.Error(t, err)
}

// TestFetchRedactsEndpointFromStderr is the other half of keeping the
// credentials out of reach: they stay out of argv, but cryo echoes its target
// back on stderr, and that text is embedded in the error the worker logs.
func TestFetchRedactsEndpointFromStderr(t *testing.T) {
	for name, body := range map[string]string{
		"full url":    `echo "error sending request for url ($ETH_RPC_URL): connection refused" >&2; exit 1`,
		"credentials": `echo "authentication failed for user:secret" >&2; exit 1`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := testFetcher(t, fakeCryo(t, body)).Fetch(t.Context(), testGroup(t, "blocks"), 100, 100)

			require.Error(t, err)
			require.NotContains(t, err.Error(), "secret", "the endpoint's basic-auth credentials must not reach the logs")
			require.Contains(t, err.Error(), redacted)
		})
	}
}

func TestFetchErrorsAreDistinguishable(t *testing.T) {
	parquet := filepath.Join(t.TempDir(), "blocks.parquet")
	require.NoError(t, os.WriteFile(parquet, []byte("not parquet at all"), 0o600))

	tests := []struct {
		name string
		body string
		want error
	}{
		{
			name: "subprocess failure",
			body: "echo boom >&2; exit 1",
			want: ErrFetchFailed,
		},
		{
			name: "missing parquet",
			body: "exit 0",
			want: ErrParquetMissing,
		},
		{
			name: "decode failure",
			body: `out=""
while [ $# -gt 0 ]; do
  case "$1" in --output-dir) out="$2"; shift 2 ;; *) shift ;; esac
done
cp ` + parquet + ` "$out/ethereum__blocks__100_to_100.parquet"`,
			want: ErrDecodeFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := testFetcher(t, fakeCryo(t, tt.body)).Fetch(t.Context(), testGroup(t, "blocks"), 100, 100)

			require.Error(t, err)
			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestFetchErrorIsTimeoutWhenSubprocessOverruns(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cfg := &Config{BinaryPath: fakeCryo(t, "sleep 120"), TempDir: t.TempDir(), FetchTimeout: 300 * time.Millisecond}
	cfg.Addr = "localhost:9000"
	cfg.Enabled = true
	cfg.Groups = []GroupConfig{{Name: "t", Enabled: true, Datatypes: []string{"blocks"}}}
	require.NoError(t, cfg.Validate())

	f := newExecFetcher(log, cfg, func() (string, error) { return "http://node", nil })

	_, err := f.Fetch(t.Context(), testGroup(t, "blocks"), 100, 100)

	require.ErrorIs(t, err, ErrFetchTimeout)
	require.NotErrorIs(t, err, ErrFetchFailed, "a timeout is a different operational problem to a non-zero exit")
}

func TestFetchErrorIsNoEndpointWhenPoolIsUnhealthy(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cfg := &Config{BinaryPath: fakeCryo(t, "exit 0"), TempDir: t.TempDir()}
	cfg.Addr = "localhost:9000"
	cfg.Enabled = true
	cfg.Groups = []GroupConfig{{Name: "t", Enabled: true, Datatypes: []string{"blocks"}}}
	require.NoError(t, cfg.Validate())

	f := newExecFetcher(log, cfg, func() (string, error) {
		return "", errors.New("no healthy execution node exposes an RPC endpoint for cryo")
	})

	_, err := f.Fetch(t.Context(), testGroup(t, "blocks"), 100, 100)

	require.ErrorIs(t, err, ErrNoEndpoint)
}

// TestFetchKillsGrandchildren covers what the direct child alone does not: cryo
// spawning a helper that outlives it would keep an RPC connection open against a
// node the worker has already given up on.
func TestFetchKillsGrandchildren(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cfg := &Config{
		BinaryPath:   fakeCryo(t, "sleep 120 & echo $! > "+pidFile+"; sleep 120"),
		TempDir:      t.TempDir(),
		FetchTimeout: 300 * time.Millisecond,
	}
	cfg.Addr = "localhost:9000"
	cfg.Enabled = true
	cfg.Groups = []GroupConfig{{Name: "t", Enabled: true, Datatypes: []string{"blocks"}}}
	require.NoError(t, cfg.Validate())

	f := newExecFetcher(log, cfg, func() (string, error) { return "http://node", nil })

	_, err := f.Fetch(t.Context(), testGroup(t, "blocks"), 100, 100)
	require.Error(t, err)

	raw, readErr := os.ReadFile(pidFile)
	require.NoError(t, readErr)

	pid, parseErr := strconv.Atoi(strings.TrimSpace(string(raw)))
	require.NoError(t, parseErr)

	require.Eventually(t, func() bool {
		return syscall.Kill(pid, 0) != nil
	}, 10*time.Second, 50*time.Millisecond, "a process cryo spawned must not survive the kill")
}

func TestVersionErrorIncludesStderr(t *testing.T) {
	// The documented failure is a binary that exists but cannot load a shared
	// library, which says so only on stderr while the status says only 127.
	binary := fakeCryo(t, `echo "cryo: error while loading shared libraries: libssl.so.3: cannot open shared object file" >&2; exit 127`)

	_, err := testFetcher(t, binary).Version(t.Context())

	require.Error(t, err)
	require.Contains(t, err.Error(), "libssl.so.3")
	require.Contains(t, err.Error(), "exit status 127")
}

func TestRedactEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		text     string
		endpoint string
		want     string
	}{
		{
			name:     "full url",
			text:     "dialing https://user:secret@node.example/ failed",
			endpoint: "https://user:secret@node.example/",
			want:     "dialing " + redacted + " failed",
		},
		{
			name:     "url without trailing slash",
			text:     "dialing https://user:secret@node.example failed",
			endpoint: "https://user:secret@node.example/",
			want:     "dialing " + redacted + " failed",
		},
		{
			name:     "credentials alone",
			text:     "rejected credentials user:secret",
			endpoint: "https://user:secret@node.example/",
			want:     "rejected credentials " + redacted,
		},
		{
			name:     "password alone",
			text:     "bad password secret",
			endpoint: "https://user:secret@node.example/",
			want:     "bad password " + redacted,
		},
		{
			name:     "endpoint without credentials",
			text:     "dialing http://node.example failed",
			endpoint: "http://node.example",
			want:     "dialing " + redacted + " failed",
		},
		{
			name:     "unrelated text",
			text:     "block 100 not found",
			endpoint: "https://user:secret@node.example/",
			want:     "block 100 not found",
		},
		{
			name:     "no endpoint",
			text:     "block 100 not found",
			endpoint: "",
			want:     "block 100 not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, redactEndpoint(tt.text, tt.endpoint))
		})
	}
}

func TestTruncateKeepsValidUTF8(t *testing.T) {
	t.Parallel()

	// "…" is three bytes, so every limit that lands inside one exercises the
	// rune boundary walk.
	for limit := range 10 {
		got := truncate("……………", limit)

		require.True(t, utf8.ValidString(got), "limit %d split a rune: %q", limit, got)
		require.LessOrEqual(t, len(got), limit+len("…"))
	}

	require.Equal(t, "short", truncate("short", 5))
	require.Equal(t, "shor…", truncate("short", 4))
}

func indexOf(t *testing.T, args []string, want string) int {
	t.Helper()

	for i, arg := range args {
		if arg == want {
			return i
		}
	}

	t.Fatalf("argument %q not passed to cryo: %v", want, args)

	return -1
}

// TestFetchFindsZeroPaddedOutput covers cryo padding block numbers in the file
// name. Mainnet heights are already eight digits so this is invisible there,
// but it breaks every shorter height: the whole of mainnet below block
// 10,000,000, and every testnet.
func TestFetchFindsZeroPaddedOutput(t *testing.T) {
	src, err := filepath.Abs(filepath.Join("testdata", "b23000000"))
	require.NoError(t, err)

	binary := fakeCryo(t, `
out=""
while [ $# -gt 0 ]; do
  case "$1" in --output-dir) out="$2"; shift 2 ;; *) shift ;; esac
done
cp `+src+`/ethereum__blocks__*.parquet "$out/network_560048__blocks__01647952_to_01647952.parquet"
`)

	tables, err := testFetcher(t, binary).Fetch(t.Context(), testGroup(t, "blocks"), 1647952, 1647952)

	require.NoError(t, err, "a zero-padded, non-ethereum-prefixed name must still be found")
	require.Equal(t, 1, tables["blocks"].Rows())
}
