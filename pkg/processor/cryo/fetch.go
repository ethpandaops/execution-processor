package cryo

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

const (
	// tempDirPattern marks every scratch directory so the startup sweep can
	// tell them apart from anything else sharing the temp filesystem.
	tempDirPattern = "cryo-*"

	// killGrace is how long a killed cryo has to exit before Wait gives up on
	// it, so a wedged subprocess cannot hold a worker forever.
	killGrace = 5 * time.Second

	// stderrLimit bounds what is retained from a failing cryo, so a chatty
	// failure cannot grow without limit.
	stderrLimit = 8 << 10

	// sweepMinAge is how old an orphan must look before startup removes it.
	// Replicas can share a temp filesystem, so this must comfortably exceed any
	// plausible in-flight fetch or one replica will delete another's working
	// directory mid-collection.
	sweepMinAge = time.Hour

	// redacted replaces the RPC endpoint wherever cryo echoes it back, because
	// the endpoint usually carries basic-auth credentials and a failed fetch's
	// error text ends up in the logs.
	redacted = "[redacted]"
)

// Fetch failure causes. Every error Fetch returns wraps exactly one of these,
// so a caller can attribute the failure without matching on message text.
var (
	// ErrNoEndpoint means the pool offered no healthy node to collect from, so
	// cryo was never started.
	ErrNoEndpoint = errors.New("no rpc endpoint available")

	// ErrFetchTimeout means the cryo subprocess outlived its fetch timeout and
	// was killed.
	ErrFetchTimeout = errors.New("cryo subprocess timed out")

	// ErrFetchFailed means cryo ran and exited non-zero.
	ErrFetchFailed = errors.New("cryo subprocess failed")

	// ErrParquetMissing means cryo exited successfully without writing the
	// output for a dataset the group expects, which must never be read as a
	// block that legitimately had no rows.
	ErrParquetMissing = errors.New("cryo wrote no parquet")

	// ErrParquetAmbiguous means more than one file matched a dataset, so the
	// one holding this block's rows cannot be identified.
	ErrParquetAmbiguous = errors.New("cryo wrote more than one parquet")

	// ErrDecodeFailed means the parquet cryo wrote could not be decoded.
	ErrDecodeFailed = errors.New("parquet decode failed")
)

// boundedBuffer keeps at most stderrLimit bytes and counts what it discards.
type boundedBuffer struct {
	buf     []byte
	dropped int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	room := stderrLimit - len(b.buf)

	switch {
	case room <= 0:
		b.dropped += len(p)
	case room >= len(p):
		b.buf = append(b.buf, p...)
	default:
		b.buf = append(b.buf, p[:room]...)
		b.dropped += len(p) - room
	}

	return len(p), nil
}

// String renders what was kept. The byte cap can land mid-rune, so the result
// is scrubbed to valid UTF-8 before it reaches an error or a log line.
func (b *boundedBuffer) String() string {
	kept := strings.ToValidUTF8(string(b.buf), "")

	if b.dropped == 0 {
		return kept
	}

	return fmt.Sprintf("%s… (%d more bytes)", kept, b.dropped)
}

// Fetcher retrieves decoded rows for one group over a contiguous block range,
// keyed by dataset name.
//
// Per-block processing calls this with from == to. A range-batched
// implementation would change only this seam and the enqueue path, not the
// tracking model, so cryo's command line must not leak past it.
type Fetcher interface {
	Fetch(ctx context.Context, g *Group, from, to uint64) (map[string]*decode.Table, error)
}

// endpointFunc resolves the JSON-RPC URL for one invocation. It is called per
// fetch rather than captured once so that cryo follows the pool away from an
// unhealthy node.
type endpointFunc func() (string, error)

// execFetcher runs cryo as a subprocess and decodes the parquet it writes.
type execFetcher struct {
	log        logrus.FieldLogger
	binaryPath string
	endpoint   endpointFunc
	tempDir    string
	globalArgs []string
	timeout    time.Duration
}

func newExecFetcher(log logrus.FieldLogger, cfg *Config, endpoint endpointFunc) *execFetcher {
	return &execFetcher{
		log:        log,
		binaryPath: cfg.BinaryPath,
		endpoint:   endpoint,
		tempDir:    cfg.TempDir,
		globalArgs: cfg.GlobalArgs,
		timeout:    cfg.FetchTimeout,
	}
}

// Version returns the bundled cryo's version string.
func (f *execFetcher) Version(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	//nolint:gosec // G204: the binary path is operator configuration, not request data
	out, err := exec.CommandContext(ctx, f.binaryPath, "--version").Output()
	if err != nil {
		return "", fmt.Errorf("exec %s --version: %w%s", f.binaryPath, err, exitStderr(err))
	}

	return strings.TrimSpace(string(out)), nil
}

// exitStderr renders what an *exec.ExitError captured on stderr, or an empty
// string when there is none. Without it the error reads only "exit status 127",
// which hides the reason a binary that exists still will not run — a missing
// shared library such as libssl.so.3 says so only on stderr.
func exitStderr(err error) string {
	var exitErr *exec.ExitError

	if !errors.As(err, &exitErr) {
		return ""
	}

	text := strings.TrimSpace(string(exitErr.Stderr))
	if text == "" {
		return ""
	}

	return ": " + truncate(text, stderrLimit)
}

// Fetch runs one cryo invocation and decodes every dataset the group expects.
func (f *execFetcher) Fetch(ctx context.Context, g *Group, from, to uint64) (map[string]*decode.Table, error) {
	dir, err := os.MkdirTemp(f.tempDir, tempDirPattern)
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}

	defer func() {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			f.log.WithError(rmErr).WithField("dir", dir).Error("Failed to remove cryo temp dir")
		}
	}()

	if err := f.run(ctx, g, from, to, dir); err != nil {
		return nil, err
	}

	collected := g.collected()
	tables := make(map[string]*decode.Table, len(collected))

	for _, ds := range collected {
		path, findErr := findParquet(dir, ds.Name, from, to)
		if findErr != nil {
			return nil, findErr
		}

		table, decodeErr := decode.File(path)
		if decodeErr != nil {
			return nil, fmt.Errorf("%w: dataset %s: %w", ErrDecodeFailed, ds.Name, decodeErr)
		}

		tables[ds.Name] = table
	}

	return tables, nil
}

// run executes cryo once for the whole group.
func (f *execFetcher) run(ctx context.Context, g *Group, from, to uint64, dir string) error {
	ctx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()

	rpcURL, err := f.endpoint()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrNoEndpoint, err)
	}

	// cryo's range end is exclusive.
	args := make([]string, 0, len(g.Datatypes)+len(f.globalArgs)+len(g.ExtraArgs)+6)
	args = append(args, g.Datatypes...)
	args = append(args,
		"--blocks", fmt.Sprintf("%d:%d", from, to+1),
		"--output-dir", dir,
		"--no-report",
	)
	args = append(args, f.globalArgs...)

	if required := g.RequiredColumns(); len(required) > 0 {
		args = append(args, "--include-columns", strings.Join(required, ","))
	}

	args = append(args, g.ExtraArgs...)

	//nolint:gosec // G204: the binary path and arguments are operator configuration, not request data
	cmd := exec.CommandContext(ctx, f.binaryPath, args...)

	// The endpoint usually carries basic-auth credentials, so it goes through
	// the environment rather than argv, which any process sharing the PID
	// namespace can read out of /proc.
	cmd.Env = append(os.Environ(), "ETH_RPC_URL="+rpcURL)

	// Killing a hung cryo must also kill anything it spawned: a grandchild that
	// outlived its parent would keep an RPC connection open against a node the
	// worker has already given up on. WaitDelay additionally stops the parent
	// blocking on pipes that group inherited.
	killProcessGroup(cmd)

	cmd.WaitDelay = killGrace

	var stderr boundedBuffer

	cmd.Stderr = &stderr
	cmd.Stdout = nil

	start := time.Now()

	if err := cmd.Run(); err != nil {
		cause := ErrFetchFailed
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			cause = ErrFetchTimeout
		}

		// Redact before truncating: cutting the text first could leave a
		// fragment of the credentials behind.
		detail := truncate(redactEndpoint(stderr.String(), rpcURL), 2048)

		return fmt.Errorf("%w: datatypes %s blocks %d-%d: %w: %s",
			cause, strings.Join(g.Datatypes, " "), from, to, err, detail)
	}

	f.log.WithFields(logrus.Fields{
		"group":    g.Name,
		"from":     from,
		"to":       to,
		"duration": time.Since(start),
	}).Debug("cryo invocation complete")

	return nil
}

// findParquet locates the file cryo wrote for one dataset.
//
// Only the dataset is matched. cryo decorates the rest of the name in ways that
// are not worth predicting: the prefix is whatever it resolved the chain id to,
// and the block numbers are zero-padded to a width it chooses, so a chain whose
// heights are shorter than mainnet's produces 01647952 where the request said
// 1647952. The invocation writes into a directory of its own, so the dataset
// alone identifies the file, and two matches are treated as an error rather
// than guessed between.
func findParquet(dir, dataset string, from, to uint64) (string, error) {
	pattern := filepath.Join(dir, fmt.Sprintf("*__%s__*.parquet", dataset))

	matches, err := filepath.Glob(pattern)
	if err != nil {
		return "", fmt.Errorf("glob %s: %w", pattern, err)
	}

	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return "", fmt.Errorf("%w for dataset %s at blocks %d-%d", ErrParquetMissing, dataset, from, to)
	default:
		return "", fmt.Errorf("%w: %d files for dataset %s at blocks %d-%d", ErrParquetAmbiguous, len(matches), dataset, from, to)
	}
}

// redactEndpoint removes the RPC endpoint from text cryo produced. cryo echoes
// its target in several failure messages and the endpoint usually carries
// basic-auth credentials, which would otherwise travel with the error into the
// logs. The credentials are also matched on their own, because cryo may print a
// normalised or partial form of the URL.
func redactEndpoint(text, endpoint string) string {
	if endpoint == "" || text == "" {
		return text
	}

	for _, form := range []string{endpoint, strings.TrimSuffix(endpoint, "/")} {
		text = strings.ReplaceAll(text, form, redacted)
	}

	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.User == nil {
		return text
	}

	if userinfo := parsed.User.String(); userinfo != "" {
		text = strings.ReplaceAll(text, userinfo, redacted)
	}

	if password, ok := parsed.User.Password(); ok && password != "" {
		text = strings.ReplaceAll(text, password, redacted)
	}

	return text
}

// sweepTempDirs removes scratch directories left behind by a process that was
// killed before its deferred cleanup could run. Without this they accumulate in
// the container's writable layer until the node evicts the pod for disk
// pressure.
func sweepTempDirs(log logrus.FieldLogger, parent string) {
	if parent == "" {
		parent = os.TempDir()
	}

	matches, err := filepath.Glob(filepath.Join(parent, tempDirPattern))
	if err != nil {
		log.WithError(err).Warn("Failed to scan for orphaned cryo temp dirs")

		return
	}

	var removed int

	cutoff := time.Now().Add(-sweepMinAge)

	for _, dir := range matches {
		info, statErr := os.Stat(dir)
		if statErr != nil || !info.IsDir() {
			continue
		}

		if info.ModTime().After(cutoff) {
			continue
		}

		if rmErr := os.RemoveAll(dir); rmErr != nil {
			log.WithError(rmErr).WithField("dir", dir).Warn("Failed to remove orphaned cryo temp dir")

			continue
		}

		removed++
	}

	if removed > 0 {
		log.WithField("count", removed).Info("Removed orphaned cryo temp dirs")
	}
}

// truncate keeps at most limit bytes of s. The cut is moved back to a rune
// boundary so a multi-byte character straddling the limit is dropped whole
// rather than left as invalid UTF-8 in a log line.
func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}

	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}

	return s[:cut] + "…"
}
