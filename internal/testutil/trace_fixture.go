package testutil

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// Trace fixture files in testdata, recorded from real transactions.
const (
	// TraceFixtureCreate is hoodi tx 0x8c246fbf (block 3847147): a CREATE2 and a CREATE, each from a nested call.
	TraceFixtureCreate = "hoodi_create_create2_0x8c246fbf.json.gz"
	// TraceFixtureEOACall is hoodi tx 0xa133dd8c (block 3847107): a CALL that sends value to an EOA.
	TraceFixtureEOACall = "hoodi_eoa_call_0xa133dd8c.json.gz"
)

// CallTraceFrame is one frame of a callTracer result.
type CallTraceFrame struct {
	Type  string           `json:"type"`
	From  string           `json:"from"`
	To    string           `json:"to"`
	Error string           `json:"error"`
	Calls []CallTraceFrame `json:"calls"`
}

// TraceFixture is a transaction's RPC structlog trace (stack only) and its callTracer result.
type TraceFixture struct {
	Network          string         `json:"network"`
	BlockNumber      uint64         `json:"blockNumber"`
	TransactionHash  string         `json:"transactionHash"`
	TransactionIndex int            `json:"transactionIndex"`
	CallTrace        CallTraceFrame `json:"callTrace"`

	Trace struct {
		Gas         uint64                `json:"gas"`
		Failed      bool                  `json:"failed"`
		ReturnValue string                `json:"returnValue"`
		StructLogs  []execution.StructLog `json:"structLogs"`
	} `json:"trace"`
}

// LoadTraceFixture reads a fixture from this package's testdata directory.
func LoadTraceFixture(t *testing.T, name string) *TraceFixture {
	t.Helper()

	f, err := os.Open(filepath.Join(fixtureDir(), name))
	if err != nil {
		t.Fatalf("open trace fixture: %v", err)
	}
	defer f.Close()

	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("read trace fixture: %v", err)
	}
	defer zr.Close()

	var fx TraceFixture
	if err := json.NewDecoder(zr).Decode(&fx); err != nil {
		t.Fatalf("decode trace fixture: %v", err)
	}

	return &fx
}

// fixtureDir is this package's testdata directory, independent of the calling test's working directory.
func fixtureDir() string {
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		return "testdata"
	}

	return filepath.Join(filepath.Dir(self), "testdata")
}

// RPCTrace returns the trace as the RPC node returns it: stacks, no tracer-computed fields.
func (fx *TraceFixture) RPCTrace() *execution.TraceTransaction {
	trace := &execution.TraceTransaction{
		Gas:        fx.Trace.Gas,
		Failed:     fx.Trace.Failed,
		Structlogs: make([]execution.StructLog, len(fx.Trace.StructLogs)),
	}

	if rv := strings.TrimPrefix(fx.Trace.ReturnValue, "0x"); rv != "" {
		trace.ReturnValue = &rv
	}

	for i := range fx.Trace.StructLogs {
		sl := fx.Trace.StructLogs[i]

		if sl.Stack != nil {
			stack := append([]string(nil), *sl.Stack...)
			sl.Stack = &stack
		}

		trace.Structlogs[i] = sl
	}

	execution.SanitizeStructLogs(trace.Structlogs)

	return trace
}

// CreatedAddresses returns the address each CREATE/CREATE2 frame deployed, in execution order, per the callTracer.
func (fx *TraceFixture) CreatedAddresses() []string {
	var out []string

	var walk func(f *CallTraceFrame)

	walk = func(f *CallTraceFrame) {
		if f.Type == "CREATE" || f.Type == "CREATE2" {
			out = append(out, strings.ToLower(f.To))
		}

		for i := range f.Calls {
			walk(&f.Calls[i])
		}
	}

	walk(&fx.CallTrace)

	return out
}

// EmbeddedTrace returns the trace as erigone's StructLogTracer emits it: targets and GasUsed filled, no stacks.
func (fx *TraceFixture) EmbeddedTrace() *execution.TraceTransaction {
	trace := fx.RPCTrace()
	logs := trace.Structlogs

	type pendingCreate struct {
		index int
		depth uint64
	}

	var creates []pendingCreate

	pendingGas := map[uint64]int{}

	for i := range logs {
		sl := &logs[i]

		for d := range pendingGas {
			if d > sl.Depth {
				delete(pendingGas, d)
			}
		}

		if idx, ok := pendingGas[sl.Depth]; ok {
			logs[idx].GasUsed = logs[idx].Gas - sl.Gas
		}

		for len(creates) > 0 && sl.Depth <= creates[len(creates)-1].depth {
			if top := stackWord(sl, 0); top != "" {
				addr := wordToAddress(top)
				logs[creates[len(creates)-1].index].CallToAddress = &addr
			}

			creates = creates[:len(creates)-1]
		}

		sl.GasUsed = sl.GasCost

		switch sl.Op {
		case "CALL", "CALLCODE", "DELEGATECALL", "STATICCALL":
			if target := stackWord(sl, 1); target != "" {
				addr := wordToAddress(target)
				sl.CallToAddress = &addr
			}

			if sl.Op == "CALL" || sl.Op == "CALLCODE" {
				value := strings.TrimLeft(strings.TrimPrefix(stackWord(sl, 2), "0x"), "0")
				sl.CallTransfersValue = value != ""
			}
		case "CREATE", "CREATE2":
			creates = append(creates, pendingCreate{index: i, depth: sl.Depth})
		}

		pendingGas[sl.Depth] = i
	}

	for i := range logs {
		logs[i].Stack = nil
	}

	return trace
}

// stackWord returns the stack item n positions below the top, or "" if the stack is shorter.
func stackWord(sl *execution.StructLog, n int) string {
	if sl.Stack == nil || len(*sl.Stack) <= n {
		return ""
	}

	return (*sl.Stack)[len(*sl.Stack)-1-n]
}

// wordToAddress takes the low 20 bytes of a stack word, as the tracer's uint256.Bytes20 does.
func wordToAddress(word string) string {
	h := strings.ToLower(strings.TrimPrefix(word, "0x"))
	if len(h) > 40 {
		h = h[len(h)-40:]
	}

	return "0x" + strings.Repeat("0", 40-len(h)) + h
}
