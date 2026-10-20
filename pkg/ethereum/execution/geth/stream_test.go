//go:build !embedded

package geth

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// writeStructLogResponse writes a debug_traceTransaction response with n structlogs, the outcome after them like erigon does.
func writeStructLogResponse(w io.Writer, n int) error {
	bw := bufio.NewWriterSize(w, 1<<16)

	if _, err := bw.WriteString(`{"jsonrpc":"2.0","id":1,"result":{"structLogs":[`); err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		sep := ","
		if i == 0 {
			sep = ""
		}

		if _, err := fmt.Fprintf(bw, `%s{"pc":%d,"op":"ADD","gas":%d,"gasCost":3,"depth":1,"stack":["0x1","0x2"]}`, sep, i, 1<<40-uint64(i)*3); err != nil {
			return err
		}
	}

	if _, err := bw.WriteString(`],"gas":21000,"failed":true,"returnValue":"0xdead"}}`); err != nil {
		return err
	}

	return bw.Flush()
}

func TestDecodeStructLogStreamHoldsOneStructLog(t *testing.T) {
	const n = 2_000_000

	pr, pw := io.Pipe()

	go func() {
		pw.CloseWithError(writeStructLogResponse(pw, n))
	}()

	runtime.GC()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	var (
		count    int
		outOfSeq int
		maxHeap  uint64
	)

	summary, err := decodeStructLogStream(pr, func(sl *execution.StructLog) error {
		if int(sl.PC) != count {
			outOfSeq++
		}

		count++

		if count%100_000 == 0 {
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			maxHeap = max(maxHeap, ms.HeapAlloc)
		}

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, n, count)
	require.Zero(t, outOfSeq)
	require.Equal(t, uint64(21000), summary.Gas)
	require.True(t, summary.Failed)
	require.Equal(t, "0xdead", *summary.ReturnValue)

	// About 170MB of JSON streams through; the decoder must not accumulate it.
	require.Less(t, maxHeap, before.HeapAlloc+64<<20)
}

func TestDecodeStructLogStreamAnyKeyOrder(t *testing.T) {
	body := `{"result":{"gas":7,"failed":false,"returnValue":"0x","structLogs":[{"pc":0,"op":"STOP","gas":7,"gasCost":0,"depth":1}]},"id":1,"jsonrpc":"2.0"}`

	var ops []string

	summary, err := decodeStructLogStream(strings.NewReader(body), func(sl *execution.StructLog) error {
		ops = append(ops, sl.Op)

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"STOP"}, ops)
	require.Equal(t, uint64(7), summary.Gas)
	require.Nil(t, summary.ReturnValue)
}

func TestDecodeStructLogStreamReturnsRPCError(t *testing.T) {
	_, err := decodeStructLogStream(strings.NewReader(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"transaction not found"}}`), func(*execution.StructLog) error {
		return nil
	})
	require.ErrorContains(t, err, "transaction not found")
}

func newTestRPCNode(t *testing.T, url string, limit int64) *RPCNode {
	t.Helper()

	client := &http.Client{Transport: &limitTransport{base: http.DefaultTransport, limit: limit}}

	rpcClient, err := rpc.DialOptions(context.Background(), url, rpc.WithHTTPClient(client))
	require.NoError(t, err)

	t.Cleanup(rpcClient.Close)

	return &RPCNode{
		config:     &execution.Config{NodeAddress: url, Name: "test"},
		rpcClient:  rpcClient,
		httpClient: client,
	}
}

func TestStreamStructLogsOverHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
			Params []any  `json:"params"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Method != "debug_traceTransaction" {
			http.Error(w, "bad request", http.StatusBadRequest)

			return
		}

		config, _ := req.Params[1].(map[string]any)
		if config["enableReturnData"] != false || config["timeout"] == nil {
			http.Error(w, "unexpected trace config", http.StatusBadRequest)

			return
		}

		_ = writeStructLogResponse(w, 1000)
	}))
	t.Cleanup(server.Close)

	node := newTestRPCNode(t, server.URL, 1<<30)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	count := 0

	summary, err := node.StreamStructLogs(ctx, "0x01", execution.CallFrameTraceOptions(), func(*execution.StructLog) error {
		count++

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1000, count)
	require.Equal(t, uint64(21000), summary.Gas)
}

func TestStreamStructLogsStopsAtResponseLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = writeStructLogResponse(w, 1_000_000)
	}))
	t.Cleanup(server.Close)

	node := newTestRPCNode(t, server.URL, 1<<20)

	_, err := node.StreamStructLogs(context.Background(), "0x01", execution.CallFrameTraceOptions(), func(*execution.StructLog) error {
		return nil
	})
	require.ErrorIs(t, err, execution.ErrResponseTooLarge)
}

func TestTraceCallFramesAppliesReturnBufferRules(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Params []any           `json:"params"`
		}

		_ = json.NewDecoder(r.Body).Decode(&req)

		config, _ := req.Params[1].(map[string]any)
		if tracer, _ := config["tracer"].(string); !strings.Contains(tracer, "enter") || config["timeout"] == nil {
			http.Error(w, "unexpected tracer config", http.StatusBadRequest)

			return
		}

		_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"frames":[
			{"i":3,"t":"CALL","o":"0xaabb","e":""},
			{"i":9,"t":"STATICCALL","o":"0xccdd","e":"execution reverted"},
			{"i":12,"t":"CALL","o":"","e":"out of gas"},
			{"i":20,"t":"CREATE","o":"0x6080","e":""},
			{"i":30,"t":"CREATE2","o":"0x08c379a0","e":"execution reverted"}
		],"gasUsed":50000,"error":"","output":"0x"}}`, req.ID)
	}))
	t.Cleanup(server.Close)

	node := newTestRPCNode(t, server.URL, 1<<20)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	trace, err := node.TraceCallFrames(ctx, "0x01")
	require.NoError(t, err)
	require.Equal(t, uint64(50000), trace.Gas)
	require.False(t, trace.Failed)
	require.Nil(t, trace.ReturnValue)
	require.Len(t, trace.Frames, 5)

	var buffers []string

	for _, f := range trace.Frames {
		if f.ReturnData == nil {
			buffers = append(buffers, "<empty>")

			continue
		}

		buffers = append(buffers, *f.ReturnData)
	}

	require.Equal(t, []string{"aabb", "ccdd", "<empty>", "<empty>", "08c379a0"}, buffers)
	require.Equal(t, []uint32{3, 9, 12, 20, 30}, []uint32{trace.Frames[0].Index, trace.Frames[1].Index, trace.Frames[2].Index, trace.Frames[3].Index, trace.Frames[4].Index})
}
