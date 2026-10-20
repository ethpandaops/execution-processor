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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// Shape of mainnet tx 0xa121687c's trace with return data: a 394,464-byte buffer on 468,867 of its opcodes.
const (
	poisonReturnDataBytes = 394464
	poisonOpcodesWithData = 468867
)

type traceServer struct {
	written atomic.Int64
	params  atomic.Value
}

// serve streams a debug_traceTransaction result whose every structlog carries the poison-sized return data.
func (s *traceServer) serve(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     json.RawMessage `json:"id"`
		Params []any           `json:"params"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	s.params.Store(req.Params)

	w.Header().Set("Content-Type", "application/json")

	bw := bufio.NewWriterSize(w, 1<<20)
	returnData := strings.Repeat("ab", poisonReturnDataBytes)

	write := func(str string) bool {
		n, err := bw.WriteString(str)
		s.written.Add(int64(n))

		return err == nil
	}

	if !write(fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{"gas":11409124,"failed":false,"structLogs":[`, req.ID)) {
		return
	}

	for i := 0; i < poisonOpcodesWithData; i++ {
		sep := ","
		if i == 0 {
			sep = ""
		}

		if !write(fmt.Sprintf(`%s{"pc":%d,"op":"MLOAD","gas":1000,"gasCost":3,"depth":2,"returnData":"0x%s"}`, sep, i, returnData)) {
			return
		}
	}

	write(`]}}`)

	_ = bw.Flush()
}

func newLimitedRPCClient(t *testing.T, url string, limit int64) *rpc.Client {
	t.Helper()

	client := &http.Client{Transport: &limitTransport{base: http.DefaultTransport, limit: limit}}

	rpcClient, err := rpc.DialOptions(context.Background(), url, rpc.WithHTTPClient(client))
	require.NoError(t, err)

	t.Cleanup(rpcClient.Close)

	return rpcClient
}

func TestTraceResponseOverLimitIsRefused(t *testing.T) {
	srv := &traceServer{}
	server := httptest.NewServer(http.HandlerFunc(srv.serve))
	t.Cleanup(server.Close)

	const limit = 8 << 20

	rpcClient := newLimitedRPCClient(t, server.URL, limit)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var rsp erigonResult

	err := rpcClient.CallContext(ctx, &rsp, "debug_traceTransaction", getTraceParams(ctx, "0xa121687c", execution.StackTraceOptions())...)
	require.ErrorIs(t, err, execution.ErrResponseTooLarge)

	// The full response would be ~370GB; the server must have stopped soon after the limit.
	server.CloseClientConnections()
	require.Less(t, srv.written.Load(), int64(256<<20))

	params, ok := srv.params.Load().([]any)
	require.True(t, ok)
	require.Len(t, params, 2)

	config, ok := params[1].(map[string]any)
	require.True(t, ok)
	require.Equal(t, true, config["enableReturnData"])

	timeout, ok := config["timeout"].(string)
	require.True(t, ok, "trace config carries a node-side timeout")

	parsed, err := time.ParseDuration(timeout)
	require.NoError(t, err)
	require.LessOrEqual(t, parsed, 30*time.Second)
}

func TestResponseUnderLimitIsDecoded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		var req struct {
			ID json.RawMessage `json:"id"`
		}

		_ = json.Unmarshal(body, &req)

		_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"gas":21000,"failed":false,"structLogs":[{"pc":0,"op":"STOP","gas":0,"gasCost":0,"depth":1}]}}`, req.ID)
	}))
	t.Cleanup(server.Close)

	rpcClient := newLimitedRPCClient(t, server.URL, 1<<20)

	var rsp erigonResult

	require.NoError(t, rpcClient.CallContext(context.Background(), &rsp, "debug_traceTransaction", getTraceParams(context.Background(), "0x01", execution.CallFrameTraceOptions())...))
	require.Len(t, rsp.StructLogs, 1)
	require.Equal(t, uint64(21000), rsp.Gas)
}

func TestAnnouncedOversizedResponseIsRefused(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(2<<20))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	rpcClient := newLimitedRPCClient(t, server.URL, 1<<20)

	var rsp erigonResult

	err := rpcClient.CallContext(context.Background(), &rsp, "debug_traceTransaction", getTraceParams(context.Background(), "0x01", execution.CallFrameTraceOptions())...)
	require.ErrorIs(t, err, execution.ErrResponseTooLarge)
}

func TestDefaultLimitRefusesPoisonTrace(t *testing.T) {
	require.Less(t, execution.DefaultMaxResponseBytes, int64(poisonOpcodesWithData)*poisonReturnDataBytes*2)
}
