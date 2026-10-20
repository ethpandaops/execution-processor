//go:build !embedded

package geth

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	pcommon "github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

var _ execution.StructLogStreamer = (*RPCNode)(nil)

// callFramesTracer records the opcode index, output and error of each call/create frame.
//
//go:embed call_frames_tracer.js
var callFramesTracer string

type callFramesResult struct {
	Frames []struct {
		Index  uint32 `json:"i"`
		Type   string `json:"t"`
		Output string `json:"o"`
		Error  string `json:"e"`
	} `json:"frames"`
	GasUsed uint64 `json:"gasUsed"`
	Error   string `json:"error"`
	Output  string `json:"output"`
}

// TraceCallFrames returns the transaction's call frames and outcome from a frame-only JS tracer.
func (n *RPCNode) TraceCallFrames(ctx context.Context, hash string) (*execution.CallFrameTrace, error) {
	ctx, cancel := withDefaultDeadline(ctx)
	defer cancel()

	config := map[string]any{"tracer": callFramesTracer}
	if deadline, ok := ctx.Deadline(); ok {
		config["timeout"] = max(time.Until(deadline).Round(time.Second), time.Second).String()
	}

	var rsp callFramesResult

	start := time.Now()
	err := n.rpcClient.CallContext(ctx, &rsp, "debug_traceTransaction", hash, config)

	n.recordTraceCall("debug_traceTransaction_frames", start, err)

	if err != nil {
		return nil, err
	}

	trace := &execution.CallFrameTrace{
		Frames:      make([]execution.CallFrame, len(rsp.Frames)),
		Gas:         rsp.GasUsed,
		Failed:      rsp.Error != "",
		ReturnValue: normalizeReturnValue(rsp.Output),
	}

	for i, f := range rsp.Frames {
		trace.Frames[i] = execution.CallFrame{
			Index:      f.Index,
			Type:       f.Type,
			ReturnData: execution.ReturnBufferAfter(f.Type, f.Output, f.Error),
		}
	}

	return trace, nil
}

// StreamStructLogs streams debug_traceTransaction structlogs to visit without holding the response in memory.
func (n *RPCNode) StreamStructLogs(
	ctx context.Context,
	hash string,
	opts execution.TraceOptions,
	visit func(*execution.StructLog) error,
) (*execution.TraceSummary, error) {
	ctx, cancel := withDefaultDeadline(ctx)
	defer cancel()

	start := time.Now()
	summary, err := n.streamStructLogs(ctx, hash, opts, visit)

	n.recordTraceCall("debug_traceTransaction_stream", start, err)

	return summary, err
}

func (n *RPCNode) streamStructLogs(
	ctx context.Context,
	hash string,
	opts execution.TraceOptions,
	visit func(*execution.StructLog) error,
) (*execution.TraceSummary, error) {
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "debug_traceTransaction",
		"params":  getTraceParams(ctx, hash, opts),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to encode trace request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.config.NodeAddress, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to build trace request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := n.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))

		return nil, fmt.Errorf("trace request failed: %s: %s", resp.Status, strings.TrimSpace(string(snippet)))
	}

	return decodeStructLogStream(resp.Body, visit)
}

// decodeStructLogStream walks a debug_traceTransaction JSON-RPC response, handing each structlog to visit as it is decoded.
func decodeStructLogStream(r io.Reader, visit func(*execution.StructLog) error) (*execution.TraceSummary, error) {
	dec := json.NewDecoder(r)

	if err := expectDelim(dec, '{'); err != nil {
		return nil, err
	}

	var (
		summary   *execution.TraceSummary
		gotResult bool
	)

	for dec.More() {
		key, err := readKey(dec)
		if err != nil {
			return nil, err
		}

		switch key {
		case "error":
			var rpcErr struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			}

			if decodeErr := dec.Decode(&rpcErr); decodeErr != nil {
				return nil, fmt.Errorf("failed to decode rpc error: %w", decodeErr)
			}

			return nil, fmt.Errorf("rpc error %d: %s", rpcErr.Code, rpcErr.Message)
		case "result":
			summary, err = decodeTraceResult(dec, visit)
			if err != nil {
				return nil, err
			}

			gotResult = true
		default:
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return nil, fmt.Errorf("failed to skip %q: %w", key, err)
			}
		}
	}

	if !gotResult {
		return nil, errors.New("trace response has no result")
	}

	return summary, nil
}

func decodeTraceResult(dec *json.Decoder, visit func(*execution.StructLog) error) (*execution.TraceSummary, error) {
	if err := expectDelim(dec, '{'); err != nil {
		return nil, err
	}

	summary := &execution.TraceSummary{}

	var (
		raw erigonStructLog
		sl  execution.StructLog
	)

	for dec.More() {
		key, err := readKey(dec)
		if err != nil {
			return nil, err
		}

		switch key {
		case "structLogs":
			if err := expectDelim(dec, '['); err != nil {
				return nil, err
			}

			for dec.More() {
				raw = erigonStructLog{}
				if err := dec.Decode(&raw); err != nil {
					return nil, fmt.Errorf("failed to decode structlog: %w", err)
				}

				sl = execution.StructLog{
					PC:      raw.PC,
					Op:      raw.Op,
					Gas:     raw.Gas,
					GasCost: raw.GasCost,
					Depth:   raw.Depth,
					Refund:  raw.Refund,
					Error:   raw.Error,
					Stack:   raw.Stack,
				}

				execution.SanitizeGasCost(&sl)

				if err := visit(&sl); err != nil {
					return nil, err
				}
			}

			if err := expectDelim(dec, ']'); err != nil {
				return nil, err
			}
		case "gas":
			if err := dec.Decode(&summary.Gas); err != nil {
				return nil, fmt.Errorf("failed to decode gas: %w", err)
			}
		case "failed":
			if err := dec.Decode(&summary.Failed); err != nil {
				return nil, fmt.Errorf("failed to decode failed: %w", err)
			}
		case "returnValue":
			var rv string
			if err := dec.Decode(&rv); err != nil {
				return nil, fmt.Errorf("failed to decode returnValue: %w", err)
			}

			summary.ReturnValue = normalizeReturnValue(rv)
		default:
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return nil, fmt.Errorf("failed to skip %q: %w", key, err)
			}
		}
	}

	if err := expectDelim(dec, '}'); err != nil {
		return nil, err
	}

	return summary, nil
}

func readKey(dec *json.Decoder) (string, error) {
	tok, err := dec.Token()
	if err != nil {
		return "", fmt.Errorf("failed to read trace response: %w", err)
	}

	key, ok := tok.(string)
	if !ok {
		return "", fmt.Errorf("unexpected token %v in trace response", tok)
	}

	return key, nil
}

func expectDelim(dec *json.Decoder, want json.Delim) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("failed to read trace response: %w", err)
	}

	if d, ok := tok.(json.Delim); !ok || d != want {
		return fmt.Errorf("unexpected token %v in trace response, want %v", tok, want)
	}

	return nil
}

// normalizeReturnValue maps an empty or bare 0x return value to nil, matching the non-streamed trace.
func normalizeReturnValue(v string) *string {
	if v == "" || v == "0x" {
		return nil
	}

	return &v
}

func withDefaultDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, 60*time.Second)
}

func (n *RPCNode) recordTraceCall(method string, start time.Time, err error) {
	status := statusSuccess
	if err != nil {
		status = statusError
	}

	network := "unknown"
	if md := n.Metadata(); md != nil {
		network = fmt.Sprintf("%d", md.ChainID())
	}

	pcommon.RPCCallDuration.WithLabelValues(network, n.config.Name, method, status).Observe(time.Since(start).Seconds())
	pcommon.RPCCallsTotal.WithLabelValues(network, n.config.Name, method, status).Inc()
}
