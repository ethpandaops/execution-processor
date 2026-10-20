//go:build !embedded

package geth

import (
	"fmt"
	"io"
	"net/http"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// limitTransport fails a response once its body passes limit bytes instead of letting the caller buffer all of it.
type limitTransport struct {
	base  http.RoundTripper
	limit int64
}

func (t *limitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	if resp.ContentLength > t.limit {
		_ = resp.Body.Close()

		return nil, fmt.Errorf("%w: %d bytes announced, limit %d", execution.ErrResponseTooLarge, resp.ContentLength, t.limit)
	}

	resp.Body = &limitedBody{ReadCloser: resp.Body, remaining: t.limit, limit: t.limit}

	return resp, nil
}

type limitedBody struct {
	io.ReadCloser

	remaining int64
	limit     int64
}

func (b *limitedBody) Read(p []byte) (int, error) {
	if b.remaining <= 0 {
		var probe [1]byte

		n, err := b.ReadCloser.Read(probe[:])
		if n > 0 {
			return 0, fmt.Errorf("%w: more than %d bytes", execution.ErrResponseTooLarge, b.limit)
		}

		return 0, err
	}

	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}

	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)

	return n, err
}
