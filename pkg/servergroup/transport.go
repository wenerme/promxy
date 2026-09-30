package servergroup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	serverGroupCoalescedRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "server_group_coalesced_requests_total",
		Help: "Number of downstream requests that were served by joining an identical in-flight request",
	}, []string{"host"})
)

func init() {
	prometheus.MustRegister(serverGroupCoalescedRequests)
}

// coalesceTransport merges identical concurrent read requests into a single
// downstream request (singleflight). The response is fully buffered and each
// caller receives its own copy. The shared request is canceled only when every
// caller waiting on it has given up.
//
// Memory: the buffered body is held once per distinct in-flight request, in
// addition to the copy the prometheus API client reads. The shared buffer is
// released as soon as the callers have copied it; the decoded model.Value that
// promxy builds afterwards is typically much larger than either.
type coalesceTransport struct {
	next http.RoundTripper

	mu    sync.Mutex
	calls map[string]*coalescedCall
}

type coalescedCall struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int

	resp *http.Response
	body []byte
	err  error
}

func newCoalesceTransport(next http.RoundTripper) *coalesceTransport {
	return &coalesceTransport{next: next, calls: make(map[string]*coalescedCall)}
}

// coalescablePaths are the read-only prometheus API endpoints promxy sends downstream.
var coalescablePaths = []string{
	"/api/v1/query",
	"/api/v1/query_range",
	"/api/v1/series",
	"/api/v1/labels",
	"/api/v1/label/",
}

func isCoalescable(r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		return false
	}
	if r.Header.Get("Range") != "" {
		return false
	}
	for _, p := range coalescablePaths {
		if strings.HasSuffix(r.URL.Path, p) || (strings.HasSuffix(p, "/") && strings.Contains(r.URL.Path, p)) {
			return true
		}
	}
	return false
}

// coalesceKey returns a key covering everything that can influence the response,
// along with a request whose body can still be sent.
func coalesceKey(r *http.Request) (string, *http.Request, error) {
	h := sha256.New()
	io.WriteString(h, r.Method)
	h.Write([]byte{0})
	io.WriteString(h, r.URL.String())
	h.Write([]byte{0})
	io.WriteString(h, r.Host)
	h.Write([]byte{0})

	names := make([]string, 0, len(r.Header))
	for name := range r.Header {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		io.WriteString(h, name)
		h.Write([]byte{':'})
		for _, v := range r.Header[name] {
			io.WriteString(h, v)
			h.Write([]byte{0})
		}
		h.Write([]byte{0})
	}

	if r.Body != nil && r.Body != http.NoBody {
		body, err := io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			return "", nil, err
		}
		h.Write(body)

		r = r.Clone(r.Context())
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(body)), nil
		}
		r.ContentLength = int64(len(body))
	}

	return hex.EncodeToString(h.Sum(nil)), r, nil
}

func (t *coalesceTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if !isCoalescable(r) {
		return t.next.RoundTrip(r)
	}

	key, r, err := coalesceKey(r)
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	if call, ok := t.calls[key]; ok {
		call.waiters++
		t.mu.Unlock()
		serverGroupCoalescedRequests.WithLabelValues(r.URL.Host).Inc()
		return t.wait(r, key, call)
	}

	// The shared request keeps the caller's context values but not its deadline or
	// cancellation, each caller enforces its own deadline in wait().
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	call := &coalescedCall{done: make(chan struct{}), cancel: cancel, waiters: 1}
	t.calls[key] = call
	t.mu.Unlock()

	go t.execute(r.WithContext(ctx), key, call)

	return t.wait(r, key, call)
}

func (t *coalesceTransport) execute(r *http.Request, key string, call *coalescedCall) {
	resp, err := t.next.RoundTrip(r)
	if err == nil {
		var body []byte
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err == nil {
			call.resp = resp
			call.body = body
		}
	}
	call.err = err
	call.cancel()

	t.mu.Lock()
	if t.calls[key] == call {
		delete(t.calls, key)
	}
	t.mu.Unlock()

	close(call.done)
}

// wait blocks until the shared call completes or the caller's own context is done.
func (t *coalesceTransport) wait(r *http.Request, key string, call *coalescedCall) (*http.Response, error) {
	ctx := r.Context()
	select {
	case <-call.done:
		if call.err != nil {
			return nil, call.err
		}
		return call.response(r), nil

	case <-ctx.Done():
		t.mu.Lock()
		call.waiters--
		if call.waiters == 0 {
			// Nobody is waiting anymore, stop the downstream request and make sure
			// new callers don't join a canceled request.
			call.cancel()
			if t.calls[key] == call {
				delete(t.calls, key)
			}
		}
		t.mu.Unlock()
		return nil, ctx.Err()
	}
}

// response returns a copy of the buffered response for a single caller.
func (c *coalescedCall) response(r *http.Request) *http.Response {
	resp := *c.resp
	resp.Request = r
	resp.Header = c.resp.Header.Clone()
	resp.Trailer = c.resp.Trailer.Clone()
	resp.Body = io.NopCloser(bytes.NewReader(c.body))
	resp.ContentLength = int64(len(c.body))
	return &resp
}

// inflightWaiters returns the total number of callers waiting on in-flight calls (used in tests).
func (t *coalesceTransport) inflightWaiters() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, c := range t.calls {
		n += c.waiters
	}
	return n
}
