package servergroup

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"gopkg.in/yaml.v2"
)

// blockingServer counts requests and blocks every handler until release is closed.
type blockingServer struct {
	*httptest.Server
	hits     atomic.Int32
	inflight atomic.Int32
	started  chan struct{}
	release  chan struct{}
}

func newBlockingServer(t *testing.T) *blockingServer {
	s := &blockingServer{started: make(chan struct{}, 100), release: make(chan struct{})}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Consume the body first: the server only detects client disconnects
		// (canceling r.Context()) once the request body has been read.
		r.ParseForm()
		s.hits.Add(1)
		s.inflight.Add(1)
		s.started <- struct{}{}
		select {
		case <-s.release:
		case <-r.Context().Done():
		}
		s.inflight.Add(-1)
		io.WriteString(w, "result:"+r.Form.Get("query"))
	}))
	t.Cleanup(s.Close)
	return s
}

func postQuery(ctx context.Context, rt http.RoundTripper, base, query string) (string, error) {
	body := url.Values{"query": {query}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/v1/query", strings.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := rt.RoundTrip(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

func waitStarted(t *testing.T, s *blockingServer, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-s.started:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %d downstream requests, got %d", n, i)
		}
	}
}

// waitWaiters blocks until n callers are attached to in-flight coalesced calls.
func waitWaiters(t *testing.T, rt http.RoundTripper, n int) {
	t.Helper()
	ct, ok := rt.(*coalesceTransport)
	if !ok {
		t.Fatalf("expected *coalesceTransport, got %T", rt)
	}
	deadline := time.Now().Add(5 * time.Second)
	for ct.inflightWaiters() != n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d waiters, got %d", n, ct.inflightWaiters())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestCoalesceIdenticalRequests(t *testing.T) {
	s := newBlockingServer(t)
	rt := newCoalesceTransport(http.DefaultTransport)

	const n = 10
	var wg sync.WaitGroup
	results := make([]string, n)
	errs := make([]error, n)

	// Start the first request and make sure it reached the downstream before the others join.
	wg.Add(1)
	go func() {
		defer wg.Done()
		results[0], errs[0] = postQuery(context.Background(), rt, s.URL, "up")
	}()
	waitStarted(t, s, 1)

	for i := 1; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = postQuery(context.Background(), rt, s.URL, "up")
		}(i)
	}
	// Wait until every follower has joined the in-flight request.
	waitWaiters(t, rt, n)
	close(s.release)
	wg.Wait()

	if got := s.hits.Load(); got != 1 {
		t.Fatalf("expected 1 downstream request, got %d", got)
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil || results[i] != "result:up" {
			t.Fatalf("caller %d: got %q, err=%v", i, results[i], errs[i])
		}
	}
}

func TestCoalesceDifferentRequestsNotMerged(t *testing.T) {
	s := newBlockingServer(t)
	close(s.release)
	rt := newCoalesceTransport(http.DefaultTransport)

	var wg sync.WaitGroup
	for _, q := range []string{"a", "b", "c"} {
		wg.Add(1)
		go func(q string) {
			defer wg.Done()
			got, err := postQuery(context.Background(), rt, s.URL, q)
			if err != nil || got != "result:"+q {
				t.Errorf("query %q: got %q, err=%v", q, got, err)
			}
		}(q)
	}
	wg.Wait()

	if got := s.hits.Load(); got != 3 {
		t.Fatalf("expected 3 downstream requests, got %d", got)
	}
}

func TestCoalesceDifferentHeadersNotMerged(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		time.Sleep(50 * time.Millisecond)
		io.WriteString(w, r.Header.Get("X-Scope-OrgID"))
	}))
	defer srv.Close()
	rt := newCoalesceTransport(http.DefaultTransport)

	var wg sync.WaitGroup
	for _, tenant := range []string{"a", "b"} {
		wg.Add(1)
		go func(tenant string) {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/query?query=up", nil)
			req.Header.Set("X-Scope-OrgID", tenant)
			resp, err := rt.RoundTrip(req)
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			if string(b) != tenant {
				t.Errorf("tenant %q got response %q", tenant, b)
			}
		}(tenant)
	}
	wg.Wait()

	if got := hits.Load(); got != 2 {
		t.Fatalf("expected 2 downstream requests, got %d", got)
	}
}

func TestCoalesceCallerCancelDoesNotAffectOthers(t *testing.T) {
	s := newBlockingServer(t)
	rt := newCoalesceTransport(http.DefaultTransport)

	// The first (leader) caller gives up early, the second must still get the result.
	ctx, cancel := context.WithCancel(context.Background())
	leaderErr := make(chan error, 1)
	go func() {
		_, err := postQuery(ctx, rt, s.URL, "up")
		leaderErr <- err
	}()
	waitStarted(t, s, 1)

	followerRes := make(chan string, 1)
	go func() {
		got, err := postQuery(context.Background(), rt, s.URL, "up")
		if err != nil {
			t.Errorf("follower: %v", err)
		}
		followerRes <- got
	}()
	waitWaiters(t, rt, 2)

	cancel()
	if err := <-leaderErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader: expected context.Canceled, got %v", err)
	}

	close(s.release)
	if got := <-followerRes; got != "result:up" {
		t.Fatalf("follower got %q", got)
	}
	if got := s.hits.Load(); got != 1 {
		t.Fatalf("expected 1 downstream request, got %d", got)
	}
}

func TestCoalesceAllCallersCancelStopsDownstream(t *testing.T) {
	s := newBlockingServer(t)
	defer close(s.release)
	rt := newCoalesceTransport(http.DefaultTransport)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := postQuery(ctx, rt, s.URL, "up")
		done <- err
	}()
	waitStarted(t, s, 1)
	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	// The downstream handler should observe the cancellation.
	deadline := time.Now().Add(5 * time.Second)
	for s.inflight.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("downstream request was not canceled")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// A new identical request must not join the canceled one.
	go postQuery(context.Background(), rt, s.URL, "up")
	waitStarted(t, s, 1)
	if got := s.hits.Load(); got != 2 {
		t.Fatalf("expected a fresh downstream request, got %d hits", got)
	}
}

func TestCoalesceSkipsNonReadPaths(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		time.Sleep(50 * time.Millisecond)
	}))
	defer srv.Close()
	rt := newCoalesceTransport(http.DefaultTransport)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/read", strings.NewReader("x"))
			resp, err := rt.RoundTrip(req)
			if err != nil {
				t.Error(err)
				return
			}
			resp.Body.Close()
		}()
	}
	wg.Wait()

	if got := hits.Load(); got != 3 {
		t.Fatalf("expected 3 downstream requests, got %d", got)
	}
}

func TestCoalesceSharesErrorResponse(t *testing.T) {
	var hits atomic.Int32
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		<-release
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, `{"status":"error","error":"too many series"}`)
	}))
	defer srv.Close()
	rt := newCoalesceTransport(http.DefaultTransport)

	const n = 5
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/query?query=up", nil)
			resp, err := rt.RoundTrip(req)
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(b), "too many series") {
				t.Errorf("got status %d body %q", resp.StatusCode, b)
			}
			// Each caller must get its own header map.
			resp.Header.Set("X-Mutated", "1")
		}()
	}
	waitWaiters(t, rt, n)
	close(release)
	wg.Wait()

	if got := hits.Load(); got != 1 {
		t.Fatalf("expected 1 downstream request, got %d", got)
	}
}

func TestCoalesceRequestsConfig(t *testing.T) {
	tests := []struct {
		name         string
		config       string
		wantCoalesce bool
	}{
		{name: "default enabled", config: "scheme: http\n", wantCoalesce: true},
		{name: "explicitly disabled", config: "coalesce_requests: false\n", wantCoalesce: false},
		{name: "explicitly enabled", config: "coalesce_requests: true\n", wantCoalesce: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg Config
			if err := yaml.Unmarshal([]byte(tt.config), &cfg); err != nil {
				t.Fatal(err)
			}
			if cfg.CoalesceRequests != tt.wantCoalesce {
				t.Fatalf("got coalesce=%v, want %v", cfg.CoalesceRequests, tt.wantCoalesce)
			}
		})
	}
}

// TestServerGroupCoalesceEndToEnd drives the real production path:
// ServerGroup.QueryRange -> promclient/client_golang -> ServerGroup.RoundTrip ->
// coalescing transport -> HTTP, against a fake downstream counting hits.
func TestServerGroupCoalesceEndToEnd(t *testing.T) {
	oldInterval := DiscoveryUpdateInterval
	DiscoveryUpdateInterval = 10 * time.Millisecond
	defer func() { DiscoveryUpdateInterval = oldInterval }()

	for _, tc := range []struct {
		name     string
		coalesce bool
	}{
		{name: "enabled", coalesce: true},
		{name: "disabled", coalesce: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			started := make(chan struct{}, 100)
			release := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r.ParseForm()
				hits.Add(1)
				started <- struct{}{}
				<-release
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, `{"status":"success","data":{"resultType":"matrix","result":[`+
					`{"metric":{"__name__":"up","job":"vm"},"values":[[1700000000,"1"],[1700000060,"2"]]}]}}`)
			}))
			defer srv.Close()

			sg, err := NewServerGroup()
			if err != nil {
				t.Fatal(err)
			}
			defer sg.Cancel()

			var cfg Config
			raw := "static_configs:\n  - targets: [" + strings.TrimPrefix(srv.URL, "http://") + "]\n" +
				"labels:\n  env: novita\n"
			if !tc.coalesce {
				raw += "coalesce_requests: false\n"
			}
			if err := yaml.Unmarshal([]byte(raw), &cfg); err != nil {
				t.Fatal(err)
			}
			if err := sg.ApplyConfig(&cfg); err != nil {
				t.Fatal(err)
			}
			select {
			case <-sg.Ready:
			case <-time.After(10 * time.Second):
				t.Fatal("servergroup never became ready")
			}

			const n = 8
			r := v1.Range{Start: time.Unix(1700000000, 0), End: time.Unix(1700000060, 0), Step: time.Minute}
			var wg sync.WaitGroup
			type result struct {
				series  int
				samples int
				env     string
				job     string
				err     error
			}
			results := make([]result, n)
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ss := sg.QueryRange(context.Background(), `sum(rate(up[5m]))`, r)
					var res result
					for ss.Next() {
						res.series++
						lbls := ss.At().Labels()
						res.env, res.job = lbls.Get("env"), lbls.Get("job")
						it := ss.At().Iterator(nil)
						for it.Next() != chunkenc.ValNone {
							res.samples++
						}
					}
					res.err = ss.Err()
					results[i] = res
				}(i)
			}

			wantHits := 1
			if !tc.coalesce {
				wantHits = n
			}
			for i := 0; i < wantHits; i++ {
				select {
				case <-started:
				case <-time.After(10 * time.Second):
					t.Fatalf("timed out waiting for downstream request %d", i+1)
				}
			}
			if tc.coalesce {
				waitWaiters(t, sg.httpClient().Transport, n)
			}
			close(release)
			wg.Wait()

			if got := int(hits.Load()); got != wantHits {
				t.Fatalf("expected %d downstream requests, got %d", wantHits, got)
			}
			for i, res := range results {
				if res.err != nil {
					t.Fatalf("caller %d: %v", i, res.err)
				}
				if res.series != 1 || res.samples != 2 || res.env != "novita" || res.job != "vm" {
					t.Fatalf("caller %d: unexpected result %+v", i, res)
				}
			}
		})
	}
}
