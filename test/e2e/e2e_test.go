// Package e2e exercises the rate limiter through the real HTTP transport,
// wiring production components exactly like cmd/server/main.go:
// limiter.NewStore -> Start -> httpapi.NewHandler, served over a real
// httptest.Server with a real HTTP client.
//
// A local fake clock (implementing the exported limiter.Clock; the one in
// store_test.go is package-private) keeps window boundaries deterministic —
// these tests never sleep. CleanupInterval is deliberately short so the
// sweep worker ticks concurrently with live traffic under -race, while
// InactiveAfter stays large enough that no test relies on eviction (that is
// pinned by the unit tests, including the inclusive-boundary and
// rejection-retention cases).
package e2e

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"example.com/claude-code-go-rate-limiter/internal/httpapi"
	"example.com/claude-code-go-rate-limiter/internal/limiter"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newServer(t *testing.T, cfg limiter.Config) (*httptest.Server, *fakeClock) {
	t.Helper()
	clock := newFakeClock()
	store, err := limiter.NewStore(cfg, clock)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	store.Start()
	srv := httptest.NewServer(httpapi.NewHandler(store))
	t.Cleanup(func() {
		srv.Close()
		store.Close()
	})
	return srv, clock
}

// statusFor performs one GET with the X-User-ID header. setHeader false
// models an entirely absent header.
func statusFor(srv *httptest.Server, userID string, setHeader bool) (int, error) {
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/", nil)
	if err != nil {
		return 0, err
	}
	if setHeader {
		req.Header.Set("X-User-ID", userID)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func mustStatus(t *testing.T, srv *httptest.Server, userID string, setHeader bool) int {
	t.Helper()
	code, err := statusFor(srv, userID, setHeader)
	if err != nil {
		t.Fatalf("request as %q: %v", userID, err)
	}
	return code
}

func baseConfig() limiter.Config {
	return limiter.Config{
		Limit:           2,
		Window:          10 * time.Second,
		InactiveAfter:   10 * time.Minute,
		CleanupInterval: 50 * time.Millisecond,
	}
}

func TestEndToEndQuotaAndUserIndependence(t *testing.T) {
	srv, _ := newServer(t, baseConfig())

	for i, want := range []int{http.StatusOK, http.StatusOK, http.StatusTooManyRequests} {
		if got := mustStatus(t, srv, "alice", true); got != want {
			t.Fatalf("alice request %d: status = %d, want %d", i+1, got, want)
		}
	}
	if got := mustStatus(t, srv, "bob", true); got != http.StatusOK {
		t.Fatalf("bob: status = %d, want 200 (independent quota)", got)
	}
	if got := mustStatus(t, srv, "", false); got != http.StatusBadRequest {
		t.Fatalf("missing header: status = %d, want 400", got)
	}
	if got := mustStatus(t, srv, "   ", true); got != http.StatusBadRequest {
		t.Fatalf("blank header: status = %d, want 400", got)
	}
}

func TestEndToEndWindowReset(t *testing.T) {
	cfg := baseConfig()
	srv, clock := newServer(t, cfg)

	mustStatus(t, srv, "alice", true)
	mustStatus(t, srv, "alice", true)
	if got := mustStatus(t, srv, "alice", true); got != http.StatusTooManyRequests {
		t.Fatalf("third request in window: status = %d, want 429", got)
	}

	clock.Advance(cfg.Window) // exactly the inclusive reset boundary
	if got := mustStatus(t, srv, "alice", true); got != http.StatusOK {
		t.Fatalf("first request after window reset: status = %d, want 200", got)
	}
	if got := mustStatus(t, srv, "alice", true); got != http.StatusOK {
		t.Fatalf("second request in new window: status = %d, want 200", got)
	}
	if got := mustStatus(t, srv, "alice", true); got != http.StatusTooManyRequests {
		t.Fatalf("third request in new window: status = %d, want 429", got)
	}
}

func TestEndToEndConcurrentSameUser(t *testing.T) {
	cfg := baseConfig()
	cfg.Limit = 3
	srv, _ := newServer(t, cfg)

	const workers = 50
	var wg sync.WaitGroup
	var allowed, rejected int32
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, err := statusFor(srv, "bursty", true)
			if err != nil {
				t.Errorf("concurrent request: %v", err)
				return
			}
			switch code {
			case http.StatusOK:
				atomic.AddInt32(&allowed, 1)
			case http.StatusTooManyRequests:
				atomic.AddInt32(&rejected, 1)
			default:
				t.Errorf("unexpected status %d", code)
			}
		}()
	}
	wg.Wait()

	if got, want := atomic.LoadInt32(&allowed), int32(cfg.Limit); got != want {
		t.Fatalf("allowed = %d, want %d", got, want)
	}
	if got, want := atomic.LoadInt32(&rejected), int32(workers-cfg.Limit); got != want {
		t.Fatalf("rejected = %d, want %d", got, want)
	}
}

// TestEndToEndTrimsUserIDToSharedQuota pins the transport contract that
// surrounding whitespace is stripped before keying: "  alice  " and "alice"
// consume the same quota (baseConfig Limit 2).
func TestEndToEndTrimsUserIDToSharedQuota(t *testing.T) {
	srv, _ := newServer(t, baseConfig())

	if got := mustStatus(t, srv, "  alice  ", true); got != http.StatusOK {
		t.Fatalf("first request as '  alice  ': status = %d, want 200", got)
	}
	if got := mustStatus(t, srv, "alice", true); got != http.StatusOK {
		t.Fatalf("second request as 'alice': status = %d, want 200", got)
	}
	if got := mustStatus(t, srv, " alice ", true); got != http.StatusTooManyRequests {
		t.Fatalf("third request as ' alice ': status = %d, want 429 (same trimmed key)", got)
	}
}
