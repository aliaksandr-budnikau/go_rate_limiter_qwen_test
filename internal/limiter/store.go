package limiter

import (
	"fmt"
	"sync"
	"time"
)

// Clock is injectable so tests can advance time without sleeping.
type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

type Config struct {
	Limit           int
	Window          time.Duration
	InactiveAfter   time.Duration
	CleanupInterval time.Duration
}

type entry struct {
	windowStart time.Time
	count       int
	lastSeen    time.Time
}

type Store struct {
	mu      sync.Mutex
	entries map[string]*entry
	cfg     Config
	clock   Clock

	stopCh  chan struct{}
	doneCh  chan struct{}
	lifeMu  sync.Mutex // guards started and closed; never taken while holding mu
	started bool
	closed  bool
}

// NewStore constructs a Store from cfg. Policy that cannot express a quota is
// rejected: Limit <= 0, Window <= 0, or InactiveAfter < Window returns a
// non-nil error and a nil Store; callers must treat that as "do not start".
// InactiveAfter < Window lets a sweep evict an entry whose window is still
// open, so the user's next request recreates it with a fresh full quota — a
// reproduced rate-limit bypass (senior review). A nil clock uses RealClock.
// CleanupInterval <= 0 is normalized to one minute (it is a tuning parameter,
// not policy).
func NewStore(cfg Config, clock Clock) (*Store, error) {
	if clock == nil {
		clock = RealClock{}
	}
	if cfg.Limit <= 0 {
		return nil, fmt.Errorf("limiter: Limit must be > 0, got %d", cfg.Limit)
	}
	if cfg.Window <= 0 {
		return nil, fmt.Errorf("limiter: Window must be > 0, got %v", cfg.Window)
	}
	if cfg.InactiveAfter < cfg.Window {
		return nil, fmt.Errorf("limiter: InactiveAfter (%v) must be >= Window (%v)", cfg.InactiveAfter, cfg.Window)
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = time.Minute
	}
	return &Store{
		entries: make(map[string]*entry),
		cfg:     cfg,
		clock:   clock,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}, nil
}

// Allow reports whether this request fits in the user's fixed window.
//
// Semantics (pinned by store_test.go):
//   - The window is anchored at the arrival of the user's first request in it.
//     A request with now.Sub(windowStart) >= Window (inclusive boundary) opens
//     a new window anchored at its own arrival time and resets the count.
//   - The first Limit requests per window are allowed; the count never carries
//     across a reset.
//   - A backward clock step (NTP adjustment) puts windowStart in the future;
//     negative elapsed can never reach Window, so such a request re-anchors
//     the window rather than stalling the user's quota until wall clock
//     catches up. Pinned by TestClockMovesBackwardReanchorsWindow.
//   - lastSeen is refreshed on every request, allowed or rejected, so cleanup
//     cannot evict a user who is still hammering and thereby hand them a fresh
//     full window.
//   - The clock is read INSIDE s.mu: critical sections then observe
//     non-decreasing timestamps for any monotonic clock, so windowStart can
//     only exceed now after a genuine step. Reading before the lock lets
//     ordinary read-vs-lock-acquisition reordering trip the re-anchor guard
//     and refund consumed quota under bursts (senior review; pinned by
//     TestConcurrentMonotonicClockBurstNoOverAdmission, whose mutation check
//     caught exactly this with accepted=4..6 against the unfixed ordering).
//     Consequence for implementors: a Clock must never call back into Store.
//   - Config validity (Limit > 0, Window > 0) is guaranteed by NewStore, so
//     no per-request fail-closed check is needed here.
//   - userID is opaque policy-wise; blank/missing identity is the transport's
//     400 contract (see internal/httpapi), never a limiter decision.
//
// Known fixed-window limitation: up to ~2x Limit across two adjacent window
// boundaries. Accepted; a sliding window is an out-of-scope extension.
func (s *Store) Allow(userID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now() // under the lock: see the semantics comment above

	e := s.entries[userID]
	if e == nil {
		e = &entry{windowStart: now}
		s.entries[userID] = e
	}

	if now.Sub(e.windowStart) >= s.cfg.Window || e.windowStart.After(now) {
		e.windowStart = now
		e.count = 0
	}
	e.lastSeen = now

	if e.count >= s.cfg.Limit {
		return false
	}
	e.count++
	return true
}

// cleanup removes entries with now.Sub(lastSeen) >= InactiveAfter (inclusive
// boundary, pinned by TestCleanupRemovesInactiveEntries). It holds the same
// s.mu as Allow, so each sweep is one critical section: a request can never
// observe a half-updated or concurrently-deleted entry — one arriving after
// eviction simply recreates the window. Because Allow refreshes lastSeen on
// rejected requests too, only genuinely silent users are evicted; an
// over-limit hammerer keeps its window state and cannot farm fresh quotas.
// NewStore guarantees InactiveAfter >= Window, so a sweep can never evict an
// entry whose window is still open. A backward clock step makes elapsed
// negative for recently seen entries, so cleanup can never spuriously evict
// because of it.
func (s *Store) cleanup(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, e := range s.entries {
		if now.Sub(e.lastSeen) >= s.cfg.InactiveAfter {
			delete(s.entries, id) // delete-during-range is safe in Go
		}
	}
}

// Start launches the cleanup worker, at most once. It is idempotent, and a
// no-op after Close: a leaked caller can never resurrect the sweeper.
func (s *Store) Start() {
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	if s.started || s.closed {
		return
	}
	s.started = true
	go func() {
		defer close(s.doneCh)
		ticker := time.NewTicker(s.cfg.CleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.cleanup(s.clock.Now())
			case <-s.stopCh:
				return
			}
		}
	}()
}

// Close marks the store dead and, if a worker ever started, stops it and
// waits for it to exit before returning. It is safe to call more than once,
// including when Start was never called; a concurrent second Close returns
// as a no-op while the first completes the join.
//
// The whole decision runs inside lifeMu as one started/closed state
// machine. The previous sync.Once scaffold raced: Close observed
// started==false, a concurrent Start then launched the worker, and Close's
// fallback — consuming the start-once — was already spent, leaking a goroutine
// that swept forever (senior review). Under this machine every interleaving
// either joins the worker or ensures none exists.
func (s *Store) Close() {
	s.lifeMu.Lock()
	if s.closed {
		s.lifeMu.Unlock()
		return
	}
	s.closed = true
	running := s.started
	s.lifeMu.Unlock()

	if running {
		close(s.stopCh)
		<-s.doneCh
	}
}

// Len is intentionally small observability/test support; it is not an API.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}
