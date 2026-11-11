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
	start   sync.Once
	close   sync.Once
	lifeMu  sync.Mutex
	started bool
}

// NewStore constructs a Store from cfg. Policy that cannot express a quota is
// rejected: Limit <= 0 or Window <= 0 returns a non-nil error and a nil Store;
// callers must treat that as "do not start". A nil clock uses RealClock.
// CleanupInterval <= 0 is normalized to one minute (it is a tuning parameter,
// not policy). InactiveAfter is not validated: zero or negative is allowed but
// evicts every entry on the next cleanup tick — keep it >= Window.
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
// With InactiveAfter <= 0 every stored entry is deleted on the next tick
// (now - lastSeen >= 0 always); keep it >= Window. A backward clock step
// makes elapsed negative for recently seen entries, so cleanup can never
// spuriously evict because of it.
func (s *Store) cleanup(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, e := range s.entries {
		if now.Sub(e.lastSeen) >= s.cfg.InactiveAfter {
			delete(s.entries, id) // delete-during-range is safe in Go
		}
	}
}

// Start launches one cleanup worker. It is safe to call more than once.
func (s *Store) Start() {
	s.start.Do(func() {
		s.lifeMu.Lock()
		s.started = true
		s.lifeMu.Unlock()
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
	})
}

// Close stops the cleanup worker and waits for it to exit. It is safe to call
// more than once, including when Start was never called.
func (s *Store) Close() {
	s.close.Do(func() {
		s.lifeMu.Lock()
		started := s.started
		s.lifeMu.Unlock()
		if !started {
			// Consume the start-once so a later Start cannot launch a worker
			// after Close has returned.
			s.start.Do(func() { close(s.doneCh) })
			return
		}
		close(s.stopCh)
		<-s.doneCh
	})
}

// Len is intentionally small observability/test support; it is not an API.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}
