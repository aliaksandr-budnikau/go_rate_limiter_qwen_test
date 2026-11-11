package limiter

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
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

func testStore(t *testing.T, clock Clock) *Store {
	t.Helper()
	s, err := NewStore(Config{
		Limit: 3, Window: time.Minute,
		InactiveAfter: 2 * time.Minute, CleanupInterval: time.Hour,
	}, clock)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

func TestNewStoreRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"zero limit", Config{Limit: 0, Window: time.Minute}},
		{"negative limit", Config{Limit: -1, Window: time.Minute}},
		{"zero window", Config{Limit: 3, Window: 0}},
		{"negative window", Config{Limit: 3, Window: -time.Second}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewStore(tt.cfg, newFakeClock())
			if err == nil {
				t.Fatalf("NewStore(%+v) error = nil, want error", tt.cfg)
			}
			if s != nil {
				t.Fatal("NewStore returned a non-nil Store alongside an error")
			}
		})
	}

	s, err := NewStore(Config{Limit: 3, Window: time.Minute, InactiveAfter: 2 * time.Minute}, newFakeClock())
	if err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if s == nil {
		t.Fatal("valid config returned a nil Store")
	}

	// CleanupInterval <= 0 is normalized, not an error.
	if _, err := NewStore(Config{Limit: 3, Window: time.Minute, InactiveAfter: 2 * time.Minute, CleanupInterval: 0}, newFakeClock()); err != nil {
		t.Fatalf("CleanupInterval normalization regressed: %v", err)
	}
	// InactiveAfter < Window is rejected: a sweep could then evict an entry
	// whose window is still open and hand the user a fresh quota.
	for _, cfg := range []Config{
		{Limit: 3, Window: time.Minute},
		{Limit: 3, Window: time.Minute, InactiveAfter: time.Second},
		{Limit: 3, Window: time.Minute, InactiveAfter: time.Minute - time.Nanosecond},
		{Limit: 3, Window: time.Minute, InactiveAfter: -time.Second},
	} {
		if _, err := NewStore(cfg, newFakeClock()); err == nil {
			t.Fatalf("NewStore(%+v) error = nil, want InactiveAfter < Window rejected", cfg)
		}
	}
	// Exactly Window is legal: the earliest eviction coincides with the
	// earliest legitimate window reset.
	if _, err := NewStore(Config{Limit: 3, Window: time.Minute, InactiveAfter: time.Minute}, newFakeClock()); err != nil {
		t.Fatalf("InactiveAfter == Window must be accepted: %v", err)
	}
}

func TestAllowWithinAndBeyondLimit(t *testing.T) {
	s := testStore(t, newFakeClock())
	for i := 0; i < 3; i++ {
		if !s.Allow("alice") {
			t.Fatalf("request %d unexpectedly rejected", i+1)
		}
	}
	if s.Allow("alice") {
		t.Fatal("fourth request should be rejected")
	}
}

func TestUsersHaveIndependentLimits(t *testing.T) {
	s := testStore(t, newFakeClock())
	for i := 0; i < 3; i++ {
		s.Allow("alice")
	}
	if s.Allow("alice") {
		t.Fatal("alice should be limited")
	}
	if !s.Allow("bob") {
		t.Fatal("bob should have an independent quota")
	}
}

func TestWindowResets(t *testing.T) {
	clock := newFakeClock()
	s := testStore(t, clock)
	for i := 0; i < 3; i++ {
		s.Allow("alice")
	}
	clock.Advance(time.Minute)
	if !s.Allow("alice") {
		t.Fatal("first request in new window should be accepted")
	}
}

// TestWindowReExhaustionAfterReset pins that rejected requests never consume
// quota: after exhausting a window and hammering through rejections, the next
// window must offer the full Limit again — no carry-over debt, no off-by-one.
func TestWindowReExhaustionAfterReset(t *testing.T) {
	clock := newFakeClock()
	s := testStore(t, clock) // Limit 3, Window 1m
	for i := 0; i < 3; i++ {
		if !s.Allow("alice") {
			t.Fatalf("request %d in first window should be accepted", i+1)
		}
	}
	for i := 0; i < 5; i++ {
		if s.Allow("alice") {
			t.Fatalf("rejected run: request %d should be denied", 4+i)
		}
	}
	clock.Advance(time.Minute)
	for i := 0; i < 3; i++ {
		if !s.Allow("alice") {
			t.Fatalf("request %d in new window should be accepted (rejections must not eat quota)", i+1)
		}
	}
	if s.Allow("alice") {
		t.Fatal("fourth request in new window should be rejected")
	}
}

// TestClockMovesBackwardReanchorsWindow models an NTP-style step backward:
// after the jump the stored windowStart lies in the future. Naive elapsed
// arithmetic (now - windowStart >= Window) then evaluates negative and the
// user's quota stays stalled until wall clock catches up. Allow must instead
// re-anchor the window at the arriving request, and cleanup must neither
// panic nor spuriously evict on negative elapsed time.
func TestClockMovesBackwardReanchorsWindow(t *testing.T) {
	clock := newFakeClock()
	s := testStore(t, clock) // Limit 3, Window 1m, InactiveAfter 2m
	for i := 0; i < 3; i++ {
		s.Allow("alice")
	}
	if s.Allow("alice") {
		t.Fatal("quota should be exhausted")
	}

	clock.Advance(-2 * time.Minute) // windowStart now 2m in the future
	if !s.Allow("alice") {
		t.Fatal("backward clock jump must re-anchor the window, not stall the quota")
	}
	if !s.Allow("alice") || !s.Allow("alice") {
		t.Fatal("re-anchored window must offer the full Limit again")
	}
	if s.Allow("alice") {
		t.Fatal("fourth request in re-anchored window should be rejected")
	}

	s.cleanup(clock.Now()) // negative-elapsed entries must survive, no panic
	if got := s.Len(); got != 1 {
		t.Fatalf("stored users = %d, want 1", got)
	}
}

func TestConcurrentRequestsSameUser(t *testing.T) {
	s := testStore(t, newFakeClock())
	const workers = 100
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := 0
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.Allow("same-user") {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if accepted != 3 {
		t.Fatalf("accepted = %d, want exactly 3", accepted)
	}
}

func TestConcurrentDifferentUsers(t *testing.T) {
	s := testStore(t, newFakeClock())
	const users = 40
	var wg sync.WaitGroup
	for i := 0; i < users; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 3; j++ {
				if !s.Allow(userKey(i)) {
					t.Errorf("user %d request %d unexpectedly rejected", i, j)
				}
			}
		}(i)
	}
	wg.Wait()
	if got := s.Len(); got != users {
		t.Fatalf("stored users = %d, want %d", got, users)
	}
}

func TestCleanupRemovesInactiveEntries(t *testing.T) {
	clock := newFakeClock()
	s := testStore(t, clock)
	s.Allow("alice")
	clock.Advance(2 * time.Minute)
	s.cleanup(clock.Now())
	if got := s.Len(); got != 0 {
		t.Fatalf("stored users after cleanup = %d, want 0", got)
	}
}

// TestRejectedRequestsKeepWindowStateAlive pins the choice that Allow
// refreshes lastSeen on rejected requests too. If rejections did not count as
// activity, cleanup could evict a user who is still hammering, and their next
// request would recreate the entry with a fresh full window — a rate-limit
// bypass. At t0+2m the buggy entry (lastSeen frozen at the accepted request,
// t0) is exactly InactiveAfter (2m) old, while the correct one (lastSeen from
// the rejection at t0+30s) is 90s old: Len()==1 is the discriminator. The
// old follow-up "cleanup must not hand out a fresh window" Allow assertion is
// gone on purpose — past t0+1m the window has legitimately elapsed, so an
// evicted-and-recreated entry behaves identically to a retained one; only the
// sweep can distinguish the implementations.
func TestRejectedRequestsKeepWindowStateAlive(t *testing.T) {
	clock := newFakeClock()
	s, err := NewStore(Config{
		Limit: 1, Window: time.Minute,
		InactiveAfter: 2 * time.Minute, CleanupInterval: time.Hour,
	}, clock)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	if !s.Allow("hammerer") {
		t.Fatal("first request should be allowed")
	}
	clock.Advance(30 * time.Second)
	if s.Allow("hammerer") {
		t.Fatal("second request should be rejected")
	}

	clock.Advance(90 * time.Second) // t0+2m; lastSeen 90s ago
	s.cleanup(clock.Now())
	if got := s.Len(); got != 1 {
		t.Fatalf("stored users = %d, want 1: active-but-limited user was cleaned up", got)
	}
}

// TestUserKeysAreCaseSensitiveAndEmptyIsOpaque pins the documented key
// contract: userIDs are opaque map keys — "alice" and "Alice" hold
// independent quotas, and "" (unreachable through the HTTP transport, which
// 400s blank IDs first) behaves like any other key.
func TestUserKeysAreCaseSensitiveAndEmptyIsOpaque(t *testing.T) {
	s := testStore(t, newFakeClock()) // Limit 3
	for i := 0; i < 3; i++ {
		s.Allow("alice")
	}
	if s.Allow("alice") {
		t.Fatal("alice should be limited")
	}
	if !s.Allow("Alice") {
		t.Fatal("keys are case-sensitive; Alice has its own quota")
	}
	if !s.Allow("") {
		t.Fatal("empty userID must work as an opaque key")
	}
	if got := s.Len(); got != 3 {
		t.Fatalf("stored users = %d, want 3", got)
	}
}

// TestCleanupEvictsOnlyInactiveUsers runs one sweep that must remove a stale
// user while leaving a recently-active user's entry — and its partially
// consumed window state — untouched.
func TestCleanupEvictsOnlyInactiveUsers(t *testing.T) {
	clock := newFakeClock()
	s, err := NewStore(Config{
		Limit: 3, Window: time.Minute,
		InactiveAfter: 2 * time.Minute, CleanupInterval: time.Hour,
	}, clock)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	s.Allow("stale") // lastSeen t0
	clock.Advance(2 * time.Minute)
	s.Allow("active") // window anchored t0+2m, lastSeen t0+2m
	clock.Advance(time.Second)

	s.cleanup(clock.Now()) // stale idle 2m1s >= 2m; active idle 1s
	if got := s.Len(); got != 1 {
		t.Fatalf("stored users = %d, want 1 (only the stale user)", got)
	}
	// The surviving entry must still be mid-window: two of three left.
	if !s.Allow("active") || !s.Allow("active") {
		t.Fatal("active user should keep its remaining quota")
	}
	if s.Allow("active") {
		t.Fatal("fourth request must exhaust the preserved window")
	}
}

// TestConcurrentAllowAndCleanup sweeps cleanup hard while 50 goroutines
// account against the same frozen clock (so no entry can go stale: elapsed
// stays 0). It proves sweeps and accounting share the lock cleanly under
// -race and that a sweep never corrupts in-flight quota — accepted must stay
// exactly Limit and the entry must survive.
func TestConcurrentAllowAndCleanup(t *testing.T) {
	clock := newFakeClock()
	s := testStore(t, clock) // Limit 3, InactiveAfter 2m
	const allowWorkers = 50
	const sweepers = 8
	const sweepsPerGoroutine = 1000

	var wg sync.WaitGroup
	var accepted int32
	for i := 0; i < sweepers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < sweepsPerGoroutine; j++ {
				s.cleanup(clock.Now())
			}
		}()
	}
	for i := 0; i < allowWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.Allow("racer") {
				atomic.AddInt32(&accepted, 1)
			}
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&accepted); got != 3 {
		t.Fatalf("accepted = %d, want exactly 3", got)
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("stored users = %d, want 1 (frozen clock means nothing is inactive)", got)
	}
}

// tickClock advances one microsecond per Now call: strictly monotonic (no
// genuine backward steps exist) yet every caller sees a different timestamp,
// so bursts exercise timestamp-vs-lock-acquisition reordering deterministically.
type tickClock struct {
	mu sync.Mutex
	t  time.Time
}

func newTickClock() *tickClock {
	return &tickClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *tickClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(time.Microsecond)
	return c.t
}

// TestConcurrentMonotonicClockBurstNoOverAdmission closes the blind spot of
// the frozen-clock concurrency tests: with one constant timestamp,
// read-vs-lock reordering can never occur. The tick clock gives every
// goroutine a different, strictly increasing time — if Allow read the clock
// outside s.mu, an inversion (a locker holding a timestamp older than the
// current windowStart) would trip the backward-step guard and refund consumed
// quota. The clock never steps back, so for the fixed implementation the
// guard CANNOT fire under any schedule and exactly Limit must be admitted in
// every trial: the pass condition is deterministic for all interleavings.
// Catching the pre-fix bug remains probabilistic, as it is for any race test
// — forcing the inversion would require a scheduling seam in production code.
func TestConcurrentMonotonicClockBurstNoOverAdmission(t *testing.T) {
	const trials = 25
	const workers = 200
	for trial := 0; trial < trials; trial++ {
		s, err := NewStore(Config{
			Limit: 3, Window: time.Hour,
			InactiveAfter: 10 * time.Hour, CleanupInterval: 10 * time.Hour,
		}, newTickClock())
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}
		var wg sync.WaitGroup
		var accepted int32
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if s.Allow("burst") {
					atomic.AddInt32(&accepted, 1)
				}
			}()
		}
		wg.Wait()
		if got := atomic.LoadInt32(&accepted); got != 3 {
			t.Fatalf("trial %d: accepted = %d, want exactly 3 (over-admission via reordering)", trial, got)
		}
	}
}

func TestCleanupWorkerStops(t *testing.T) {
	s, err := NewStore(Config{Limit: 1, Window: time.Second, InactiveAfter: time.Second, CleanupInterval: time.Millisecond}, newFakeClock())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	s.Start()
	s.Close()
	select {
	case <-s.doneCh:
	case <-time.After(time.Second):
		t.Fatal("cleanup goroutine did not stop")
	}
	// Close is idempotent.
	s.Close()
}

// TestStartAfterCloseIsNoOp pins the state machine's terminal transition:
// once Close has run, Start must never launch a worker. A resurrected
// sweeper would outlive Close and keep evicting entries forever. Detection
// side is probabilistic (a broken Start would sweep on a 1ms ticker and evict
// the deliberately-aged entry within the bounded wait); the pass condition —
// no worker exists, so the entry cannot be swept — holds for every schedule.
func TestStartAfterCloseIsNoOp(t *testing.T) {
	clock := newFakeClock()
	s, err := NewStore(Config{Limit: 1, Window: time.Second, InactiveAfter: time.Second, CleanupInterval: time.Millisecond}, clock)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	s.Close() // Close before any Start: must return without joining anything
	s.Start() // must be a no-op

	s.Allow("zombie")
	clock.Advance(2 * time.Second) // stale for any live sweeper
	time.Sleep(50 * time.Millisecond)
	if got := s.Len(); got != 1 {
		t.Fatalf("stored users = %d, want 1: Start after Close resurrected the sweeper", got)
	}
	s.Close() // second Close: still a no-op
}

// TestConcurrentStartCloseNoLeak hammers the reviewed leak race directly:
// under the old sync.Once scaffold, a Close that read started==false just
// before a concurrent Start completed leaked the worker (Close's start-once
// fallback was already consumed). With the lifeMu-guarded state machine,
// every interleaving either joins the worker or guarantees none exists, so
// after all calls return no sweeper may be running — the aged probe entry
// must survive. Pass is deterministic; catching the old bug was
// schedule-dependent, which 4x4 goroutines over many trials maximizes.
func TestConcurrentStartCloseNoLeak(t *testing.T) {
	const trials = 20
	const racers = 4
	for trial := 0; trial < trials; trial++ {
		clock := newFakeClock()
		s, err := NewStore(Config{Limit: 1, Window: time.Second, InactiveAfter: time.Second, CleanupInterval: time.Millisecond}, clock)
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}
		var wg sync.WaitGroup
		for i := 0; i < racers; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); s.Start() }()
			wg.Add(1)
			go func() { defer wg.Done(); s.Close() }()
		}
		wg.Wait()

		s.Allow("probe")
		clock.Advance(2 * time.Second) // stale for any leaked sweeper
		time.Sleep(20 * time.Millisecond)
		if got := s.Len(); got != 1 {
			s.Close()
			t.Fatalf("trial %d: stored users = %d, want 1: a worker survived every Close", trial, got)
		}
	}
}

func userKey(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "user-0"
	}
	out := ""
	for i > 0 {
		out = string(digits[i%10]) + out
		i /= 10
	}
	return "user-" + out
}
