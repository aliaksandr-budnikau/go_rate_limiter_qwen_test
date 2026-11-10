package limiter

import (
	"sync"
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

	// CleanupInterval <= 0 is normalized, not an error; InactiveAfter <= 0 is
	// deliberately unvalidated (TestCleanupWorkerStops relies on zero).
	if _, err := NewStore(Config{Limit: 3, Window: time.Minute, CleanupInterval: 0}, newFakeClock()); err != nil {
		t.Fatalf("CleanupInterval normalization regressed: %v", err)
	}
	if _, err := NewStore(Config{Limit: 3, Window: time.Minute, InactiveAfter: 0}, newFakeClock()); err != nil {
		t.Fatalf("InactiveAfter must stay unvalidated: %v", err)
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

func TestCleanupWorkerStops(t *testing.T) {
	s, err := NewStore(Config{Limit: 1, Window: time.Second, CleanupInterval: time.Millisecond}, newFakeClock())
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
