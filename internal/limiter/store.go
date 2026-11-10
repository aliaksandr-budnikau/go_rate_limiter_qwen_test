package limiter

import (
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

func NewStore(cfg Config, clock Clock) *Store {
	if clock == nil {
		clock = RealClock{}
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
	}
}

// Allow reports whether this request fits in the user's fixed window.
//
// TODO: implement per-user fixed-window accounting. Define behavior for invalid
// configuration and exact window boundaries. All shared state must be protected.
func (s *Store) Allow(userID string) bool {
	// TODO: replace this permissive scaffold with the limiter algorithm.
	return true
}

// cleanup removes entries that have been inactive for at least InactiveAfter.
// TODO: implement deletion while preserving synchronization safety.
func (s *Store) cleanup(now time.Time) {
	// TODO: iterate over entries and remove expired inactive users.
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
