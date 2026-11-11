# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

Go 1.22+ interview starter with no external dependencies (keep it that way). `master` carries the incomplete scaffold where `Store.Allow` and `Store.cleanup` in `internal/limiter/store.go` are TODO stubs — the exercise. The `practice/rate-limiter` branch has completed it: fixed-window per-user limiting keyed on `X-User-ID`, inactive-entry cleanup, constructor config validation, and `test/e2e` integration tests, with the whole suite green under `-race`.

Important conventions for this repo:

- Existing test assertions are the spec — never rewrite them to make code pass (adjusting `NewStore` call sites for signature changes is fine; changing assertions is not). The repo must keep compiling and `go vet ./...` must stay clean.
- Tests assert **exact** accepted/rejected counts and use the fake clock in `internal/limiter/store_test.go` (`fakeClock` with `Advance`) rather than sleeps. Time-boundary behavior must stay deterministic.
- The extensions in `docs/interview-guide.md` (per-user config, `Retry-After`, distributed/algorithm variants) are deliberately out of scope for the base exercise. Do not implement them unless asked.
- Work happens as an interview simulation: small reviewable steps, inspect every diff (`git diff`), keep the initial commit intact, no new dependencies or broad refactors.

## Commands

```bash
go test ./...                     # full suite (green on practice/rate-limiter; fails by design on master)
go test -race ./...               # required — concurrency correctness is the core of the exercise
go vet ./...                      # must pass
go run ./cmd/server               # starts on :8080; config (5 req / 10 s) lives in cmd/server/main.go
go test ./internal/limiter -run TestWindowResets -v   # single test
curl -i -H 'X-User-ID: alice' http://localhost:8080/
```

## Architecture

Request flow: `cmd/server/main.go` constructs `limiter.NewStore(Config, RealClock{})` (returns `(*Store, error)`; a bad `Limit`/`Window` makes the server exit via `log.Fatalf` before serving), calls `store.Start()`, and wires `httpapi.NewHandler(store)` into the mux.

- **`internal/httpapi`** — transport only. `Handler` depends on the one-method `Limiter` interface (`Allow(userID) bool`), never on the concrete store. Status mapping: 400 for missing/blank `X-User-ID` (trimmed), 429 when `Allow` returns false, 200 otherwise.
- **`internal/limiter`** — owns per-user state, policy, and concurrency. Two seams to preserve:
  - **`Clock` interface** makes window-boundary tests deterministic; production wiring uses `RealClock`.
  - **Lifecycle** (`Start`/`Close`) is already scaffolded with `sync.Once`, `lifeMu`, `stopCh`/`doneCh`: `Start` is idempotent, `Close` is idempotent and safe even if `Start` was never called, and it waits for the cleanup goroutine to exit before returning. `TestCleanupWorkerStops` exercises this — do not regress it.
- `Store` uses one `sync.Mutex` (`s.mu`) guarding the `entries map[string]*entry`; `Allow`, `cleanup` and `Len()` each hold it for their whole operation, so cleanup sweeps and request accounting serialize and in-flight entry deletion is safe. `Allow` reads `s.clock.Now()` *inside* the lock — reading it before locking let lock-acquisition reordering trip the backward-step re-anchor guard and refund quota under bursts (found in review; pinned by `TestConcurrentMonotonicClockBurstNoOverAdmission`). A `Clock` implementation must never call back into `Store`. Pinned semantics: window anchored at the user's first request; reset at `now.Sub(windowStart) >= Window` (inclusive), re-anchored at the arriving request; `lastSeen` refreshed on allowed **and** rejected requests — `TestRejectedRequestsKeepWindowStateAlive` proves that without this, cleanup evicts an over-limit hammerer and hands them a fresh window; cleanup deletes at `now.Sub(lastSeen) >= InactiveAfter`. Known accepted limitation: fixed windows admit ~2x burst across adjacent boundaries.
- Config is validated at construction: `NewStore` rejects `Limit <= 0`, `Window <= 0`, and `InactiveAfter < Window` — the last one because a sweep could otherwise evict an entry whose window is still open and hand that user a fresh full quota (reproduced in review; `TestNewStoreRejectsInvalidConfig` pins all three). `CleanupInterval <= 0` normalizes to 1 minute.
- **`test/e2e`** — mirrors the `main.go` wiring (`NewStore` with a local fake clock → `Start` → `httpapi.NewHandler` over `httptest.NewServer`). Cross-package tests belong there; keep production dependencies one-directional (`httpapi` must not import `limiter`).
