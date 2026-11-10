# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A deliberately incomplete Go 1.22+ interview starter (no external dependencies). The HTTP server, package boundaries, and cleanup-worker lifecycle already work; `Store.Allow` and `Store.cleanup` in `internal/limiter/store.go` are TODO stubs that are **the exercise**. The task is to implement a per-user fixed-window in-memory rate limiter keyed on the `X-User-ID` header.

Important conventions for this repo:

- Failing tests are expected until the TODOs are implemented — they define the required behavior. The repo must keep compiling and `go vet ./...` must stay clean.
- Tests assert **exact** accepted/rejected counts and use the fake clock in `internal/limiter/store_test.go` (`fakeClock` with `Advance`) rather than sleeps. Time-boundary behavior must stay deterministic.
- The extensions in `docs/interview-guide.md` (per-user config, `Retry-After`, distributed/algorithm variants) are deliberately out of scope for the base exercise. Do not implement them unless asked.
- Work happens as an interview simulation: small reviewable steps, inspect every diff (`git diff`), keep the initial commit intact, no new dependencies or broad refactors.

## Commands

```bash
go test ./...                     # full suite (behavioral limiter tests fail until TODOs are done)
go test -race ./...               # required — concurrency correctness is the core of the exercise
go vet ./...                      # must pass
go run ./cmd/server               # starts on :8080; config (5 req / 10 s) lives in cmd/server/main.go
go test ./internal/limiter -run TestWindowResets -v   # single test
curl -i -H 'X-User-ID: alice' http://localhost:8080/
```

## Architecture

Request flow: `cmd/server/main.go` constructs `limiter.NewStore(Config, RealClock{})`, calls `store.Start()`, and wires `httpapi.NewHandler(store)` into the mux.

- **`internal/httpapi`** — transport only. `Handler` depends on the one-method `Limiter` interface (`Allow(userID) bool`), never on the concrete store. Status mapping: 400 for missing/blank `X-User-ID` (trimmed), 429 when `Allow` returns false, 200 otherwise.
- **`internal/limiter`** — owns per-user state, policy, and concurrency. Two seams to preserve:
  - **`Clock` interface** makes window-boundary tests deterministic; production wiring uses `RealClock`.
  - **Lifecycle** (`Start`/`Close`) is already scaffolded with `sync.Once`, `lifeMu`, `stopCh`/`doneCh`: `Start` is idempotent, `Close` is idempotent and safe even if `Start` was never called, and it waits for the cleanup goroutine to exit before returning. `TestCleanupWorkerStops` exercises this — do not regress it.
- `Store` uses one `sync.Mutex` (`s.mu`) guarding the `entries map[string]*entry`. The implementing `Allow` and `cleanup` must both take that lock; `Len()` already does. Key semantics to get right: fixed-window reset (window starts at first request, count resets when `now >= windowStart + Window`), per-user independence, and whether an entry may be deleted while requests are in flight (`InactiveAfter` vs `Window`).

Invalid configuration (`Limit <= 0`, non-positive `Window`) behavior is part of the exercise — decide it explicitly, as `NewStore` already does for `CleanupInterval`.
