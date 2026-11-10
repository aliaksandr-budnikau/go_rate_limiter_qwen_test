# Claude Code Go Rate Limiter — Interview Starter

A deliberately incomplete Go 1.22+ HTTP service for a 60-minute Senior Go Backend Engineer interview simulation. The HTTP server and package boundaries are in place; the in-memory limiter and inactive-entry cleanup are intentionally unfinished. Some tests are expected to fail until you implement the TODOs. The starter should compile.

## Requirements

- Go 1.22+
- Git
- Claude Code installed and authenticated (optional, but central to the exercise)

## Run locally

```bash
go version
go test ./...
go test -race ./...
go vet ./...
go run ./cmd/server
```

The server listens on `:8080`. Send requests with a user header:

```bash
curl -i -H 'X-User-ID: alice' http://localhost:8080/
curl -i -H 'X-User-ID: bob' http://localhost:8080/
```

Expected API behavior:
- `200 OK` while the user's quota is available.
- `429 Too Many Requests` after the per-user fixed-window quota is exhausted.
- `400 Bad Request` when `X-User-ID` is missing or blank.

Default starter configuration: 5 requests per 10 seconds. Change values in `cmd/server/main.go` while experimenting.

## Repository map

```text
cmd/server/             application wiring and HTTP server
internal/httpapi/       transport layer, status mapping, handler tests
internal/limiter/       limiter contract, state, clock and cleanup tests
docs/interview-guide.md interview plan, prompts, rubric and extensions
```

The handler depends on a small interface, not on the concrete store. The limiter owns per-user state and concurrency. `Clock` makes time-boundary tests deterministic. The cleanup worker has an explicit lifecycle (`Start`/`Close`) so resource ownership is visible.

## 60-minute Claude Code workflow

Treat Claude Code as a junior pair programmer, not an authority. Keep the repository's initial commit intact and inspect every diff.

### Phase 1 — Repository exploration (5–10 minutes)

Prompt:

> Explore this repository. Explain the architecture, main components, dependencies and execution flow. Identify potential problems. Do not modify any files yet.

Ask it to trace a request from `main` through the handler to the limiter interface. Confirm its explanation against the actual code.

### Phase 2 — Planning (5 minutes)

Prompt:

> We need to implement per-user rate limiting. Inspect the existing implementation and propose a detailed implementation plan. Consider concurrency, memory management, testing and edge cases. Do not modify files yet.

Require a discussion of fixed-window boundary semantics, lock granularity, cleanup safety, clock injection, and whether a user's state can be removed while requests are in flight.

### Phase 3 — Implementation (20–25 minutes)

Implement in small, reviewable steps. Ask Claude to state assumptions before editing. Review each diff (`git diff`) and reject unnecessary dependencies or broad refactors. Ask it to explain lock ownership and linearization points. Do not accept code you cannot explain.

### Phase 4 — Testing (10–15 minutes)

Prompt:

> Review the implementation and identify missing test cases. Add tests for concurrency, time boundaries and cleanup. Run go test ./... and go test -race ./....

Check that tests assert exact allowed/rejected counts, avoid arbitrary sleeps, and exercise simultaneous calls for both same and distinct users.

### Phase 5 — Code review (10 minutes)

Prompt:

> Act as a senior Go engineer reviewing this implementation. Look for race conditions, resource leaks, incorrect synchronization, edge cases, poor abstractions and performance issues. Report findings before modifying anything.

Ask for severity-ranked findings with file/line references. Verify each finding yourself, then fix only substantiated issues.

## Expected starter test state

The project is designed to compile. `go test ./...` is expected to fail on behavioral assertions because `Store.Allow` and `Store.cleanup` are TODO stubs. Those are intentional exercise failures, not build failures. `go vet ./...` should pass. The worker lifecycle is scaffolded and has a test; you may improve it, but the distributed and algorithmic extensions below are discussion/optional follow-ups.

## Git workflow

An initial commit is included. Useful commands:

```bash
git status
git log --oneline --decorate -3
git diff
git diff --stat
git diff --check
git show HEAD
```

Before the interview, create a separate working branch:

```bash
git switch -c practice/implementation
```

## Interview guide

See [`docs/interview-guide.md`](docs/interview-guide.md) for the interviewer scorecard, detailed follow-ups, strong/weak behaviors, and alternative exercises.
