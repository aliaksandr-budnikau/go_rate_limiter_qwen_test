# Senior Go Backend Engineer — Interviewer & Candidate Guide

## Exercise brief

Implement an in-memory, concurrent, per-user fixed-window rate limiter for the existing HTTP service. Users are identified by `X-User-ID`. The base requirements are 200 accepted, 429 over quota, 400 missing/blank identity, independent quotas, concurrency safety, and bounded memory through inactive-entry cleanup.

Suggested timebox: **60 minutes**. Do not try to build a production distributed rate limiter in the base exercise. Prioritize correctness, explicit semantics, tests, and explainable trade-offs.

## Candidate deliverables

- A working implementation in the existing package boundaries.
- Deterministic unit tests and HTTP-level tests.
- `go test ./...`, `go test -race ./...`, and `go vet ./...` results.
- A short explanation of synchronization, time-window semantics, cleanup safety, and known limitations.

## Suggested prompts

### Explore (5–10 min)

> Explore this repository. Explain the architecture, main components, dependencies and execution flow. Identify potential problems. Do not modify any files yet.

Follow-up: ask Claude to trace one request and identify which package owns policy versus HTTP concerns.

### Plan (5 min)

> We need to implement per-user rate limiting. Inspect the existing implementation and propose a detailed implementation plan. Consider concurrency, memory management, testing and edge cases. Do not modify files yet.

A strong plan defines fixed-window boundary behavior, validates configuration, protects map and entry state, handles cleanup races, and injects time.

### Implement (20–25 min)

Ask for one small change at a time. Inspect `git diff` after each. Challenge the model to explain the critical section and why cleanup cannot race with request accounting. Avoid premature sharding or dependencies unless justified by measured needs.

### Test (10–15 min)

> Review the implementation and identify missing test cases. Add tests for concurrency, time boundaries and cleanup. Run go test ./... and go test -race ./....

Look for exact accepted counts under concurrency, independent users, boundary tests, cleanup retention/removal, invalid configuration, and worker lifecycle. Prefer a fake clock over sleeps.

### Review (10 min)

> Act as a senior Go engineer reviewing this implementation. Look for race conditions, resource leaks, incorrect synchronization, edge cases, poor abstractions and performance issues. Report findings before modifying anything.

Require findings first, severity, concrete evidence, and a proposed fix. The candidate must validate findings rather than accept them blindly.

## Follow-up requirements — reveal one at a time

These are deliberately **not implemented** in the starter. Do not reveal all extensions at once.

### Follow-up 1: Per-user configuration

Different users have different limits and/or windows. Discuss where policy comes from (static config, interface, tenant service), default behavior, dynamic policy changes, and how to avoid coupling HTTP transport to policy lookup. Test unknown users and policy changes.

### Follow-up 2: Retry-After

For rejected requests, return an appropriate `Retry-After` header. Decide whether to express seconds (HTTP-date is also possible), round up rather than understate the wait, and calculate from the active window's reset time. Add deterministic boundary tests.

### Follow-up 3: Graceful shutdown

Ensure the cleanup goroutine terminates correctly when the application shuts down. Discuss ownership, idempotent `Start`/`Close`, cancellation, waiting for completion, and ordering relative to HTTP server shutdown. The starter includes a lifecycle scaffold and a shutdown test; review it rather than assuming it is perfect.

### Follow-up 4: Distributed deployment

The service now runs on multiple instances behind a load balancer. Per-process maps no longer enforce a global per-user quota. Compare:
- **Redis**: shared state and operational dependency; atomicity must be designed.
- **Redis Lua scripts**: combine read/decide/increment/expiry atomically on the server.
- **Database counters**: durable but often higher latency/contention; transaction and row-lock semantics matter.
- **Distributed token bucket**: smooth bursts and supports refill, but requires atomic shared token accounting and careful clock assumptions.

Discuss failure policy (fail open vs fail closed), network partitions, hot keys, TTLs, observability, and consistency. Do not implement distributed behavior in this exercise.

### Follow-up 5: Algorithm change

Compare:
- **Fixed window**: simplest and cheap; permits boundary bursts (up to roughly twice the nominal quota across adjacent windows).
- **Sliding window log**: accurate rolling quota; stores timestamps and costs memory/cleanup work.
- **Sliding window counter**: approximate rolling quota with lower memory and computation.
- **Token bucket**: permits controlled bursts while enforcing an average refill rate; more state/math.
- **Leaky bucket**: smooths output rate; queueing adds latency and queue-capacity/backpressure questions.

Explain what product traffic shape would justify changing algorithms and how tests would change.

## Evaluation scorecard (1–5)

| Dimension | 1 — weak | 3 — solid | 5 — senior-level |
|---|---|---|---|
| Repository exploration | Starts editing without tracing flow | Correctly maps packages and request path | Identifies boundaries, hidden assumptions, and risks before proposing changes |
| Prompt engineering | Vague “implement it” prompt | States requirements and asks for plan/tests | Iterative, scoped prompts; asks for assumptions, evidence, and review before edits |
| Planning | No plan or ignores cleanup | Covers core algorithm and tests | Defines semantics, invariants, lifecycle, edge cases, and incremental checkpoints |
| Go implementation quality | Unidiomatic, tangled, unchecked errors | Clear packages and idiomatic synchronization | Minimal abstractions, validated config, clear ownership, maintainable API |
| Concurrency understanding | Data races or unsafe map access | Correct coarse-grained locking | Explains linearization, lock contention trade-offs, cleanup interaction, and race testing |
| Testing strategy | Happy path only | Covers status codes, limits, reset | Deterministic time, exact concurrent counts, lifecycle, boundaries, and failure cases |
| Code review | Accepts generated code wholesale | Inspects diffs and fixes obvious issues | Finds subtle correctness/performance issues and validates evidence |
| Critical AI evaluation | Treats model output as authoritative | Questions questionable choices | Detects hallucinated APIs, overengineering, race bugs, and unsupported claims |
| Architectural reasoning | Mixes HTTP and policy | Maintains handler/store separation | Explains extensibility and why not to add distributed complexity prematurely |
| Communication | Cannot explain own code | Explains main decisions | Concise, structured reasoning; states trade-offs and uncertainty clearly |

### Strong candidate behavior

- Reads the tests and interfaces before asking the agent to edit.
- Clarifies fixed-window semantics and invalid configuration.
- Requests a plan, then constrains implementation to small diffs.
- Can explain why the chosen lock protects every shared access.
- Uses fake time and race tests; does not hide failures.
- Spots overengineering and independently verifies AI review findings.
- Communicates what is done, what remains, and the operational limitations.

### Weak candidate behavior

- Pastes the entire task and accepts a large unreviewed rewrite.
- Cannot explain the mutex, map lifecycle, or window reset.
- Uses `time.Sleep` for core time tests without need.
- Ignores `go test -race`, leaves goroutines running, or tests only one user.
- Adds Redis or a third-party limiter before the in-memory requirement is solved.
- Claims production readiness without discussing process-local state and boundary bursts.

## Alternative Claude Code exercises

Use these as separate future practice sessions, not as mandatory additions to this task:

1. **Investigate and fix a race condition** — reproduce with `-race`, isolate shared state, add a regression test.
2. **Add a REST endpoint** — extend an existing handler while preserving validation, error conventions, and tests.
3. **In-memory cache with TTL** — design expiration, concurrency, cleanup, and deterministic time tests.
4. **Optimize an inefficient database query** — inspect query plans, reduce N+1 behavior, add indexes only with evidence.
5. **Refactor legacy Go code** — preserve behavior with characterization tests and small commits.
6. **Idempotent transaction processing** — define idempotency keys, transaction boundaries, uniqueness, and replay semantics.
7. **Retries and exponential backoff** — add bounded retries, jitter, context cancellation, and retryability classification.
8. **Investigate a memory leak** — use heap profiles, identify retention paths, and verify with before/after measurements.

## Interviewer notes

Do not grade solely on whether every TODO is completed. A candidate who surfaces a sound design, tests critical invariants, catches an AI-generated defect, and explains trade-offs may demonstrate stronger seniority than one who rushes to a superficially complete implementation.
