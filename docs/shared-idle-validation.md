# Shared idle shutdown: local Windows validation

These are local observations on Windows x64, not release acceptance or results for
other platforms. The source base was v0.1.1 (`3bc397ef090ba70f26ac638abd62b328cded1d7e`).
The backend was `@nexus2520/bitbucket-mcp-server` 3.0.1 (serverInfo reports 3.0.0),
launched through PowerShell 7. Only read operations were exercised against the real
API. Credentials, backend payloads and private configuration are intentionally omitted.

## Automated regression checks

```sh
cargo xtask check
cargo test --locked --features test-backend --test shared_idle
```

The Windows check passed source budgets, formatting, strict workspace Clippy and
all non-ignored unit/mock integration tests. The new configuration test checks
default-off serialization and rejects zero, session ownership and Docker. The new
`shared_idle` suite checks:

- An idle worker exits while two HTTP sessions and an open SSE stream survive;
  catalog access stays lazy and concurrent calls reuse one newly started backend
- Active and admitted queued requests survive the idle deadline
- Idle cleanup does not clear manual recovery after an in-call failure
- An external process exit between calls remains a failure instead of being
  silently converted into an idle restart

The external-exit case needs both SDK cancellation state and transport closure
detection: natural EOF does not necessarily cancel the rmcp service token.

One repeated full run failed the existing
`recovery::crash_recovers_but_catalog_drift_stays_blocked` assertion that the old
PID was no longer alive. Its immediate isolated rerun and a subsequent full
`cargo xtask check` passed. The cause was not
established; the PID-only probe does not distinguish process identity reuse.
This failure is recorded rather than treated as a proven backend leak or a
proven test-fixture problem.

## Real backend measurements

| Check | Observed result |
| --- | --- |
| `shared_idle_seconds = 900` | Backend processes gone after 901.293 seconds of inactivity |
| Summed gateway process-tree working set | 331.51 MiB warm to 6.22 MiB idle |
| HTTP/SSE at the idle boundary | Both session IDs and streams retained; zero backend workers |
| Final build, separate 8-second idle cycle | Same-session restart passed; two parallel reads used one new worker |
| Working set in that accelerated cycle | 322.46 MiB warm, 13.82 MiB idle, 320.19 MiB after restart |
| Final installed gateway via HTTP SDK clients | Authentication, lazy catalog (25 tools), parallel reads and single-worker checks passed |

Working set is a point-in-time Windows observation, can include shared pages and
changes with page trimming; it is not a fixed memory requirement. The idle tree
retains the gateway and the console host created by Task Scheduler. All backend
PowerShell/cmd/Node processes disappeared. Process creation times were checked to
distinguish the console host from the backend and avoid PID-reuse errors.

The original 900-second harness stopped after measuring shutdown because it
incorrectly expected exactly one frontend process, overlooking that console host.
It did not check wakeup after the long wait. The long run used the first idle
build; the final build additionally detects external transport closure. The full
900-second wait was not repeated on that final build. Same-session wakeup was
verified independently with the accelerated timer, not inferred from the long run.

An isolated delayed-shutdown mock also checked a request arriving during cleanup:
it waited 5.140 seconds, executed after the old process exited, and the journal
contained exactly two dispatched actions. An external kill between requests
remained a manual failure without automatic dispatch or replay.

## Limits

These real-backend checks used HTTP SDK clients directly against the gateway.
Already-open Codex chats retained their previous stdio connections; their native
tool success is not evidence of routing through the new gateway. A fresh desktop
connection and Windows logout/login were not tested. Opt-in native Chrome/Codex
acceptance suites were not run. No other MCP configuration was changed.
