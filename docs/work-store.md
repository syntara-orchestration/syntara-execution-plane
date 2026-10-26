# Execution Plane: Work Store and Scheduler

`WorkStore` is the concrete persistence API for `WorkItem` records and their
existing completion outbox. Consumers depend directly on `WorkStore`.
`WorkScheduler` coordinates claiming work and resolving its placement.

**Sources:** `src/execution_plane/work_item/work_item_store.py`,
`src/execution_plane/work_item/work_scheduler.py`,
`src/execution_plane/work_item/policy.py`, and
`src/execution_plane/models/work_item.py`

## Responsibilities

| Class | Responsibility |
|---|---|
| `WorkStore` | Work commands, client-scoped reads, lifecycle transitions, completion delivery state, and target-drain checks |
| `WorkScheduler` | Claims queued Work, resolves placement, revalidates candidates, and assigns or releases the claim |

Command handlers and REST resource queries construct `WorkStore` directly.
The worker constructs one `WorkStore` and shares it with the scheduler, the shared
WorkerManager lifecycle, Completion Notifier, and drain monitor. Backend
allocations and invocation transports do not access WorkStore. Each method
signature is defined on the concrete store, without separate consumer-specific
interfaces.
Completion-event persistence stays in `WorkStore`; there is no separate
completion store.

## Scheduling and execution

`WorkScheduler.claim_and_place_work()` combines reservation, placement,
no-match failure, candidate revalidation, assignment, and claim release.
`WorkStore` enforces terminal-result status policy, bounded uncertain-outcome
explanations, claim fencing, and non-negative completion retry delays.
`WorkStore.is_target_drained` is the predicate used by `DrainMonitor`.

## Persistence invariants

`WorkStore` keeps concurrency-sensitive behavior in one transaction:

- Queue reservation uses `SELECT ... FOR UPDATE SKIP LOCKED` and rotates claim ownership and generation.
- Target assignment locks the Work item and candidate target/Cluster rows before eligibility is rechecked.
- Claim fencing rejects stale owners or generations before lifecycle updates.
- Cancellation and dispatch transitions are serialized against the same locked Work row.
- Terminal results and their `CompletionEvent` outbox rows are committed together.
- Lease-expired uncertain work is moved to `RECONCILIATION_REQUIRED` with a durable event.

The Store does not expose generic entity mutation. Every method represents an
allowed persistence transition.
`ExecutionTargetStore.finalize_delete` atomically rechecks active work and
detaches historical work while deleting a drained target.

`mark_running` records locally issued execution only for the current dispatched
claim and preserves cancellation. Pre-execution requeue is strictly
`dispatched -> pending`; `cancel_requested` requires a separate cancellation
decision with submission and termination evidence. Lease expiry and drain checks
include `running`.
