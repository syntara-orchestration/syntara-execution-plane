# WorkerManager lifecycle

`WorkerManager` is the shared execution coordinator. Every backend extends this
abstract class and implements `prepare(attempt, command)`. Its final `dispatch`
method owns WorkItem transitions, claim renewal, cancellation observation, retry
classification, terminal results, and the completion outbox. The worker process
selects a manager by `ExecutionTarget.backend_type` and delegates the attempt.

Backend implementations return an inert `WorkerAllocation`. They receive a
`WorkAttempt` snapshot containing the WorkItem ID, claim owner, generation, and
assigned target ID. They do not receive ORM objects or a WorkStore. Preparation
may resolve credentials and placement, but must not allocate external resources.

| Operation | Responsibility |
|---|---|
| `resource` | Expose deterministic allocation intent, then its acknowledged external UID |
| `allocate()` | Obtain an owned worker or execution slot |
| `wait_ready()` | Establish a usable connection without submitting work |
| `submit()` | Issue execution once and return a live `Invocation` handle |
| `Invocation.result()` | Observe a terminal `ExecutionResult` |
| `Invocation.cancel()` | Request cancellation while retaining a racing terminal result |
| `release()` | Release or quarantine the worker; report cleanup and confirmed termination separately |

A backend for a shared worker can reserve and release an execution slot. It must
never delete a shared worker to cancel one invocation. `release()` must cover
partial allocation and must not destroy resources whose ownership is uncertain.

## Ordering and persistence

The shared coordinator performs these operations in order:

1. Persist `dispatched` with the current claim owner and generation.
2. Start claim renewal and cancellation observation, including during preparation.
3. Prepare the allocation and persist its deterministic resource identity.
4. Allocate the worker and persist its acknowledged UID.
5. Wait for readiness, then recheck cancellation and claim ownership.
6. Issue the invocation and retain its handle.
7. Persist `running` without replacing `cancel_requested`.
8. Observe the invocation's terminal result and persist it with its completion event.
9. Release the worker and record infrastructure cleanup separately.

A received terminal result is committed before normal worker release. When
cancellation has no terminal result, release may provide the termination evidence
needed before EP can persist `cancelled`. A cleanup failure does not change an
already persisted execution result.

`dispatched` is the ownership and cancellation fence before any external
allocation. `running` means EP has issued Execute and has not yet persisted a
terminal outcome. It does not prove that the server received the invocation or
that user code began. A stronger meaning would require an explicit node-protocol
acknowledgement. Creating a Kubernetes Job or observing a Running Pod does not
advance the WorkItem to `running`.

Execute and the database update are separate operations. A controller crash
between them may leave submitted work recorded as `dispatched`. Both expired
`dispatched` and expired `running` claims become `reconciliation_required`;
neither is automatically executed again. Claim renewal does not substitute for
backend submission evidence.

## Cancellation and failure

Before submission, observed cancellation prevents Execute. After submission,
the coordinator keeps the invocation handle even if cancellation wins the
`mark_running` database race. It requests cancellation and waits for a terminal
result during the configured grace period. A terminal result observed during
this race wins. Otherwise, only confirmed termination permits `cancelled`, with
`execution_started` retaining whether Execute was issued.

A start-persistence failure keeps the issued invocation available for result
observation; it never converts it into a pre-execution retry. Lost claims stop
further progress mutations and release only the old attempt's owned resources.
Controller shutdown joins in-flight resource operations before release. Known
pre-submission work can be requeued after release; uncertain submission requires
reconciliation.

`RetryableDispatchError` is valid only before the submission stage and after
release confirms termination. Even a backend that incorrectly raises this error
after submission cannot trigger a retry. `requeue_pre_execute_failure` itself
only changes `dispatched` to `pending`; it cannot infer prior execution from
`cancel_requested`. Confirmed cancellation is a separate coordinator decision.
Unexpected errors and uncertain allocation ownership require reconciliation.

## Kubernetes implementation

`VanillaK8sWorkerManager.prepare` resolves the assigned target, Kubernetes
placement, verified cluster endpoint and CA, and just-in-time cluster credential.
`KubernetesWorker` owns a single-attempt Job, its NetworkPolicy, the verified Pod,
and the port-forward. Allocation and readiness are distinct from gRPC submission.
The Job UID, logical WorkItem ID, and claim generation remain separate identities.

Bounded synchronous Kubernetes operations run in threads and are joined before
cancellation can release resources. Readiness polling is asynchronous. The node
client uses an async gRPC channel; submission returns its live call, and stream
observation and cooperative cancellation are separate operations. There are no
transport callbacks for persistence or execution-start authorization.

Job and NetworkPolicy deletion use UID preconditions. Unknown ownership defers
cleanup. Cancellation is confirmed only after the owned Job and its Pods are
gone, unless a terminal node result already confirms the outcome. The aged
NetworkPolicy reconciler handles policies left after a crash once their Job and
Pods are gone. The node runtime has no result replay query, so lost results can
still require operator investigation.

## Consumer compatibility

This implementation assumes AO and every EP reader already accept `running`.
Workers emit it unconditionally. There is no database enum migration, but older
EP ORM enum mappings cannot deserialize the new value. Downgrading readers while
running rows remain requires a compatible release. Public OpenAPI and response
models include `running`; completion delivery remains terminal-only.

## Current scheduler limits

`WorkScheduler.claim_and_place_work()` still selects the first active,
project-eligible default target. The placement reconciler does not rank targets.
There is no capacity reservation, warm pool, or concurrent dispatch. One worker
replica processes one item at a time. The lifecycle contracts allow additional
backends without claiming those scheduling capabilities already exist.
