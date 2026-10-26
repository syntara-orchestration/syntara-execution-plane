# WorkerManager lifecycle

`WorkerManager` is the shared execution coordinator. Every backend extends this
abstract class and implements `prepare(attempt, command)`. Its final `dispatch`
method owns WorkItem transitions, claim renewal, cancellation observation, retry
classification, terminal results, and the completion outbox. The worker process
selects a manager by `ExecutionTarget.backend_type` and delegates the attempt.

Backend implementations return an inert `WorkerAllocation`. They receive a
`WorkAttempt` snapshot containing the WorkItem ID, claim owner, generation, and
assigned target ID. The shared coordinator owns persistence. Preparation resolves
credentials and placement; external resource creation begins in `allocate()`.

| Operation | Responsibility |
|---|---|
| `resource` | Expose deterministic allocation intent, then its acknowledged external UID |
| `allocate()` | Obtain an owned worker or execution slot |
| `wait_ready()` | Establish a usable connection for invocation submission |
| `submit()` | Issue execution once and return a live `Invocation` handle |
| `Invocation.result()` | Observe a terminal `ExecutionResult` |
| `Invocation.cancel()` | Request cancellation while retaining a racing terminal result |
| `release()` | Release or quarantine the worker; report cleanup and confirmed termination separately |

A backend for a shared worker reserves and releases an execution slot.
`release()` handles partial allocation and acts on resources whose ownership is
confirmed. Uncertain ownership defers cleanup for reconciliation.

## Ordering and persistence

The shared coordinator performs these operations in order:

1. Persist `dispatched` with the current claim owner and generation.
2. Start claim renewal and cancellation observation, including during preparation.
3. Prepare the allocation and persist its deterministic resource identity.
4. Allocate the worker and persist its acknowledged UID.
5. Wait for readiness, then recheck cancellation and claim ownership.
6. Issue the invocation and retain its handle.
7. Persist `running`, preserving a concurrent `cancel_requested` transition.
8. Observe the invocation's terminal result and persist it with its completion event.
9. Release the worker and record infrastructure cleanup separately.

A received terminal result is committed before normal worker release. When
cancellation ends before a terminal result arrives, release may provide the
termination evidence needed before EP can persist `cancelled`. Cleanup status
is recorded independently of the persisted execution result.

`dispatched` is the ownership and cancellation fence before any external
allocation. `running` records local Execute submission while EP awaits a terminal
outcome. Confirming server receipt or the start of user code would require an
explicit node-protocol acknowledgement. The WorkItem stays `dispatched` through
Kubernetes Job creation and Pod readiness.

Execute and the database update are separate operations. A controller crash
between them may leave submitted work recorded as `dispatched`. Both expired
`dispatched` and expired `running` claims become `reconciliation_required` and
require an explicit resolution before another execution attempt.

## Cancellation and failure

Before submission, observed cancellation prevents Execute. After submission,
the coordinator keeps the invocation handle even if cancellation wins the
`mark_running` database race. It requests cancellation and waits for a terminal
result during the configured grace period. A terminal result observed during
this race wins. Otherwise, only confirmed termination permits `cancelled`, with
`execution_started` retaining whether Execute was issued.

A start-persistence failure keeps the issued invocation available for result
observation. Submission uncertainty requires reconciliation. Lost claims stop
further progress mutations and release only the old attempt's owned resources.
Controller shutdown joins in-flight resource operations before release. Known
pre-submission work can be requeued after release; uncertain submission requires
reconciliation.

`RetryableDispatchError` permits a retry before the submission stage once release
confirms termination. After submission, a missing terminal result requires
reconciliation. `requeue_pre_execute_failure` changes `dispatched` to `pending`.
The coordinator settles cancellation using submission and termination evidence.
Unexpected errors and uncertain allocation ownership require reconciliation.

## Kubernetes implementation

`VanillaK8sWorkerManager.prepare` resolves the assigned target, Kubernetes
placement, verified cluster endpoint and CA, and just-in-time cluster credential.
`KubernetesWorker` owns a single-attempt Job, its NetworkPolicy, the verified Pod,
and the port-forward. Allocation and readiness are distinct from gRPC submission.
The Job UID, logical WorkItem ID, and claim generation remain separate identities.

Bounded synchronous Kubernetes operations run in threads and are joined before
cancellation can release resources. Readiness polling is asynchronous. The
EP-owned adapter in `worker_manager/node_client.py` uses the vendored protocol
stubs over an async gRPC channel. Submission returns its live call; stream
observation and cooperative cancellation operate on that handle. The shared
coordinator awaits each stage and persists WorkItem transitions between stages.
Protocol types and codecs live in `node_protocol/`; lifecycle adaptation and
result mapping live in `worker_manager/`.

Job and NetworkPolicy deletion use UID preconditions. Unknown ownership defers
cleanup. Cancellation is confirmed only after the owned Job and its Pods are
gone, unless a terminal node result already confirms the outcome. The aged
NetworkPolicy reconciler handles policies left after a crash once their Job and
Pods are gone. The node runtime has no result replay query, so lost results can
still require operator investigation.

## Consumer compatibility

This implementation assumes AO and every EP reader already accept `running`.
Workers emit it unconditionally. The database stores status as a string; EP
readers need an enum mapping that includes `running`. Downgrading readers while
running rows remain requires a compatible release. Public OpenAPI and response
models include `running`; completion delivery remains terminal-only.

## Current scheduler limits

`WorkScheduler.claim_and_place_work()` selects the first active, project-eligible
default target. One worker replica processes one item at a time. Target ranking,
capacity reservation, warm pools, and concurrent dispatch are future scheduler
capabilities. Additional backends implement the shared lifecycle contracts.
