# WorkerManager boundary

`WorkerManager` is the backend seam for obtaining a worker, invoking one
logical WorkItem, and releasing or returning the worker. Its public interface
does not require a Kubernetes Job or Pod:

```python
class WorkerManager(Protocol):
    async def dispatch(self, work_item: WorkItem) -> dict[str, Any] | None: ...
```

The worker process registers managers by `ExecutionTarget.backend_type`. The
Kubernetes implementation is `VanillaK8sWorkerManager`; it reads the target's
namespace, selectors, and tolerations plus the cluster's verified endpoint and
CA, and resolves the cluster's `credential_ref` just in time. It builds no AO or
Temporal dependency into EP.

## Cold-start implementation

The current manager allocates one single-attempt `batch/v1` Job for each
claim-generation attempt. The deterministic Job name, Job UID, logical WorkItem
ID, and claim generation are distinct identities. The backend verifies the
created Pod's Job owner UID before opening the gRPC port-forward. Invocation and
all application results use the SDK protocol; Kubernetes is used only to
provision, observe, and clean up resources.

The manager persists the terminal result and completion-delivery state on the
WorkItem before
deleting its exclusively owned Job. Cleanup state is recorded separately from
execution state. An aged NetworkPolicy reconciler removes policies left behind
after a controller crash only when both the Job and its Pods are gone.

## WorkItem lifecycle and recovery

EP's WorkItem and attempt lifecycle remains authoritative if a Job disappears,
finishes, or cannot be cleaned up. Claims carry a database owner and generation;
`dispatched` fences external attempt ownership, `running` means Execute has
been submitted and is not yet terminal, and long-running dispatch refreshes
the lease. Every mutation checks the same fence. A stale controller cannot
update a newer attempt.

Known pre-Execute failures, including credential-resolution failures, can be
requeued after backoff. A gRPC failure after Execute might have been submitted is
recorded as `reconciliation_required`,
not automatically rerun. The current SDK runtime has no result replay query, so
a missing terminal frame may require operator investigation. See
[cold-start-node-dispatch.md](cold-start-node-dispatch.md) for the crash windows
and current limits.

Cancellation first reaches the node through its gRPC `Cancel` RPC. For the
exclusive cold-start allocation, EP may delete the Job to force termination and
reports cancellation only after the invocation is confirmed stopped. A terminal
result observed during the race wins. A manager for a shared/warm worker must
return or quarantine it; it must not delete the worker when one WorkItem ends.

## Current scheduler limits

`WorkScheduler.claim_and_place_work()` resolves eligible targets, revalidates
candidates, and assigns a target while holding the work claim. There is no
capacity reservation, warm pool, or WorkWatcher.
