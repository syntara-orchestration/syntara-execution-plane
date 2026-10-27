# Cold-start node dispatch

This is the first isolated EP execution path in migration PR #1, paired with AO
PR #728. It supports script nodes and a fresh Kubernetes worker allocation per
execution attempt. The code and deployment changes still need combined-service
and target-cluster validation before this can be described as production-ready.

## Service and node protocol boundary

AO validates the script configuration, chooses a digest-pinned image, and
builds the versioned node invocation. It submits
`{invocation, image, output_config}` to EP's authenticated HTTP API. EP owns
the accepted WorkItem, attempt state, encrypted invocation payload, result,
and completion outbox in its separate database. AO never imports EP stores and
does not connect to the EP database.

The EP worker invokes the node SDK through the gRPC protocol vendored in
[`node_protocol/`](../src/execution_plane/node_protocol/). The protocol source
is the step-types revision
`f2ef663b9d7ae55b2ebcf4a421ece356cb7e6026`; generated runtime requirements are
`grpcio>=1.78.0` and `protobuf>=6.31.1`. The worker tunnels that gRPC channel
through an authenticated Kubernetes API port-forward. Invocation, progress,
errors, stdout/stderr fields, and final output all travel through gRPC. EP does
not create an input Secret, use Kubernetes exec, or read Pod logs for results.

AO persists the encrypted full request before its first HTTP submission. On a
Temporal retry it decrypts and reuses that original payload and stable request
ID, including the selected image and execution limits. EP idempotency rejects a
different payload for the same caller/project/request identity. Failure details
and partial output are retained as Temporal error details in both the
synchronous-response and completion-notification paths.

## Logical attempt and cold-start resource lifecycle

The WorkItem is the logical execution record. A claim generation identifies an
attempt; the backend Job name and UID identify infrastructure resources. They
are persisted separately on the WorkItem. The `WorkerManager` boundary accepts
a WorkItem and owns obtaining/releasing its worker; it does not make a Pod part
of the public work contract.

For this cold-start backend, EP creates a `batch/v1` Job with one completion,
one parallel Pod, `backoffLimit: 0`, a bounded deadline, and a one-hour TTL as a
cleanup backstop. The pod has no service-account token, runs non-root with a
read-only root filesystem and dropped capabilities, and has bounded CPU,
memory, and memory-backed temporary storage. A per-attempt NetworkPolicy is
installed before the Job. The backend discovers the generated Pod by Job label,
checks its Job owner UID, and forwards the gRPC stream to that Pod.

The Job provisions a worker only. It does not define WorkItem completion,
failure, or cancellation semantics. EP commits a received result and its
completion event before asking Kubernetes to remove the exclusively owned
cold-start Job. Cleanup state is tracked separately (`pending`, `complete`,
`deferred`, or `failed`), and an aged-policy reconciler removes orphaned
NetworkPolicies after confirming the corresponding Job and Pods are gone. A
future manager can attach to an already-running worker and apply a different
release/quarantine policy without changing the WorkItem contract.

## Dispatch recovery and cancellation

EP persists `dispatched` before external allocation and renews a claim lease
while the synchronous gRPC call runs. Every worker transition is fenced by its
claim owner and generation. A failure is automatically requeued only when EP
has evidence that Execute was not submitted. Once Execute may have crossed the
gRPC boundary, loss of the result becomes `reconciliation_required`; EP never
allocates a second worker and automatically reruns that attempt. EP emits that
state through its completion outbox so AO fails the activity with
`WorkloadOutcomeUnknownError` instead of leaving it waiting indefinitely. The
WorkItem remains visible for operator investigation; completion notification delivery does not
claim that execution stopped.

The current node runtime accepts one invocation and has no result replay or
status query. If EP loses the terminal frame before its database commit, a Job
being complete cannot reconstruct the business result or prove that repeating
side effects is safe. Operators must investigate and make any retry explicit
with a new request identity. A durable node-result replay protocol would be a
separate SDK/runtime change.

AO cancellation is persisted and relayed to EP. EP requests cooperative gRPC
`Cancel`; if the node stream does not confirm a stopped invocation, the
exclusively owned cold-start Job is deleted as the forced-termination path.
EP reports `cancelled` only after the invocation has stopped and the completion
race is resolved. A received terminal result wins over a concurrent
cancellation. A warm/shared worker must instead return or quarantine the worker;
cancelling one WorkItem must not delete a shared Pod.

## Deliberate first-release limits

- AO is the only configured caller and script is the only EP-dispatched node
  type. Other image-map entries and credential-bearing nodes are not enabled.
- Dispatch is serial with one EP worker replica. The existing first-active,
  default-target selection remains; the placement reconciler is not a scheduler.
- The container image is pinned by digest, but currently lives under a
  maintainer-owned public Quay namespace. Move it to an organization-owned
  publishing pipeline before treating it as a release artifact.
- `output_config` supports field selection. Template expressions still require
  a Syntara-independent evaluator.
- NetworkPolicy enforcement and the port-forward path have not yet been
  demonstrated together on a production-like CNI/cluster.

See [worker-manager.md](worker-manager.md),
[kubernetes-backend.md](kubernetes-backend.md), and the canonical
[service-isolation revisit decisions](service-isolation-revisit-decisions.md).
