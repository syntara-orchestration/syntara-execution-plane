# AO and EP service integration

AO owns workflow authorization, activities and user-facing workflow state. EP
owns work, clusters, targets and completion-delivery state in its own database.
The `execution-plane-client` distribution shares only command contracts,
validation, an in-memory handler registry and a generic handler protocol. Its
only runtime dependency is Pydantic; importing it requires no EP configuration.

## Command ingress

All mutations use `POST /v1/commands`. EP does **not** store
commands or provide command-status or command-children endpoints.

```json
{
  "message_id": "00000000-0000-0000-0000-000000000001",
  "source_id": "syntara-orchestration",
  "destination_id": "execution-plane",
  "scope": {"client_id": "syntara-orchestration"},
  "command": {
    "command_type": "work.execute",
    "command_revision": 2,
    "occurred_at": "2026-10-14T08:00:00Z",
    "payload": {
      "work_item_id": "00000000-0000-0000-0000-000000000002",
      "runtime_context": {"project_id": "caller-project", "workflow_id": "caller-workflow", "execution_id": "caller-execution"},
      "image": "registry.example/node:latest",
      "invocation": {
        "step_revision_id": "script.v1",
        "inputs": {"language": "python", "code": "print('hello')"},
        "timeout_seconds": 60
      }
    }
  }
}
```

`payload.work_item_id` supplies `WorkItem.id`. The independent `message_id`
correlates this delivery with its receipt; it does not deduplicate requests. HTTP 202 returns `received`, `message_id` and an optional `resource`
reference. It confirms application of the domain request, not completion of a
job, cluster setup or draining.

The workload image must implement the [SDK container service](workload-protocol.md)
and advertise the requested step revision. EP constructs the SDK invocation ID
from the work UUID and claim generation.
Script environment variables belong in `inputs.environment`. Workload credential
references belong in `invocation.credential_bindings`; see the
[workload protocol](workload-protocol.md#command-and-workload-data) for resolution behavior.

The caller authenticates using the ES256 service JWT and deployed TLS
configuration. `source_id` and `scope.client_id` must match its verified
`client_id`; `destination_id` must match `EP_SERVICE_JWT_AUDIENCE`. The token
must grant the exact command type as a scope. EP checks issuer, audience,
signature, expiration and configured-client identity.

The HTTP body is bounded to 1 MiB; action payloads to 512 KiB. Duplicate JSON
keys, non-finite values, unsupported revisions and malformed payloads are
rejected without echoing private input. Resource conflicts return 409; missing
or other-client resources return 404. There is no generic command deduplication:
a repeated work UUID returns 409. After a lost submission response, query the
resource by its stable UUID before deciding whether to submit again.

Python callers can instantiate a command from `execution_plane_client.commands`
and call `to_message(source_id=..., destination_id=..., client_id=...)` to build
the envelope. The command supplies its type and revision. Each call generates a
new message UUID and timestamp unless explicitly provided. Retain the message
for retries, or pass the original `message_id` and `occurred_at` when rebuilding
that delivery. Rebuilt submissions must retain the same `work_item_id` even
when they use a fresh `message_id`.
Use `message.model_dump(mode="json")` as the body for the application's HTTP
client. See the [client README](../src/execution_plane_client/README.md) for an example.

The generated OpenAPI document exposes `MessageEnvelope` and a `Command` schema
whose `oneOf` lists EP's installed command variants, discriminated by `command_type`.
Each variant fixes its type and revision and references its typed payload component.
The outbound `work.execute.completed` command is documented separately under
`webhooks`, including its required work-item UUID and receipt response.

## Installed commands

| Type (revision 1 unless specified) | Payload and effect |
| --- | --- |
| `work.execute` (revision 2) | `WorkExecute`: required `work_item_id`, optional runtime context, image, versioned invocation, optional output configuration; enqueue existing work lifecycle |
| `work.cancel` | `work_item_id`: existing work UUID; request cancellation through `WorkStore` |
| `cluster.register` | `ClusterRegister`: caller-supplied `resource_id`, name, endpoint, `credential_ref`, optional CA/labels/enabled/`is_default`; register or update only the Cluster. Register targets separately with `execution_target.register`. |
| `cluster.update` | `resource_id`, explicitly changed Cluster fields, and optional `is_default`; `true` atomically promotes the Cluster as the global default |
| `cluster.delete` | `resource_id`; request draining of a Cluster |
| `execution_target.register` | `resource_id`, owned physical `cluster_id`, name, backend, placement, endpoint, `credential_ref`, labels and optional `is_default`; create an independent target and optionally promote it as the Cluster default |
| `execution_target.update` | `resource_id`, mutable target fields and optional `is_default`; `true` atomically promotes the target and demotes the previous default |
| `execution_target.delete` | `resource_id`; request draining of a non-default target |
| `execution_target.health` | `resource_id`; resolve the owning Cluster credential and return one on-demand probe result |

Cluster registration creates or updates only the caller-owned Cluster. Register
ExecutionTargets separately with `execution_target.register`, using the Cluster
UUID as `cluster_id`. Partial updates preserve omitted fields; only the CA
certificate can be explicitly cleared with null on a Cluster. Promoting a
default atomically demotes the previous default. To delete a default Cluster or
target directly, promote a replacement first. See
[cluster and target services](cluster-and-target-services.md) for lifecycle rules.

Unknown work cancellation returns 404. No cancellation command is retained for
work that has not arrived. Cancellation of dispatched work is asynchronous;
observe its resource state or completion command for the final outcome.

## Resource queries

| Endpoint | Required scope |
| --- | --- |
| `GET /v1/capabilities` | `capabilities:read` |
| `GET /v1/work-items[/{id}]` | `work-items:read` |
| `GET /v1/clusters[/{id}]` | `clusters:read` |
| `GET /v1/execution-targets[/{id}]` | `execution-targets:read` |

All resources are scoped to the authenticated client. Cluster and target IDs
are their resource UUIDs. See [read endpoints](read-endpoints.md) for pagination
and filtering.

Queries exclude management credential references, CA material and work invocation data.
Work lists omit result bodies; individual reads return the full stored result,
with `result_size_bytes` indicating its size. This supports recovery if a callback
was missed, without needing command history. Health probes remain at
`/healthz/live` and `/healthz/ready`.

## Credential resolution is a command

EP stores only the AO-owned `credential_ref` UUID. When the worker must contact a
cluster, it constructs a `CredentialResolve` command for that reference, routes it
with the Cluster's source client identity, and posts it to
`EP_CREDENTIAL_RESOLUTION_CALLBACK_URL`. AO returns a correlated
`TransportReceipt` containing a `CredentialResolveCommandResult`; its credential
value is bounded, excluded from `repr`, held only in memory for the current
operation, and never persisted or logged by EP. The
receipt is correlated by `message_id`, and the resolution callback has a dedicated
URL, CA, client certificate/key and timeout settings, all pointing at the caller's
command ingress. Resolution failures before Execute are mapped
to retryable dispatch failures rather than unknown workload outcomes.

## Work completion is a command

EP saves each work outcome and callback delivery state inline on its
`work_items` row. Delivery state tracks attempts, retry timing, errors,
delivery acknowledgement and the delivery lease. EP's
Completion Notifier sends a `work.execute.completed` command to Syntara at
`EP_COMPLETION_CALLBACK_URL` using the configured callback CA and client certificate.

The revision-2 `Completion` payload contains required `work_item_id`, `status`,
`result`, caller `runtime_context`, and optional `error_code`. Status is `completed`,
`failed`, `cancelled` or `reconciliation_required`. `occurred_at` is the work's
completion time. Syntara's request size limit should accommodate the work
invocation's output limit.

Syntara registers a handler for `CommandCatalog().definition("work.execute.completed", 2)`
in its `CommandRegistry`. Syntara authenticates EP's client certificate and
authorizes the command's source, destination and client scope. Both completion
and revision-2 `credential.resolve.workload` use `payload.work_item_id` to look
up the corresponding AO ActivityExecution. Credential resolution also validates
the activity state, workflow-node credential binding, and persisted project
ownership; caller context never supplies authorization.
After handling the callback, Syntara returns a successful HTTP status and a
`TransportReceipt` containing its `message_id`.

Delivery is at least once. Syntara handles repeated and concurrent callbacks
idempotently by WorkItem UUID and acknowledges already-handled completions.
EP retries failed requests and invalid receipts with backoff and reclaims expired
delivery leases.

See [cold-start-node-dispatch.md](cold-start-node-dispatch.md) for execution and recovery.

## Runtime context

`WorkExecute.runtime_context` is an optional finite non-secret JSON object (default
`{}`), bounded by the ordinary payload limit. On the wire it is
`command.payload.runtime_context`, beside `invocation`. EP stores it once in
`WorkItem.runtime_context`, a JSONB column with a GIN index, outside the encrypted
payload. The worker passes that same context to the SDK's `runtime_context`.
Work-item reads and completion notifications expose the same stored value.
Duplicate submissions return 409 and cannot overwrite it.

AO supplies `project_id`, `workflow_id`, and `execution_id` together from its
persisted execution. EP does not interpret these keys. Context does not identify
work, authorize credentials, select targets, or change lifecycle decisions.
Credentials belong in the existing credential callback, never in context.
Target labels and work selectors retain their placement role.

The invocation retains `step_revision_id` to select and validate the advertised
container handler. There is no dedicated or required `workflow_run_id`. Callers
may add a precisely named `temporal_run_id` to context if they need it; AO does
not send one by default. No `metadata` or `payload.context` alias is accepted.

Filter with `GET /v1/work-items?runtime_context[project_id]=<id>`.
Values match exact JSON strings; an empty query value checks key existence.
Multiple keys are ANDed. Keys are literal top-level keys (dots do not traverse
nested objects); commas are literal, and numbers/booleans are not coerced to
strings. Filtering runs before pagination and total counts, within client scope.
Context has no update or sort API.

## Coordinated protocol upgrade

Capabilities advertise protocol revision 2. The envelope no longer accepts
`command_id` or `parent_command_id`. Work execution, completion, and workload
credential resolution use command revision 2; cancellation is `work.cancel`
revision 1 with `work_item_id`. Other resource command payload revisions remain
unchanged. All participants and token scopes must be upgraded together; there
are no legacy wire aliases. Message IDs remain receipt correlation only.

Deploy AO with the matching released `execution-plane-client` package and EP
service. Do not deploy the AO changes with the older client distribution. The
release must update AO's dependency constraint, lockfile, and exported hashed
requirements to the actual published version.

Existing work rows retain their IDs, results, claims, and notification state.
Workers recover identity from `WorkItem.id` when validating stored payloads,
including payloads saved before this protocol revision. Stop EP writers and
workers, back up the database, and run the context migration with the existing
payload encryption key. It decrypts old payloads in batches, moves nested context
and legacy metadata to the column, and retains old workflow run IDs as optional
`temporal_run_id` context. Conflicting keys abort the transaction for explicit
reconciliation. Invocation data remains encrypted, without a second context copy.
The migration is forward-only: new requests cannot reconstruct the old required
workflow run ID. Rollback requires restoring the pre-upgrade backup. Deploy the
matching SDK contracts and rebuild plugin containers as part of the upgrade.

Cancellation and deletion use work-item identity, not command ancestry. The
separate cancellation/deletion implementation can retain `WorkDelete.work_item_id`
and adopt `WorkCancel` without changing cancellation intent, cleanup fencing,
callback suppression, reconciliation, or purge semantics.

During review, the SDK contracts dependency is pinned to an immutable commit on
its feature branch. `requirements-vcs.txt` carries that source separately from
hashed PyPI requirements. Hermeto prefetches both manifests. The offline build
extracts the SDK project and canonical schemas from the pinned Git archive;
local archive URLs cannot select this package subdirectory with uv. It also consumes `requirements-build-sdk.txt` for the SDK's pinned build backend;
`make sync-requirements` regenerates these files. Replace the Git source with the
coordinated package release when it is available.
