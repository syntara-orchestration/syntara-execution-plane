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
  "command_id": "00000000-0000-0000-0000-000000000002",
  "source_id": "syntara-orchestration",
  "destination_id": "execution-plane",
  "scope": {"client_id": "syntara-orchestration"},
  "command": {
    "command_id": "00000000-0000-0000-0000-000000000002",
    "command_type": "work.execute",
    "command_revision": 1,
    "occurred_at": "2026-10-14T08:00:00Z",
    "payload": {
      "image": "registry.example/node:latest",
      "invocation": {
        "workflow_run_id": "workflow-1",
        "step_revision_id": "script.v1",
        "inputs": {"language": "python", "code": "print('hello')"},
        "runtime_context": {},
        "timeout_seconds": 60
      }
    }
  }
}
```

The envelope and nested command IDs must match. Work commands use that UUID as
`WorkItem.id`. HTTP 202 returns `received`, `message_id` and an optional `resource`
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
new command UUID and timestamp unless explicitly provided, and the message ID
defaults to the command ID. Retain the message for retries, or pass the original
`command_id`, `occurred_at` and, if different, `message_id` when rebuilding it.
Use `message.model_dump(mode="json")` as the body for the application's HTTP
client. See the [client README](../src/execution_plane_client/README.md) for an example.

The generated OpenAPI document exposes `MessageEnvelope` and a `Command` schema
whose `oneOf` lists EP's installed command variants, discriminated by `command_type`.
Each variant fixes its type and revision and references its typed payload component.
The outbound `work.execute.completed` command is documented separately under
`webhooks`, including its required parent work UUID and receipt response.

## Installed commands

| Type (revision 1) | Payload and effect |
| --- | --- |
| `work.execute` | `WorkExecute`: image, versioned invocation, optional output configuration; enqueue existing work lifecycle |
| `command.cancel` | `target_command_id`: existing work UUID; request cancellation through `WorkStore` |
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

The `Completion` payload contains `status`, `result`, an optional `resource`
reference and optional `error_code`. EP's notifier sets `resource` to
`{"kind": "work_item", "resource_id": "<work UUID>"}`. Status is `completed`,
`failed`, `cancelled` or `reconciliation_required`. Syntara's request size limit should
accommodate the work invocation's output limit.

`parent_command_id` identifies the WorkItem, and `occurred_at` is its completion time.

Syntara registers a handler for `CommandCatalog().definition("work.execute.completed", 1)`
in its `CommandRegistry`. Syntara authenticates EP's client certificate and
authorizes the command's source, destination and client scope. Its handler uses
`parent_command_id` to find and complete or fail the originating Temporal activity.
After handling the callback, Syntara returns a successful HTTP status and a
`TransportReceipt` containing its `message_id`.

Delivery is at least once. Syntara handles repeated and concurrent callbacks
idempotently by WorkItem UUID and acknowledges already-handled completions.
EP retries failed requests and invalid receipts with backoff and reclaims expired
delivery leases.

See [cold-start-node-dispatch.md](cold-start-node-dispatch.md) for execution and recovery.
