# AO and EP service integration

AO owns workflow authorization, activities and user-facing workflow state. EP
owns work, cluster bindings, targets and completion events in its own database.
The `execution-plane-client` distribution shares only command contracts,
validation, an in-memory handler registry and a generic handler protocol. Its
only runtime dependency is Pydantic; importing it requires no EP configuration.

## Command ingress

All mutations use `POST /v1/commands`. The former work submission, cancellation
and cluster-binding mutation endpoints have been removed. EP does **not** store
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
        "version": 1,
        "operation": "execute",
        "inputs": {"language": "python", "code": "print('hello')"},
        "credentials": {},
        "workflow_context": {},
        "settings": {},
        "timeout_seconds": 60,
        "max_output_bytes": 1048576
      }
    }
  }
}
```

The envelope and nested command IDs must match. Work commands use that UUID as
`WorkItem.id`; no project ID, request ID or separate work correlation ID is
stored. HTTP 202 returns `received`, `message_id` and an optional `resource`
reference. It confirms application of the domain request, not completion of a
job, cluster setup or draining.

The caller authenticates using the existing ES256 service JWT and deployed TLS
configuration. `source_id` and `scope.client_id` must match its verified
`client_id`; `destination_id` must match `EP_SERVICE_JWT_AUDIENCE`. The token
must grant the exact command type as a scope. EP retains issuer, audience,
signature, expiration and configured-client checks. This is the current EP JWT
contract, not the optional-journal branch's per-envelope signing protocol.

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
| `cluster.register`, `cluster.provision` | `ClusterRegister`: `resource_key`, `desired_revision`, name, endpoint, namespace, `credential_ref`, optional CA/project grants/labels/enabled/`is_default`; create or replace binding desired state |
| `cluster.update` | `resource_key`, `desired_revision`, explicitly changed binding fields, and optional `is_default`; `true` atomically promotes the Cluster as the global default |
| `cluster.delete` | `resource_key`, `desired_revision`; disable an existing binding for reconciliation |
| `execution_target.register` | `resource_id`, owned physical `cluster_id`, name, backend, placement, endpoint, `credential_ref`, labels and optional `is_default`; create an independent target and optionally promote it as the Cluster default |
| `execution_target.update` | `resource_id`, mutable target fields and optional `is_default`; `true` atomically promotes the target and demotes the previous default |
| `execution_target.delete` | `resource_id`; request draining of a non-default target |

Cluster commands preserve the existing binding revision rules: older revisions
conflict, identical desired state at the same revision is accepted, and different
state at the same revision conflicts. Partial updates preserve omitted fields;
only CA and project grants can be explicitly cleared with null. The
`ClusterBindingReconciler` creates/updates the physical cluster and its default
target and handles deletion. Commands never invoke that provider work directly.
Cluster desired state records the global default choice. The reconciler promotes
an `ACTIVE` Cluster atomically and demotes the previous default; the current
default cannot be explicitly demoted or deleted. If no default exists, the
reconciler promotes the first Cluster that becomes eligible.

Targets use existing resource identities and lifecycle operations, without an
additional revision journal. A Cluster binding seeds its initial default target,
but later Cluster updates do not rewrite target-owned endpoint, credential reference,
placement or label configuration. Target commands may update the current
default's metadata and may promote another target as the default. The current
default cannot be explicitly demoted or deleted; promote a replacement first.
Kubernetes placement patches retain omitted selectors/tolerations.

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

List queries accept `limit` from 1 to 200 (default 50). All resources are scoped
to the authenticated client. Cluster IDs in these routes are the binding's
`source_integration_id` (the command's `resource_key`). Responses include desired
and observed revisions, status and the separate physical `cluster_id`, including
before setup succeeds and after deletion. Use that physical ID to register an
independent execution target. Target IDs are physical target UUIDs.

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
`CredentialResolution`; its credential value is bounded, excluded from `repr`, held
only in memory for the current operation, and never persisted or logged by EP. The
resolution callback has a dedicated URL, CA, client certificate/key and timeout
settings, all pointing at the caller's command ingress. Resolution failures before Execute are mapped
to retryable dispatch failures rather than unknown workload outcomes.

## Work completion is a command

The existing worker persists work outcomes and completion delivery state. Delivery
constructs a `work.execute.completed` envelope each time; it does not persist the
envelope or a command record. Command and message IDs are the UUIDv5 derived by
using the work UUID as the namespace and `work.execute.completed` as the name, while
`parent_command_id` and the work-item resource ID equal the work UUID. Retries
reconstruct the same command and `occurred_at` uses the completion time.

The payload uses `Completion`: `status`, `result` and a `work_item` resource
reference. Terminal work maps to `succeeded`, `failed`, `cancelled` or
`outcome_unknown` for reconciliation-required work. Completion result size is
governed by the work invocation's output limit, rather than the action payload
limit; receiving callers must configure a suitable completion request limit.

Set `EP_COMPLETION_CALLBACK_URL` to the caller's command-ingress URL. It remains
operator-controlled, with the existing callback CA/client certificate settings;
the inbound command cannot redirect delivery. Completion delivery retains its
existing retry/backoff behavior. The receiving participant must authenticate
EP's client certificate and authorize its source, destination and client scope.
The shared client package provides contracts, not authentication or routing.
After handling a completion command, respond with a successful HTTP status and
a `TransportReceipt` containing its `message_id`. Failed requests, malformed
receipts and mismatched message IDs are retried.

The caller registers a handler for `CommandCatalog().definition("work.execute.completed", 1)`
in its own `CommandRegistry`. EP advertises only the action handlers it installs.
AO integration changes are outside this EP repository; AO must consume the new
completion envelope instead of the former event payload.

See [cold-start-node-dispatch.md](cold-start-node-dispatch.md) for provider work
and recovery behavior. No command persistence or database migration is added by
this API change.
