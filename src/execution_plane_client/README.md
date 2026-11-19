# Execution Plane Client

Shared Pydantic command contracts, command catalog, in-memory handler registry
and a generic handler protocol. No HTTP router, network client, persistence,
authentication implementation or EP imports are included.

Participants register their own handlers. EP implements command submission and
resource queries; the caller handles `work.execute.completed` using this same
protocol. Commands have no persisted status or child history. A receipt confirms
that the domain handler returned, not that asynchronous resource work is complete.

## Constructing messages

Instantiate a typed command and call `to_message()`; the command supplies its
wire type and revision and builds the envelope:

```python
from uuid import uuid4

from execution_plane_client.commands.clusters import ClusterRegister

message = ClusterRegister(
    resource_id=uuid4(),
    name="workers",
    endpoint="https://cluster.example",
    credential_ref=uuid4(),
).to_message(
    source_id="syntara-orchestration",
    destination_id="execution-plane",
    client_id="syntara-orchestration",
)

body = message.model_dump(mode="json")  # Send using your application's HTTP client.
```

Every concrete command supports this method, including `ClusterDelete`,
`TargetDelete` and `TargetHealth`. Partial updates serialize only supplied fields, retaining explicit
nulls and placement discriminators without resetting omitted configuration.

By default, each call generates an independent message UUID and UTC timestamp.
Keep the returned message for retries, or pass `message_id` and `occurred_at` to
reconstruct a delivery. Message IDs correlate receipts, not resource identity
or deduplication. Source, destination and client scope are always supplied by
the caller; the helper does not authenticate them.

`WorkExecute` requires a caller-assigned `work_item_id`, which becomes the new
WorkItem's primary key. Submitting an existing ID returns HTTP 409. Use that same
ID for `WorkCancel`, `Completion`, and `CredentialResolveWorkload` requests.

Work execution accepts optional non-secret `runtime_context`, passed to the
plugin and included in work-item reads and completion notifications. See
[runtime context](../../docs/integration.md#runtime-context) for examples and filtering.

## Handling messages

After parsing a `MessageEnvelope`, validate it through the catalog and pass the
returned typed payload to the registered handler:

```python
payload = catalog.validate(message)
handler = registry.get(message.command.command_type, message.command.command_revision)
result = await handler.handle(message, payload, context)
```

Handlers implement `handle(message, payload, context)`. The catalog selects the
payload model by command type and revision and validates its fields. Handlers
narrow the payload to their supported command
types without parsing the raw dictionary again. The original envelope remains
available for routing and receipt correlation; partial-update payloads retain which
fields were supplied. Authentication, authorization and domain-state validation
(including validation after merging a patch) remain participant responsibilities.

Build with `uv build src/execution_plane_client`. See the service's integration
documentation for command examples and the current HTTP authentication contract.
