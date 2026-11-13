# SDK workload protocol

EP consumes the container service owned by `syntara-plugin-sdk`. The SDK is
the source of truth for `container.proto`, generated protobuf messages, gRPC
stubs, JSON encoding, and event validation. EP does not vendor or regenerate
any of those files.

`syntara-plugin-contracts` distributes the protocol source, schema bundle,
generated bindings, and `ContainerClient`. The Python runtime APIs use the
`syntara_plugin.runtime` import path.
EP's adapter in [`sdk_adaptor.py`](../src/execution_plane/worker_manager/vanilla_k8s/sdk_adaptor.py) connects
that client to EP's cancellation, progress, and attempt lifecycle.

## Installing the pinned SDK

[`pyproject.toml`](../pyproject.toml) declares the `syntara-plugin-contracts`
dependency. [`uv.lock`](../uv.lock)
records the selected package version, registry, artifact URLs, and SHA-256
hashes; the exported `requirements.txt` records the same pinned release and
hashes for pip/Hermeto builds.

Use `uv sync --locked --all-groups` (or `make install`, which runs that
command) for reproducible installation. It requires access to PyPI but no
sibling SDK checkout, private index, or developer-local source override.
Plain `pip install .` can resolve the declared SDK version, but unlike `uv sync`
it does not use the repository's complete lockfile.

To move to another reviewed SDK release, update the version pin and regenerate
the lockfile and requirement exports.

## Command and workload data

`work.execute` accepts `image`, `selectors`, and optional `output_config`.
Its invocation contains:

```json
{
  "workflow_run_id": "workflow-1",
  "step_revision_id": "script.v1",
  "inputs": {
    "language": "bash",
    "code": "printf '%s' \"$GREETING\"",
    "environment": {"GREETING": "hello"}
  },
  "runtime_context": {},
  "credential_bindings": [],
  "timeout_seconds": 60
}
```

The selected workload defines the meaning of inputs and its step revision.
`script.v1` is the integration test workload's revision, not a universal SDK
script identifier. The work UUID identifies the logical work item. Each claimed
attempt has an ID of `<work UUID>-<claim generation>`. The Kubernetes transport
passes that attempt ID to the SDK as `invocation_id` and uses it to name and
label the attempt's resources. Claim generations also fence worker updates so
a stale worker cannot modify a newer attempt.

Script environment variables are ordinary inputs. Optional `credential_bindings`
map `credential_recipe_id` to an AO `credential_ref` UUID. EP accepts up to 16
bindings with distinct recipe IDs and keeps them outside `inputs` and
`runtime_context`. The SDK stream receives a credential handler configured with
these bindings. The handler rejects unbound recipes as `missing`, checks the
request against the active invocation and attempt, and resolves approved recipes
by sending `credential.resolve.workload` to AO with the work item as its parent
command. It discards late responses after cancellation or deadline expiry,
bounds concurrent reads, maps callback failures to safe `unavailable` errors,
and neither caches nor persists credential documents. Cluster API
authentication uses a separate resolver for the Cluster credential.

## Execution and outcomes

EP negotiates `Health`, checks that the image advertises the selected step (or
the SDK's platform-runner wildcard), then submits Execute once. It consumes
`ContainerClient.stream` to forward progress and require a valid terminal
outcome. Cancellation calls the SDK's Cancel RPC; after the grace period EP
closes its dedicated channel and cleans up the owned Kubernetes resources.

EP preserves the SDK's output schema digest, status code, status message,
error category/message, retryability, partial output, and object reference.
Zero status is success; any nonzero uint32 status is a deliberate step failure.
Runtime Failure events are separate from deliberate failed Results. Errors
after Execute may have been submitted never cause automatic replay. A lost
terminal outcome requires reconciliation. Results are persisted before cleanup.

Workload images implement `syntara.container.v1alpha1.ContainerService` for
scripts and other plugin workloads.

## Test workload

The repository includes a small SDK-backed script workload for integration
tests. Build and load it into your test cluster before running Kind dispatch:

```sh
podman build -f tests/fixtures/sdk_script/Containerfile \
  -t localhost/syntara-sdk-script:dev tests/fixtures/sdk_script
# With Docker/Kind, use docker build and then:
kind load docker-image localhost/syntara-sdk-script:dev --name integration-test
```

This is the default local helper image. Set `EP_NODE_IMAGE` (or `--image`) and
`--step-revision-id` to use another SDK-compatible runner. CI builds the fixture
image for integration tests.
