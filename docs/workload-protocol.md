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

This demo pins `syntara-plugin-contracts` to commit
`dca70a2be44d68b3e3f0545f9d504f3e20638f6f` from
[`syntara-orchestration/syntara-plugin-sdk`, branch `fix/timeout-result-delivery`](https://github.com/syntara-orchestration/syntara-plugin-sdk/pull/55).
Both EP and `tests/fixtures/sdk_script/Containerfile` install the contracts
subdirectory from the same immutable source archive. That distribution contains
the runtime used by the script fixture. `uv.lock` and `requirements.txt` record
the archive hash.

Use `uv sync --locked --all-groups` for installation. This demo requires access
to GitHub and PyPI; it does not require a sibling SDK checkout. When updating
the SDK pin, update the fixture image and regenerate the lockfile and requirement
exports together.

The source pin adds timeout-result delivery to the `0.1.0a4` SDK already
consumed by EP. Rebuild and load the fixture image before running the timeout
integration test so both client and workload use the fix.

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
these bindings. The handler rejects unbound recipes as `missing` and bound
recipes as `unavailable`; it does not resolve or deliver workload credentials.
Cluster API authentication uses a separate resolver for the Cluster credential.

## Execution and outcomes

EP negotiates `Health`, checks that the image advertises the selected step (or
the SDK's platform-runner wildcard), then submits Execute once. It consumes
`ContainerClient.stream` to forward progress and require a valid terminal
outcome. The invocation timeout remains the execution budget; the SDK adds a
bounded delivery window to its default RPC deadline. EP omits an explicit RPC
timeout so the runtime can report `TIMEOUT`, which EP records as `failed` when
no cancellation was requested. Transport loss still requires reconciliation.
Cancellation calls the SDK's Cancel RPC; after the grace period EP
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
