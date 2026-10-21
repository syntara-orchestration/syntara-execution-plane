# SDK workload protocol

EP consumes the container service owned by `syntara-plugin-sdk`. The SDK is
the source of truth for `container.proto`, generated protobuf messages, gRPC
stubs, JSON encoding, and event validation. EP does not vendor or regenerate
any of those files.

`syntara-plugin-contracts` distributes the protocol source, schema bundle,
generated bindings, and `ContainerClient`. The Python runtime APIs retain their
`syntara_plugin.runtime` import path; there is no separate runtime distribution
in the merged SDK.
EP's adapter in [`sdk_adaptor.py`](../src/execution_plane/worker_manager/vanilla_k8s/sdk_adaptor.py) connects
that client to EP's cancellation, progress, and attempt lifecycle.

## Installing the pinned SDK

`pyproject.toml` pins `syntara-plugin-contracts==0.1.0a1` from PyPI. `uv.lock`
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

`work.execute` retains `image`, `selectors`, and optional `output_config` for
scheduling. Its invocation is now:

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
  "timeout_seconds": 60
}
```

The selected workload defines the meaning of inputs and its step revision.
`script.v1` is the integration test workload's revision, not a universal SDK
script identifier. EP supplies an invocation ID derived from the work UUID and
claim generation, and currently supplies an empty credential-file-path map.

Script environment variables are ordinary inputs. There is no released EP
credential broker or secret-mount implementation. The removed `credentials`
field was empty in the script submission path; the new command rejects it and
caller-supplied `credential_file_paths` rather than pretending files are mounted.
Cluster API authentication remains internal to EP's Kubernetes transport.

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

The old `syntara.node.v1.NodeService` is removed. There is one gRPC service for
scripts and other plugin workloads: `syntara.container.v1alpha1.ContainerService`.
AO must submit the new invocation shape, and workload images must implement
this SDK service. Existing queued old-shape work must be drained before rollout;
old script images and payloads are incompatible. This pre-release change does
not install a legacy protocol fallback.

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
image instead of pulling the former NodeService image. This fixture is a test
runner, not the production script runner release.
