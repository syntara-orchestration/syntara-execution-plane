# Vendored node protocol

EP vendors the step-types node gRPC client and generated protobuf files under
`src/execution_plane/node_protocol/` so the service can build without importing
the Syntara monorepo or publishing another package. The `.proto` and generated
files are copied from step-types commit
`f2ef663b9d7ae55b2ebcf4a421ece356cb7e6026` (PR #2).

The generated descriptor retains the source module
`syntara_node_protocol.node_pb2`; only gRPC stub imports are adapted to
`execution_plane.node_protocol.node_pb2`. To regenerate from the step-types
checkout, use its pinned toolchain:

```bash
cd ../syntara-step-types
make -C . proto
```

Copy `_protocol/src/syntara_node_protocol/node.proto` and the four generated
`node_pb2*` files into EP. Update the two generated gRPC import statements to
the standalone package path. Run `make check-generated` in step-types before
copying to confirm the source generated files match its protocol source; review
the copy diff as part of any EP protocol update.

EP pins runtime floors `grpcio>=1.78.0` and `protobuf>=6.31.1`, matching the
generated client/runtime requirements. The node runtime is single-invocation:
it accepts one Execute and exits. Do not treat vendoring or the generic
WorkerManager interface as support for warm reuse or concurrent invocations.

EP-specific lifecycle integration lives in `worker_manager/node_client.py` and
`worker_manager/node_result.py`. These adapters use the vendored stubs and codec to
provide staged execution and map terminal results. Synchronize `node_protocol/`
from its upstream source using the vendoring procedure above.
