# ANSTRAT-1803 branch additions and migration status

This map tracks the work added to `feat/ANSTRAT-1803` after the initial service
split and how it is represented by the two migration PRs. The source PRs remain
in Syntara for history; this document records the new repository ownership and
the places where the old monorepo assumptions were intentionally replaced.

| Source PR | Status in this migration | New home or follow-up |
| --- | --- | --- |
| [#723 — Kubernetes worker manager](https://github.com/syntara-orchestration/syntara/pull/723) | Adapted | Alan's backend-specific worker-manager structure is in [`vanilla_k8s`](../../src/execution_plane/worker_manager/vanilla_k8s/manager.py), with the SDK gRPC client in [`node_protocol`](../../src/execution_plane/node_protocol/client.py). The first backend uses a single-attempt Job; WorkItem state is independent of Job/Pod state. |
| [#725 — target metadata and platform mapping](https://github.com/syntara-orchestration/syntara/pull/725) | Migrated across both PRs | EP owns typed placement, clusters, targets, and persistence. AO owns the integration surface and synchronizes its desired state through the EP API. |
| [#727 — Kubernetes Resource Monitor design](https://github.com/syntara-orchestration/syntara/pull/727) | Documentation migrated | [`resource-monitor.md`](../resource-monitor.md) retains the resource-monitor design; this implementation keeps serial dispatch for the first release and does not claim capacity-aware scheduling. |
| [#747 — public node images](https://github.com/syntara-orchestration/syntara/pull/747) | Adapted | AO defaults only the first-release script node to a digest-pinned image. Moving image publication to an organization-owned pipeline remains a revisit item. |
| [#749 — Kind integration test](https://github.com/syntara-orchestration/syntara/pull/749) | Not carried as an end-to-end test | The merged harness wired Kind credentials into AO tests, but those tests use AO's fake EP HTTP client; it did not start the standalone EP API/worker or exercise the service boundary. The combined-service procedure is documented in [`kind-demo-runbook.md`](../kind-demo-runbook.md), and still needs to be run against built AO/EP images. |
| [#741 — Konflux cold-start pipeline](https://github.com/syntara-orchestration/syntara/pull/741) | Old pipeline removed; replacement pending | Its scripts ran EP migrations from an AO backend and wrote EP tables directly, so they cannot validate the isolated topology. Re-enable a Konflux E2E pipeline after EP image publication and standalone database/API/worker deployment are available to that pipeline. |
| [#701 — SDK node containers](https://github.com/syntara-orchestration/syntara/pull/701) | Protocol vendored; image publishing remains upstream work | The gRPC contract and generated client are vendored from step-types PR #2; see [`node-protocol-vendoring.md`](../node-protocol-vendoring.md). The node image build/publish workflow is not owned by these service PRs. |
| [#673 — workload data sharing](https://github.com/syntara-orchestration/syntara/pull/673) | Documentation migrated | Workspace contract is in [`data-sharing-with-workspace.md`](../data-sharing-with-workspace.md). Examples 02 and 03 cover volume and object-store workspaces. |
| [#632 — cluster/target/scheduler design](https://github.com/syntara-orchestration/syntara/pull/632) | Closed upstream | Relevant accepted design is represented by EP's registry/placement docs and models; no source PR recreation is planned. |
| [#648 — OpenShift cold-start POC](https://github.com/syntara-orchestration/syntara/pull/648) | Closed historical POC | Retained as design history only; the current gRPC cold-start implementation supersedes its execution path. |

The combined-service runbook is an unverified procedure, not evidence that the
two PR heads have passed an end-to-end run. Its release gate is to run AO, EP
API, EP worker, and the two separate databases together against a NetworkPolicy-
enforcing Kubernetes cluster, then exercise callback loss, cancellation,
restart, and uncertain Execute outcomes.
