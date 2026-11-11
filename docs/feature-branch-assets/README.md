# Konflux deployment

[`tools/ci/konflux-prepare-execution-plane.sh`](../../tools/ci/konflux-prepare-execution-plane.sh)
is the Konflux/aap-dev path that deploys the standalone Execution Plane
**without changing the AO operator**. After `deploy-ao`, it applies dispatcher
RBAC, creates an `execution_plane` database on `ao-postgres`, applies
[`deploy/kubernetes/base`](../../deploy/kubernetes/base/) using the image built
from this repo's [`Containerfile`](../../Containerfile) (tag
`localhost:5001/<namespace>/ep:<AAP_VERSION>`), and registers the Kind cluster
through the EP API.

Use the deployment script above for setup. The manifests and hello-world
helpers in this directory are not supported deployment entry points.

Target-cluster dispatcher RBAC lives in
[`deploy/kubernetes/execution-target/rbac.yaml`](../../deploy/kubernetes/execution-target/rbac.yaml).
The combined-service procedure is in [`kind-demo-runbook.md`](../kind-demo-runbook.md).
