# Historical feature-branch assets

`konflux-prepare-execution-plane.sh` is the Konflux/aap-dev path that deploys
the standalone Execution Plane **without changing the AO operator**. After
`deploy-ao`, it applies dispatcher RBAC, creates an `execution_plane` database
on `ao-postgres`, applies [`deploy/kubernetes/base`](../../deploy/kubernetes/base/)
using the aap-dev `ep` image (`localhost:5001/<namespace>/ep:<AAP_VERSION>`),
and registers the Kind cluster through the EP API.

The remaining files (`execution-plane-worker.yaml`, hello-world helpers) still
reflect the pre-split in-process worker and should not be applied as-is.

Target-cluster dispatcher RBAC lives in
[`deploy/kubernetes/execution-target/rbac.yaml`](../../deploy/kubernetes/execution-target/rbac.yaml).
The combined-service procedure is in [`kind-demo-runbook.md`](../kind-demo-runbook.md).
