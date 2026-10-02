# Open Syntara PRs carried into the migration

The migration branch is based on the merged `feat/ANSTRAT-1803` branch snapshot. These PRs are not included in that snapshot and have not been recreated or closed. Their original Syntara PRs remain the source of truth until follow-up work is split across the two repositories.

| Original PR | Planned follow-up |
| --- | --- |
| [#725 — target metadata and platform mapping](https://github.com/syntara-orchestration/syntara/pull/725) | EP models, registries, placement and migrations here; Syntara integration/schema/contracts in Syntara. |
| [#723 — Kubernetes worker manager](https://github.com/syntara-orchestration/syntara/pull/723) | EP worker manager/protocol here; Syntara dispatch, configuration and compose in Syntara. |
| [#701 — SDK node containers](https://github.com/syntara-orchestration/syntara/pull/701) | Decide node/protocol ownership with #723 before splitting images, runtime and Syntara workflow routing. |
| [#727 — Kubernetes Resource Monitor design](https://github.com/syntara-orchestration/syntara/pull/727) | Newly surfaced after the initial inventory. Reconcile its Execution Plane resource-monitor design docs here during the deferred PR follow-up; the original Syntara PR remains untouched. |
| [#673 — workload data sharing](https://github.com/syntara-orchestration/syntara/pull/673) | Reconcile and move EP documentation here. |
| [#632 — cluster/target/scheduler design](https://github.com/syntara-orchestration/syntara/pull/632) | Reconcile against merged docs and move remaining EP design here. |
| [#648 — OpenShift cold-start POC](https://github.com/syntara-orchestration/syntara/pull/648) | Keep as a historical POC until it is compared with #723/#701; port only unmerged work that remains needed. |

The initial inventory captured six open PRs. PR #727 appeared in the current open-PR list after that snapshot; its head is preserved in a supplemental bundle and `execution-plane-pr-inventory-update-2026-09-30.json`. The migration workspace also retains bundles of the source branch and the six initially inventoried PR heads, plus `execution-plane-pr-inventory.json`. None of these original PRs has been recreated, changed, or closed.
