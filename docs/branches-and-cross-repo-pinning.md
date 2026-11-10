# Branches and cross-repo pinning

How the Execution Plane repository organizes its branches, and how feature
branch work moves forward by pinning the surrounding stack (AO, the operator,
aap-dev, and sibling EP branches) on specific revisions in Konflux.

## Branch model

| Branch | Role |
| --- | --- |
| `devel` | Mainline. Everything lands here first; Konflux build pipelines (`ansible-automation-orchestrator-ep-devel-*`) publish per-commit images. |
| `early-access` | Stabilization line ahead of releases (`*-early-access-*` pipelines build from it). |
| `feat/<line>` (e.g. `feat/ANSTRAT-1803`) | Long-running integration lines that cut across repositories. Several coordinated changes live on such a line in *every* participating repo before they converge onto the mainlines. |
| feature branches | Per-change branches, PR'd against `devel` (or a `feat/` line when the change belongs to that integration line). |

The `feat/` lines are the interesting case. The Execution Plane rarely
changes alone: a change typically needs matching work in syntara (AO), the
automation-orchestrator-operator, aap-dev, and sometimes the plugin SDK or
syntara-step-types. No single repository can merge first without breaking
the others, so the coordinated work lives on a branch with the same name
(or a known pairing) in each repository, and CI tests the *combination*
rather than each repository in isolation.

## The Konflux pipeline layers

Every pipeline in `.tekton/` is a thin **PipelineRun stub** resolving a
published bundle pipeline (`ep-api-tests`, `ao-api-tests`, ...) from
`quay.io/aap-ci/tekton-catalog`. The stub's only job is to fill in the
revision parameters:

```
ep-api-tests   (bundle)                ← what runs
  └─ .tekton/automation-orchestrator-ep-e2e-devel-pull-request.yaml   ← thin stub, sets params
```

Two stubs per branch line (`devel`, `early-access`), each in a pull-request
and a push variant:

| Stub | What it deploys and tests |
| --- | --- |
| `automation-orchestrator-ep-e2e-*` | This PR's EP head + the pinned AO/aap-dev, running the EP e2e suite on a Konflux Kind cluster. |
| `ansible-automation-orchestrator-ep-*` | The Konflux build (container image) for the branch. |

The pull-request stubs' CEL gate matches `target_branch == "devel"` (or
`early-access`), plus a path filter so tooling-only changes do not burn a
cluster for nothing.

## The revision knobs

The `deploy-ao-using-aap-dev` task (and the pipelines wrapping it) exposes
one knob per repository in the deployed stack. Each accepts a branch name,
tag, or SHA; each is empty by default, which means "use the published
mainline artifact":

| Param | Controls | Empty default means |
| --- | --- | --- |
| `ep-revision` / `ep-pr-number` | The EP source deployed (only used by pipelines hosted in *other* repositories — in EP-hosted stubs, the EP under test is the PR itself via `{{revision}}`). | prebuilt image / `devel` |
| `ao-revision` / `ao-git-url` / `ao-pr-number` | The syntara (AO backend) source built and deployed. | the pipeline's default (`feat/ANSTRAT-1803` while the EP dispatch work rides that line) |
| `ao-operator-revision` / `ao-operator-git-url` / `ao-operator-pr-number` | Build the operator manager from source instead of deploying the prebuilt OLM bundle. | the prebuilt `devel` bundle |
| `aap-dev-revision` / `aap-dev-git-url` | The aap-dev checkout that drives the whole deployment. | `main` |
| `step-types-*` | The syntara-step-types source for plugin-image e2e. | fixture image |

Two mechanics matter when pinning:

- **Branch must be on the org repository.** The task clones through the
  GitHub App credentials of the *organization* repository; a branch that
  only exists on a personal fork is invisible. Push the branch to the org
  remote (or pin by PR number — see below).
- **PR heads via `refs/pull/N/head`.** When the pinned revision is a
  fork-based PR branch, also set the matching `*-pr-number`: the clone step
  fetches `refs/pull/N/head` on the org repository, which exists for any
  PR regardless of where its head lives, then checks out the revision.

## The pinning patterns

### 1. Mainline default (no pin)

```yaml
- name: ao-revision
  value: feat/ANSTRAT-1803
```

The stub pins AO to the integration line that carries the EP dispatch
features. This is the steady state: EP PRs test against a known-good AO,
and the pin drops to `devel` when the line lands on the AO mainline.
Every long-lived pin carries a comment saying *when* it drops.

### 2. Operator from source

The prebuilt operator bundle lags the source: while a needed capability
(for example, honoring `ENABLE_EXECUTION_PLANE` in the admin-settings, or
a new probe) is merged but not yet in a published bundle, pin the operator
branch:

```yaml
- name: ao-operator-git-url
  value: https://github.com/automation-nexus/automation-orchestrator-operator.git
- name: ao-operator-revision
  value: <branch>
```

`deploy-ao` then builds the manager image from source and deploys it
instead of the bundle. This is the standard way EP work moves ahead of the
operator release cadence — and it disappears again when the bundle catches
up (the pin in the e2e stub was dropped exactly this way once
automation-orchestrator-operator#1573 merged).

### 3. Coordinated feature pairs (validation PRs)

When a change spans two repositories as a *breaking pair* — one side is
useless or broken without the other — neither PR's own CI can go green on
its own. The pattern: a **throwaway draft PR** in one repository whose
stub pins the other side's branch, so a single Konflux run deploys and
tests the pair. Examples from this repository:

- *EP stops auto-provisioning the default ExecutionTarget; aap-dev starts
  registering targets explicitly.* The validation PR was based on the EP
  branch (so the EP under test was the PR's own head) with
  `aap-dev-revision` pinned to the aap-dev branch — plus the mirrored
  stub in the syntara repository pinning both, to run the backend e2e
  suite against the pair.
- *The plugin-image e2e chain.* The step-types e2e pipeline lived as a
  demo stub here first, with `aap-dev-revision` pinned to the branch
  carrying the image-staging hooks, before moving to the
  syntara-step-types repository once that became possible.

Rules of the pattern:

- mark the PR **draft** and title it *throwaway*; state the drop
  condition (usually "once both sides merge") in the description and the
  stub comment;
- base the branch on the side you want tested via `{{revision}}`, pin the
  other sides explicitly;
- only the Konflux check matters — GitHub Actions runs on the stub diff
  and can be ignored.

### 4. CI-dependency pins (temporary workarounds)

Occasionally the *deployment tooling itself* needs a change (an aap-dev
bugfix, a hook behavior). Pin `aap-dev-revision` to the branch carrying
the fix in the stub, run, then drop the pin when the fix merges to
`aap-dev` `main`. The same applies in reverse: repositories hosting
pipelines that deploy the EP can pin `ep-revision` to an EP branch while
waiting for an EP merge.

## Checklist for a new pin

1. Does the branch exist on the **org remote** (or can you pin by PR
   number)? The GitHub App clone cannot see personal forks.
2. Which side is the PR's own head (`{{revision}}`), and which sides need
   explicit params? Set only the latter.
3. Comment every pin with **when it drops** — the branch it tracks, or the
   PR that must merge first.
4. If the pin exists to validate a coordinated pair, consider the mirrored
   stub in the partner repository: the backend e2e suite (ao-api-tests)
   exercises far more than the EP-side suite.
5. For throwaway validation PRs: draft, *throwaway* in the title, drop
   condition in the description.

## Related reading

- `docs/kind-demo-runbook.md` — running the same stack locally
- `docs/feature-branch-assets/README.md` — assets for feature-branch demos
- `.tekton/automation-orchestrator-ep-e2e-devel-pull-request.yaml` — the
  stub referenced throughout
