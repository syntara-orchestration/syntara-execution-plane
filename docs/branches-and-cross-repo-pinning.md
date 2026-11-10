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
| `ep-revision` / `ep-pr-number` | The EP source deployed. Only meaningful in pipelines hosted in *other* repositories — see the note below the table. | — |
| `ao-revision` / `ao-git-url` / `ao-pr-number` | The syntara (AO backend) source built and deployed. | the pipeline's default (`feat/ANSTRAT-1803` while the EP dispatch work rides that line) |
| `ao-operator-revision` / `ao-operator-git-url` / `ao-operator-pr-number` | Build the operator manager from source instead of deploying the prebuilt OLM bundle. | the prebuilt `devel` bundle |
| `aap-dev-revision` / `aap-dev-git-url` | The aap-dev checkout that drives the whole deployment. | `main` |
| `step-types-*` | The syntara-step-types source for plugin-image e2e. | fixture image |

`ep-revision` behaves differently from the other knobs because the
Execution Plane is usually the component *under test*, and every
pipeline treats its own repository as the thing under test:

- **In stubs hosted in this repository** (`automation-orchestrator-ep-*`),
  the EP deployed is always this PR's own head — `{{revision}}` — so the
  stub never sets `ep-revision`. The parameter simply has no role here.
- **In stubs hosted in other repositories** (syntara's `ao-api-tests`,
  syntara-step-types' `step-types-e2e`), `ep-revision` selects the EP that
  gets deployed alongside the component under test: the stub's default
  (`devel`) or a pinned branch for co-testing. A concrete example from
  syntara's own history
  ([`.tekton/automation-orchestrator-api-tests-devel-pull-request.yaml` at
  `6de8af2`](https://github.com/syntara-orchestration/syntara/blob/6de8af2238b9931e49a55170b4cf02b392b1e93b/.tekton/automation-orchestrator-api-tests-devel-pull-request.yaml#L57-L60))
  shows all three roles at once:

  ```yaml
  - name: ao-revision
    value: '{{revision}}'          # syntara's own PR head: the component under test
  - name: ao-operator-revision
    value: AAP-90779-ep-from-source  # operator built from source (pattern 2)
    # (+ ao-operator-git-url)
  - name: ep-revision
    value: v0.1.7                   # the EP pinned to a release *tag*
  ```

  The mirror of this repository's stubs: the hosting repo's PR head is the
  component under test, and the other stack members are pinned around it.
  Note that a pin can be a branch, a tag, or a SHA — a released tag like
  `v0.1.7` is the right choice when the co-test needs a known-good EP
  rather than a moving branch.
- **In the `deploy-ao` task itself**, an empty `ep-revision` means "no EP
  path at all" — every EP step is skipped — unless the
  `ENABLE_EXECUTION_PLANE` flag is set, in which case aap-dev deploys the
  prebuilt `quay.io/aap` EP image.

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

### 1. Integration-line pin (the steady state)

The pipeline's default for `ao-revision` is `devel` — the AO mainline.
The stub overrides it with the integration line that carries the EP
dispatch features:

```yaml
- name: ao-revision
  value: feat/ANSTRAT-1803
```

This is the steady state: EP PRs test against a known-good AO, and the
pin drops back to the `devel` default when the line lands on the AO
mainline. Every long-lived pin carries a comment saying *when* it
drops.

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
