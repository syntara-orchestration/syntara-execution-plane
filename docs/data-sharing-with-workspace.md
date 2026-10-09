# Execution Plane: Data sharing with workspace

This document is the **workspace** contract: successive WorkItems in
one Automation Orchestrator (AO) workflow share a directory.

- Ticket: [AAP-94189](https://redhat.atlassian.net/browse/AAP-94189)
- Feature: [ANSTRAT-1803](https://redhat.atlassian.net/browse/ANSTRAT-1803)
- Parent epic: [AAP-82060](https://redhat.atlassian.net/browse/AAP-82060)
- Labels contract: [labels.md](labels.md)
- Placement examples: [examples/](examples/)

## What this document is

A first cut of the **data-sharing contract** on the WorkItem payload:
data sharing via the **workspace** feature. It names who creates the
workspace (a dedicated API call), where it is mounted, how concurrent
WorkItems share it, and a payload shape EP can implement without
knowing what a Project or Workflow is.

The **workspace** is the same idea on every backend (Kubernetes PVC or
Podman volume). This is not a PVC spec, not AO FileManager
([file-storage.md](https://github.com/syntara-orchestration/syntara/blob/devel/backend/docs/file-storage.md)), and not the in-container SDK.

Placement stays in [labels.md](labels.md).
Volume vs object-store workspace is in
[example 02](examples/02-data-sharing-with-workspace.md) (same
cluster, a volume) and
[example 03](examples/03-data-sharing-with-workspace-object-store.md)
(the same three WorkItems, but an S3 snapshot so they can run on
different clusters).

## The workspace in one page

A **workspace** is a UUID that names a shared directory. By default
that directory is mounted at `/workspace`.

1. **AO creates the workspace.** AO mints a UUID and `POST`s it to
   EP **before** any WorkItem cites that id. A good default is **one
   workspace per workflow**. AO puts that same id on every WorkItem
   that should see the files. Making the workspace optional is an
   AO / UX problem; EP only sees the id on the payload, or its
   absence. Submitting a WorkItem does **not** create a workspace.
2. **EP provisions the volume.** AO does not pick the
   ExecutionTarget. After the first WorkItem is matched, and
   **before** it is dispatched, EP creates the volume on that
   target. That is the backing store, not the workspace identity.
3. **Containers see `/workspace`.** That path is the convention
   Extensions write and read. Paths in `activity.params` (`dest`,
   `playbook`, …) are absolute paths under that mount. Coordinate
   with [ANSTRAT-2422](https://redhat.atlassian.net/browse/ANSTRAT-2422)
   (step types / Extension SDK) so Git, HTTP, playbook, and other
   images agree on `/workspace`.
4. **AO sets the access mode.** `rw`, `ro`, or `copy` tells EP
   whether this WorkItem may run in parallel with others on the same
   id. EP does **not** infer that from Extension SDK input/output
   metadata.

```text
workspace id 7c1a9f3e-4b2d-41a8-9c1f-91c0d4e5a6b7  →  unique in the Execution Plane
                          →  volume on ExecutionTarget ep-default
                          →  mounted at /workspace

AO execution (one workspace UUID, passed to each WorkItem)
  node A  WorkItem  workspace=7c1a9f3e-4b2d-41a8-9c1f-91c0d4e5a6b7  →  ep-default  →  writes /workspace/state.json
  node B  WorkItem  workspace=7c1a9f3e-4b2d-41a8-9c1f-91c0d4e5a6b7  →  ep-default  →  reads  /workspace/state.json
```

## Principles

1. **Git and HTTP downloads are ordinary WorkItems.** There is no
   `data.inputs` list on the playbook (or other) WorkItem. To get a
   repo or a file onto disk, AO submits another WorkItem whose
   `activity.image` is a Git client (for example
   `registry.redhat.io/ao/git-clone:1.0.0`) or an HTTP client (for
   example `registry.redhat.io/ao/http-request:1.0.0`). That
   container writes into `/workspace`. EP does not clone or GET
   inside the Worker Manager.
2. **AO writes the exact link. EP just uses it.** The author may
   say "this file" or "branch main". AO turns that into something
   that cannot move **before** it submits the WorkItem: a download
   URL for a file it already stored
   ([file-storage.md](https://github.com/syntara-orchestration/syntara/blob/devel/backend/docs/file-storage.md)), or a clone URL plus one
   commit SHA for Git. EP does not ask AO "where is that file?" and
   does not ask Git "what is `main` today?"
3. **JSON result ≠ file bytes.** `WorkItem.result` stays a small
   JSON blob (stdout, return code). Large files stay in the
   workspace, not stuffed into the result.

## Who creates the workspace

AO mints the UUID and **creates** the workspace with a dedicated EP
API call **before** any WorkItem cites that id. Illustrative:

```http
POST /workspaces
```

```json
{
  "id": "7c1a9f3e-4b2d-41a8-9c1f-91c0d4e5a6b7"
}
```

EP refuses a duplicate id. A WorkItem that cites an unknown UUID
fails. Submitting work never creates a workspace.

The `POST` does not take an ExecutionTarget. AO does not know the
target yet, and it does not create the volume.

**Default:** one workspace per workflow. Every WorkItem of that run
carries the same id. AO knows the workflow graph, so AO decides which
WorkItems share the folder. EP does not introspect `activity.params`
to guess that two path-shaped arguments should share a directory.

A WorkItem with no `workspace` field does not mount a workspace.
Whether the UI exposes that as an opt-out is an AO problem.

The WorkItem carries the **UUID**, not a PVC or Podman volume id. EP
**looks up** that id on every WorkItem. The workspace must already
exist. If no volume is attached yet, this is the first WorkItem: after
reconcile and before dispatch, EP provisions the volume on the chosen
ExecutionTarget. If a volume already exists, later WorkItems pin to
that ExecutionTarget. The Worker Manager mounts it. Size is the
target's **default workspace size**. There is no size field on the
WorkItem and no per-workspace override for now.

```json
"data": {
  "workspace": "7c1a9f3e-4b2d-41a8-9c1f-91c0d4e5a6b7"
}
```

| Field | Meaning |
|---|---|
| `data.workspace` | Workspace **UUID**. Unique across every ExecutionTarget. |
| Mount path | `/workspace` by default. Override with `{ "id": "7c1a9f3e-4b2d-41a8-9c1f-91c0d4e5a6b7", "path": "/work" }`. |
| Size | Default on the **ExecutionTarget**. The WorkItem does not override it. |

## Default mount `/workspace`

One default location: `/workspace`. Every Extension that reads or
writes workflow files should use that mount. If this contract is
accepted, coordinate with
[ANSTRAT-2422](https://redhat.atlassian.net/browse/ANSTRAT-2422) so
the step SDK and Extension images document the same path.

This document does **not** introduce an SDK helper that rewrites
relative paths onto the mount. The examples use absolute paths
(`dest: "/workspace/src"`, `playbook: "/workspace/src/site.yml"`).
An override of the mount path (`"path": "/work"`) is for the rare
image that cannot use `/workspace`; it is not the common case.

## Access modes and concurrency

WorkItems that share a workspace are **not** ordered by EP reading
the workflow definition. AO already sequences the graph (including
split/merge and loops) when it submits WorkItems. What EP needs is
whether **this** WorkItem may overlap with another on the same id.

AO sets that with `access`. Depending on the mode, EP can expose the
workspace differently:

| Flag | Disk | Parallel | Writes |
|---|---|---|---|
| **`rw`** | Writable shared tree | **No.** One WorkItem at a time. | Persist (volume stays, or snapshot after exit). |
| **`ro`** | Read-only view of the current tree | **Yes.** Loops and parallel branches. | None. Nobody thinks they own the tree. |
| **`copy`** | Private writable clone | **Yes.** Loops and parallel branches. | Local only. Changes are discarded unless AO publishes that copy as a new generation. Two parallel copies do not merge. |

Omitted flag is `rw`. Illustrative payload:

```json
"data": {
  "workspace": {
    "id": "7c1a9f3e-4b2d-41a8-9c1f-91c0d4e5a6b7",
    "access": "ro"
  }
}
```

EP does **not** infer `ro` vs `rw` from Extension SDK metadata
(path-typed inputs vs path-typed outputs). A Git clone writes; a
playbook may read and write; a later reporter may only read. AO
knows that. Guessing from parameter types would mix the step SDK
into EP placement, which this contract avoids.

On a **volume**, `rw` is ReadWriteOnce: Kubernetes and Podman both
refuse a second RW mount of the **same** volume. The Work Scheduler
does not dispatch a second `rw` item for that id until the first has
exited and released the mount. `ro` is a read-only mount of the same
tree. `copy` is a clone; the original stays put.

On an **object-store snapshot**, the same flags apply to a hydrated
copy of the tree. B and C can run **in parallel** after A's snapshot
is `available`. They must not both be exclusive `rw` on the same
generation.

[Example 02](examples/02-data-sharing-with-workspace.md) and
[example 03](examples/03-data-sharing-with-workspace-object-store.md)
keep the happy path: three successive `rw` WorkItems, one at a time.

## Volume workspace

A workspace is a UUID that names a shared directory. That directory
may be a volume (Kubernetes PVC or Podman volume) or a snapshot from
object storage (S3). This section is the volume backend.

The UUID is unique across all ExecutionTargets. A volume workspace
lives on **exactly one** ExecutionTarget. There is no second
workspace with that UUID, on this target or any other.

WorkItems that carry that id run on the target that holds the volume
and see it at `/workspace` by default. Node A writes a file; node B
with the same id reads it. No S3 round-trip.

Once the volume exists, **reuse is a placement constraint.** The Work
Scheduler looks up the UUID, sees the volume on that ExecutionTarget,
and uses **that target as the only candidate**. It does not ask the
reconciler for a different place. Selectors on later WorkItems do
not move the volume.

Those selectors still apply to the pinned target. If they do not
match its effective labels — B requires `env=production` and the
volume is on a target that does not advertise `env` — the WorkItem
fails (`NO_MATCHING_TARGETS`). The error explains the constraint: the
volume pins this WorkItem to that target, and that target does not
satisfy the selectors. The pin does not ignore placement metadata.
WorkItems that must run on different targets use an object-store
snapshot, not a volume.

[Example 02](examples/02-data-sharing-with-workspace.md) is three
WorkItems on one volume, one ExecutionTarget.

### Alternative: object-store snapshot

A volume pins the workspace to **one** ExecutionTarget (one
cluster). The alternative is to publish a **full copy** of
`/workspace` to object storage (S3) when a writer exits, and
hydrate `/workspace` from that snapshot on the next WorkItem.

That snapshot is the **whole tree**. Any ExecutionTarget that can
reach the bucket can run the next WorkItem. AO does not have to keep
those nodes on the same cluster.

```text
workspace id 7c1a9f3e-4b2d-41a8-9c1f-91c0d4e5a6b7
                          →  snapshot s3://workspaces/7c1a9f3e-…/

AO execution
  node A  access=rw    →  cluster-1  →  writes /workspace  →  EP PUTs snapshot
  node B  access=ro    →  cluster-2  →  hydrates snapshot, reads only
  node C  access=copy  →  cluster-3  →  hydrates a private writable copy
```

This PUT **can** gate the next WorkItem when that item runs on
another target: it cannot hydrate until the snapshot is
`available`. A successor on the **same volume** still does not wait
on S3.

| System | Pros | Cons |
|---|---|---|
| **Volume** (PVC / Podman volume) | Live directory; no pack/unpack. **Better for a large workspace:** the next WorkItem does not upload the tree. Next WorkItem on the same target is unmount + mount. Native Kubernetes and Podman. Writes are visible on disk immediately. | Pinned to one ExecutionTarget / cluster. ReadWriteOnce for `rw`: one mount at a time; parallel `rw` nodes queue. Target going away takes the tree with it. OpenShell has no volume attach. |
| **Object-store snapshot** (S3) | Any cluster. Parallel `ro` or `copy` without RWO. Survives the original target. Hydrate into a container filesystem (OpenShell). | Full-tree PUT/GET every generation (time, bandwidth, cost). Cross-target next WorkItem waits for `available`. Parallel `copy` has no merge; two writers still need `rw` sequencing or an AO rule. Extra contract: snapshot format (tar vs prefix) and generation id. |

Leaning for MVP: **volume**. Snapshot is the path when AO must place
WorkItems on different clusters, run `ro` / `copy` in parallel, or
share a tree on a backend with no volume attach.
[Example 03](examples/03-data-sharing-with-workspace-object-store.md)
is the same three WorkItems as example 02, on two Clusters, using S3
instead of a PVC.

## Workspace life time

AO **creates** the workspace with `POST` and later **deletes** it
with `DELETE`. Both are API calls. **MVP is the API.** That is how
AO scopes the folder's life.

The `POST` registers the UUID. It does not pick a target and does
not create a volume. EP looks up the id on each WorkItem (unknown id
fails). It provisions the volume on the ExecutionTarget chosen for
the **first** WorkItem that cites that id, after reconcile and
before dispatch. Later WorkItems with the same id reuse that volume.

**Deletion** is EP deleting the volume (if any) and forgetting the
id. AO `DELETE`s when it is done with the folder (for example when
the execution finishes).

TTL is a safety net if nobody DELETEs. The ExecutionTarget default applies.

| How | Who | When | Priority |
|---|---|---|---|
| **API create** | AO | `POST` before any WorkItem cites the id | **MVP.** |
| **API delete** | AO | `DELETE` by id, at any time | **MVP.** AO owns the folder's life. |
| **TTL** | Execution Plane | For example `3h` **after last unmount** | Also useful. User-controlled. |

The TTL clock is **last unmount**: it starts when a WorkItem that held
the volume exits, and it resets on every later unmount of the same
id. A workspace that is still mounted is never deleted by TTL; a long
run can outlive the original TTL. If the volume was never mounted,
`POST` time counts as the last unmount (idle from birth).

## Getting Git and HTTP files onto `/workspace`

The author attached a file, pointed at a bucket, or pointed at a Git
repo. EP does not grow a fetch feature for that. AO submits a normal
WorkItem whose image already speaks HTTP or Git. That WorkItem
writes under `/workspace`. The next WorkItem on the same UUID sees
the files. There is no `data.inputs` list.

### HTTP (object store)

AO already stores uploads in S3-compatible storage
([file-storage.md](https://github.com/syntara-orchestration/syntara/blob/devel/backend/docs/file-storage.md)). It turns a file id into an
HTTP(S) URL (presigned, or the platform file HTTP API) and submits a
WorkItem whose activity is HTTP:

```json
{
  "payload": {
    "activity": {
      "image": "registry.redhat.io/ao/http-request:1.0.0",
      "params": {
        "url": "https://files.example.com/files/abc123/site.yml",
        "dest": "/workspace/site.yml"
      }
    },
    "data": {
      "workspace": "7c1a9f3e-4b2d-41a8-9c1f-91c0d4e5a6b7"
    }
  }
}
```

`activity.params` is owned by the HTTP activity (AO / Extension), not
by this contract. The activity must write the body to a filesystem
path (for example `dest`), not only into `WorkItem.result`.

A later node that does not share the workspace does not pull
bytes through a second HTTP-activity WorkItem. A different
target cannot mount that UUID; AO either shares a workspace on that
target or uses an object-store snapshot.

### Git

AO turns the author's branch or tag into a clone URL plus a **pinned
commit SHA** (the same way it resolves an image tag → digest for
`activity.image`) and submits a Git activity:

```json
{
  "payload": {
    "activity": {
      "image": "registry.redhat.io/ao/git-clone:1.0.0",
      "params": {
        "uri": "git+https://gitlab.example.com/org/playbooks.git",
        "ref": "a1b2c3d4e5f6",
        "dest": "/workspace/src"
      }
    },
    "data": {
      "workspace": "7c1a9f3e-4b2d-41a8-9c1f-91c0d4e5a6b7"
    }
  }
}
```

`activity.params` is owned by the Git activity, not by this contract.
EP does not follow `main`. A floating branch name would make two
nodes of one execution clone different trees. Credentials are a
Credential Provider reference on the activity, not a password in the
URI.

The clone is writeable on disk like any other workspace files. Pushing
commits back to the remote is not a use-case here; later nodes see
whatever was left under `/workspace`.

Submodules, LFS, sparse checkout, and clone depth are the Git
activity's problem, not EP's. Lean MVP for that Extension: one repo,
one SHA, full tree, shallow clone, no LFS.

### What EP does

That WorkItem mounts the workspace UUID; the Git or HTTP image writes
at a path under `/workspace`. The next WorkItem on the same UUID
reads the files. The Worker Manager does not clone or GET as a
start-of-run step on the playbook WorkItem.

OpenShell has no volume attach. HTTP and Git activities still run;
they cannot leave files on a workspace for the next WorkItem.
Cross-run files on that backend go through an object-store snapshot
or stay in that one container.

[Example 02](examples/02-data-sharing-with-workspace.md) is three WorkItems on one
volume workspace. Files under `/workspace` are still there for the
next WorkItem. There is no `data.inputs` list.

## Payload shape

Illustrative keys only. Not the final field design.

```json
{
  "selectors": {},
  "payload": {
    "activity": {
      "image": "registry.redhat.io/ao/ansible-playbook:1.0.0",
      "params": {
        "playbook": "/workspace/src/site.yml"
      }
    },
    "data": {
      "workspace": "7c1a9f3e-4b2d-41a8-9c1f-91c0d4e5a6b7"
    }
  }
}
```

| Block | Direction | Shared across WorkItems? |
|---|---|---|
| HTTP or Git WorkItem + `workspace` | URL or clone → `/workspace` | Yes, once that Git/HTTP WorkItem has written. |
| `workspace` | live directory, **UUID unique across all ExecutionTargets**, volume on one target | Yes, **successive** `rw` WorkItems on **that** UUID, or parallel `ro` / `copy`. Reuse of a volume pins those WorkItems to that target. Default path `/workspace`. AO `POST`s then `DELETE`s by id (MVP). TTL is an optional user-set safety net (last unmount). |

Omitted `workspace` means "none". There is no `data.inputs`.

## Who does what

```
AO
  mint one workspace UUID per workflow (default)
  POST /workspaces {id}   ← creates the workspace; duplicate id refused
  put that id on each WorkItem
  resolve file id → HTTP(S) URL; Git ref → clone URL + SHA
    → HTTP or Git WorkItem (workspace UUID, access)
        → Work Scheduler
              lookup UUID (unknown id fails the WorkItem)
              no volume yet: reconcile, provision volume on selected ET
              volume exists: pin to that ET; selector miss → NO_MATCHING_TARGETS (explain the pin)
              serialize rw; allow parallel ro / copy
        → Worker Manager
              workspace id → volume on its ExecutionTarget → /workspace
        → Work Watcher
              WorkItem.result (JSON)
        → Completion Notifier → AO
```

| Concern | Owner |
|---|---|
| Upload UI, `POST /files`, `FileMetadata` | AO |
| Mint globally unique workspace UUID (default: one per workflow) | AO |
| Create workspace (`POST` by id against EP). **MVP.** Duplicate id refused. | AO |
| Decide which WorkItems carry that UUID; optional workspace | AO |
| Flag workspace access (`rw` / `ro` / `copy`) | AO |
| Provision volume on the selected ET (first WorkItem, after reconcile, before dispatch) | Work Scheduler |
| Default workspace size (no per-workspace override) | ExecutionTarget |
| Look up workspace UUID (must already exist; never create from a WorkItem) | Execution Plane |
| Fail a WorkItem whose workspace UUID is unknown | Execution Plane |
| Fail a volume-reuse WorkItem whose selectors miss the pinned target (`NO_MATCHING_TARGETS`, explaining the pin) | Work Scheduler |
| Place the first WorkItem that cites a workspace with no volume yet | ExecutionTarget Reconciler (selectors; ignores `data`) |
| Place later WorkItems that reuse a volume workspace | Work Scheduler (only the ET that holds the volume; selectors must still match) |
| Serialize dispatch per workspace id (volume RWO / snapshot `rw`; `ro` / `copy` may overlap) | Work Scheduler |
| Hydrate / publish workspace snapshot (full tree to S3) | Worker Manager / SDK |
| Delete workspace (`DELETE` by id against EP). **MVP.** AO scopes the folder's life. | AO |
| Purge workspace (optional TTL from last unmount). User-set; ET default if omitted. | Execution Plane |
| Map file ids to HTTP(S) URLs; run HTTP activity into `/workspace` | AO |
| Map repo + branch/tag to clone URL + SHA; run Git activity into `/workspace` | AO |
| Credential reference for Git remote (or HTTP if not presigned) | AO → Credential Provider |
| Mount workspace id at `/workspace` | Worker Manager |
| Match ExecutionTarget | First WorkItem (no volume yet): Reconciler (ignores `data`). Volume workspace reuse: Work Scheduler (ET that holds the volume). |
| Document `/workspace` on Extension images | AO / [ANSTRAT-2422](https://redhat.atlassian.net/browse/ANSTRAT-2422) |

EP does not become a file manager. If the HTTP URL or Git remote is
unreachable from the ExecutionTarget, that fetch WorkItem fails. That
is a connectivity / credential problem, not a selector miss.

## Materialization (leaning)

The backing store is a volume on the ExecutionTarget. Object-store
bytes and Git trees arrive because an HTTP or Git activity wrote
them there.

| Mechanism | Fits | Warm pool |
|---|---|---|
| **HTTP or Git activity** writing into `/workspace` | K8s and Podman (needs the workspace volume) | The activity itself yes; workspace is one `rw` holder per id |
| **Workspace volume** (globally unique UUID, one ET) at `/workspace` | K8s PVC and Podman volume | One `rw` holder per id: concurrency 1. `ro` / `copy` may overlap. |
| **Workspace snapshot** (full tree to S3, hydrate anywhere) | All backends that can reach the bucket (incl. OpenShell) | `rw` serial; `ro` / `copy` may run in parallel |

OpenShell has no volume attach. A shared `/workspace` on OpenShell is
the object-store snapshot path; HTTP and Git activities cannot leave
files for the next WorkItem via a volume on that backend.

Leaning for MVP: **HTTP or Git activity + workspace volume** for
inbound files. Object-store **workspace snapshot** is the alternative
when AO needs another cluster, parallel `ro` / `copy`, or OpenShell.
The Worker Manager attaches the workspace UUID at `/workspace`. It
does not GET object storage or clone Git as WorkItem **inputs**.

Because a volume is ReadWriteOnce for `rw`, a second container cannot
mount the same id while the first `rw` WorkItem is running. Two
different ids are different volumes. That is not a per-WorkItem PVC
create; it is one id, one `rw` mount, one WorkItem.

[Example 02](examples/02-data-sharing-with-workspace.md) is the volume workspace,
not an input list. Workspace is at `/workspace` unless overridden.

## Sequence (workspace on one ExecutionTarget, two nodes)

```mermaid
sequenceDiagram
    participant AO as Automation Orchestrator
    participant API as Execution Plane API
    participant WS as Work Store
    participant WM as Worker Manager
    participant HTTP as HTTP activity
    participant Play as playbook
    participant Vol as workspace 7c1a9f3e-4b2d-41a8-9c1f-91c0d4e5a6b7 on ep-default
    participant S3 as object storage

    AO->>API: POST /workspaces {id: 7c1a9f3e-…}
    AO->>WS: WorkItem A { image: http-request, workspace: 7c1a9f3e-… } on ep-default
    WM->>HTTP: start with /workspace mounted
    HTTP->>S3: GET url
    HTTP->>Vol: write /workspace/site.yml
    HTTP-->>WM: exit
    Note over Vol: RW volume released; only now can B mount the same id as rw

    AO->>WS: WorkItem B { image: ansible-playbook, workspace: 7c1a9f3e-… } on ep-default
    WM->>Play: start with same workspace id at /workspace
    Play->>Vol: read /workspace/site.yml
    Play->>Vol: write /workspace/out/report.json
    Play-->>WM: exit
    Note over Vol: RW volume released; tree remains until AO DELETE
```

## Out of scope

| Omitted | Why |
|---|---|
| Placement / selectors | [labels.md](labels.md) |
| Live PVC / Podman volume or disk capacity as a selector | Not a label. Open question in labels.md. |
| AO file upload, conversion, RBAC | [file-storage.md](https://github.com/syntara-orchestration/syntara/blob/devel/backend/docs/file-storage.md) |
| In-container activity SDK | Separate SDK design. Default mount is `/workspace`; coordinate with ANSTRAT-2422. |
| Inferring `rw` / `ro` from Extension SDK path-typed inputs/outputs | AO sets `access` explicitly. |
| Customer S3 IAM setup | Platform / credential work. |
| Git write-back (commit, push, PR) | Git activity is clone into `/workspace`. |
| Cross-target **volume** | A volume lives on exactly one ExecutionTarget. Cross-target: object-store snapshot. |
| Per-workspace size override | Size is the ExecutionTarget default. Not on the WorkItem. |
| `data.inputs` | Use a Git or HTTP WorkItem that writes into `/workspace`. |
| Streaming stdout as files | Still `WorkItem.result` / log plumbing |

## Open questions

1. **Credential reference shape.** Payload-level secret is wrong.
   HTTP and Git activities may take a Credential Provider id, or AO
   mints a presigned URL so an HTTP GET is unauthenticated.
2. **Two workspace ids on one target at once.** Different volumes
   could in principle mount in parallel. Lean: serialize per id;
   two ids may run together if the target has capacity.
3. **OpenShell workspace.** No volume attach. Object-store snapshot
   (hydrate into the container), or no shared directory on that
   backend. HTTP and Git activities cannot leave files for the next
   WorkItem via a volume.
4. **Workspace snapshot format.** Tar blob vs key prefix per file;
   generation id on the WorkItem vs implicit "latest". Lean: one
   generation per exclusive `rw` exit; `ro` / `copy` pin that
   generation.
5. **When the snapshot PUT runs.** A cross-target successor must wait
   for `available`. Same-target volume successors do not. Whether the
   pack is Worker Manager after exit or a dedicated activity is
   open.
6. **ANSTRAT-2422.** Default `/workspace` needs to land in the step
   SDK / Extension images if this contract is accepted.

Git extras (submodules, LFS, sparse checkout, clone depth) and Git
credentials (HTTPS token vs SSH key) belong to the Git activity
Extension, not this contract.

## Coordination

- **[example 02](examples/02-data-sharing-with-workspace.md):** three WorkItems on
  one volume workspace at `/workspace`. The first places via
  selectors. Reuse pins B and C to that ExecutionTarget; a selector
  miss is `NO_MATCHING_TARGETS` and the error explains the pin. The
  tree remains after each unmount.
- **[example 03](examples/03-data-sharing-with-workspace-object-store.md):**
  the same three WorkItems as example 02, but the workspace is an
  object-store snapshot instead of a PVC. Each places via
  selectors. Reuse hydrates `/workspace` from S3 and does **not** pin
  the ExecutionTarget. The next `rw` WorkItem waits for the snapshot.
- **[labels.md](labels.md):** HTTP and Git activity params and
  workspace UUID are not labels. Reuse of a volume workspace shrinks
  the eligible set to the ExecutionTarget that holds the volume;
  that target must still satisfy the WorkItem's selectors.
- **[Worker Manager](worker-manager.md):** looks up the workspace id,
  mounts its volume at `/workspace` (one `rw` mount per id; `ro` /
  `copy` as above). It does not GET object storage or clone Git as
  WorkItem inputs.
- **AAP-92722 (Work Scheduler):** after claim, if the workspace UUID
  already has a volume, that ExecutionTarget is the only candidate.
  Fail with `NO_MATCHING_TARGETS` if the WorkItem's selectors do not
  match it, and explain that the volume pin is the constraint. Do not
  dispatch a
  second **`rw`** WorkItem for a workspace id whose volume is still
  mounted. Snapshot `ro` / `copy` may overlap after that generation
  is `available`.
- **Workspace API:** AO mints the UUID and `POST`s it to EP (default:
  one per workflow) **before** any WorkItem. Duplicate id is refused.
  A WorkItem that cites an unknown id fails. EP provisions the volume
  on the ExecutionTarget chosen for the first WorkItem that cites that
  id, after reconcile and before dispatch. AO does not pass a target
  on `POST`. **MVP lifetime:** AO `POST`s then `DELETE`s by id (for
  example when the execution finishes). **Also:** optional TTL, set by
  the user through AO (ExecutionTarget default if omitted). Clock is
  last unmount. Size comes from the ExecutionTarget.
- **[Work Store](work-store.md):** `WorkItem.result` stays small JSON.
  Large files stay in the workspace.
- **[file-storage.md](https://github.com/syntara-orchestration/syntara/blob/devel/backend/docs/file-storage.md):** AO S3 for uploads. AO
  turns file ids into HTTP(S) URLs for the HTTP activity. EP does
  not import `FileManager`.
- **AAP-92720 (Work Executor):** persist `payload.data`; reject a
  WorkItem whose workspace id was not created via `POST`.
- **HTTP activity (AO Extension):** writes the GET body under
  `/workspace`. Params such as `url` and `dest` are the Extension's.
- **Git activity (AO Extension):** clones a pinned SHA under
  `/workspace`. Params such as `uri`, `ref`, and `dest` are the
  Extension's.
- **[ANSTRAT-2422](https://redhat.atlassian.net/browse/ANSTRAT-2422):**
  step types / Extension SDK. Default mount `/workspace` should be
  the documented convention for images that read or write workflow
  files.
- **Container SDK (AO / EP, separate design):** how the image reads
  `/workspace`.
