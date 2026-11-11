# Running AWX-style jobs on the cold-start execution plane

Status: **evaluation / future work.** Nothing here is implemented. This document
maps how AWX executes jobs on Kubernetes today, how that maps onto the cold-start
node-dispatch framework ([cold-start-node-dispatch.md](cold-start-node-dispatch.md)),
what slots in cleanly, and what has to change — followed by a frank assessment of
the hard parts (chiefly secrets).

It is written so each gap below can become its own Jira. It is **out of
ANSTRAT-1803 scope** — 1803 ships the single-node-type cold-start MVP; this is the
"what would it take to run an Ansible job this way" question that MVP invites.

Audience: someone who knows one side (AWX *or* this framework) and needs the other.

---

## Overview

Running Ansible jobs on this framework is a **good fit, with one genuinely hard
problem and one sizeable one.**

- **Good fit.** AWX's Kubernetes "container group" already does a cold start of a
  single pod per job and reaps it — the same lifecycle this framework is built
  around. The execution-environment (EE) image maps directly onto our per-node
  container image. Even the *data path* is closer than it looks: AWX container
  groups reach the pod **through the kube-apiserver** (receptor's k8s work plugin
  attaches to the pod's stdio), not over the receptor mesh — which is exactly what
  our port-forward does today.
- **Must change (sizeable).** The transport contract. AWX ships the whole job as
  an **ansible-runner `transmit`/`worker`/`process` stream over stdin/stdout** (a
  zipped `private_data_dir` + newline-delimited JSON frames). This framework talks
  **gRPC** (the node protocol, request + streamed progress). The stdin/stdout
  streaming contract goes; ansible-runner's *local* run engine stays, wrapped by an
  in-pod gRPC node runtime.
- **The hard problem.** Secrets. AWX writes credentials to **plaintext files on
  disk** (`env/passwords`, `env/extravars`, `env/envvars`, `env/ssh_key`, mode
  0600) inside the job's `private_data_dir`, which lands on the worker pod's
  filesystem. Our stated goal is to **not write secrets to disk at all**, and
  ideally to avoid bare-text in transit — which AWX does *not* do either. This is
  solvable but non-trivial, and it is the heart of the integration design.

The rest of this doc justifies that summary.

---

## 1. How AWX runs a Kubernetes job today

Sources: `~/repos/awx/awx/main/tasks/receptor.py`, `.../tasks/jobs.py`,
`.../utils/execution_environments.py`, `.../scheduler/kubernetes.py`;
`~/repos/ansible-runner/src/ansible_runner/{streaming.py,__main__.py,interface.py}`.

### 1.1 The pod

A container group is an `InstanceGroup` with `is_container_group`
(`models/ha.py`). On launch, `AWXReceptorJob.pod_definition`
(`tasks/receptor.py`) starts from a default pod spec
(`utils/execution_environments.py:get_default_pod_spec`), deep-merges the
instance group's `pod_spec_override`, and forces:

- `image` = the job's effective **execution environment** (EE),
- `args = ['ansible-runner', 'worker', '--private-data-dir=/runner']`,
- `automountServiceAccountToken: False`, modest CPU/mem requests, pod anti-affinity.

So the pod's entire job is to run **`ansible-runner worker`** — a streaming server
that reads a job on stdin and writes results on stdout.

### 1.2 The dispatch (receptor)

AWX does **not** create the pod via the k8s API itself. It submits a **receptor
work unit** (`receptor_ctl.submit_work`, `tasks/receptor.py`) of worktype
`kubernetes-runtime-auth` (or `kubernetes-incluster-auth`), passing the rendered
pod YAML as `secret_kube_pod` and, when the instance group has a cluster
credential, a kubeconfig as `secret_kube_config`. **Receptor's `work-kubernetes`
plugin** creates the pod, attaches to its stdio through the kube-apiserver, and
exposes that as the work unit's socket.

Two receptor layers, and the distinction matters for us:

- **netceptor** — the mesh overlay (node-to-node TLS links). Used to reach a
  *remote execution node* (worktype `ansible-runner`, with `node=<exec node>`).
- **workceptor** — work units (`submit_work`, `work status`, `work results`,
  `work release`, `work cancel`).

For **container groups the `node` kwarg is not set** — the work runs on the
*local* controller receptor, which reaches the pod via the kube-apiserver. The
mesh is **not** in the pod's data path for container groups. The mesh only enters
when the target cluster's API is itself across a network boundary (a remote
receptor node fronts it).

### 1.3 The in-pod process and the wire protocol

`ansible-runner`'s remote model has three stages
(`src/ansible_runner/streaming.py`):

- **transmit** (controller): writes one JSON line `{"kwargs": ...}`, then a
  `{"zipfile": N}` header line followed by `N` bytes of base64 ZIP of the whole
  `private_data_dir`, then `{"eof": true}`.
- **worker** (pod): reads that off stdin, unzips to `/runner`, runs the job
  locally, and streams results back on stdout as newline-delimited JSON frames —
  `status`, job `event`s, an artifacts `zipfile`, periodic `keepalive`, `eof`.
- **process** (controller): reassembles artifacts + per-event JSON files on disk.

AWX runs `transmit` and `process` in threads on the controller
(`tasks/receptor.py`), bridged to the pod through the receptor work-unit socket.

### 1.4 Secrets, and where they live

`BaseTask.run` (`tasks/jobs.py`) assembles the `private_data_dir` on the
**controller's disk**, then it is zipped into the transmit stream and **extracted
onto the pod's disk** by the worker. The secret-bearing files
(`ansible_runner.utils.dump_artifacts`, written mode 0600):

- `env/passwords` — become/vault/SSH prompt responses,
- `env/extravars` — extra vars, frequently credential values,
- `env/envvars` — env, may carry tokens,
- `env/ssh_key` — SSH private key material.

Credential plugins (`inject_credential`, incl. external secret managers) resolve
values into these files / env on the controller first. In-pod file paths are
rewritten to `/runner/...`.

Two nuances worth keeping:

- **`ssh_key` is special.** At run time ansible-runner streams the key through a
  **named pipe (FIFO) to ssh-agent** (`open_fifo_write`), not a regular file — so
  the *live* key need not persist as a plain file (though `env/ssh_key` from the
  zip may still exist on disk).
- **`suppress_env_files=True`** makes `dump_artifacts` keep `envvars`/`extravars`/
  `passwords`/`settings` **in the kwargs JSON stream** instead of writing env
  files — i.e. they travel in-memory in the frame rather than as files. AWX
  defaults `AWX_RUNNER_OMIT_ENV_FILES=True` for the *artifacts* dir, but the job's
  working `private_data_dir` on the pod is still populated by the worker on unzip.

Net: **secrets are plaintext on the pod's filesystem** for the duration of the
run, and **plaintext within the transmit stream** (confidentiality rests on the
receptor link TLS, not on encrypting the payload).

---

## 2. How this framework runs a job today

(Full detail in [cold-start-node-dispatch.md](cold-start-node-dispatch.md).)

- The **AO** (Temporal activity `ep_dispatch_activity`) selects a container image
  per node type and builds an invocation using the
  [SDK workload protocol](workload-protocol.md), persisted to `work_items.payload`.
- The **EP worker** (`VanillaK8sWorkerManager` → `transport.run_job`) creates one
  hardened, single-attempt Job around the workload Pod (`job_body`), waits for
  its owned Pod to become ready, and makes the SDK gRPC calls over a
  kube-apiserver **port-forward** bridged to a loopback socket. The Pod gets no
  credential Secret or certificate mount.
- The node runs, returns one `Result`, progress events stream back over gRPC, and
  the pod is reaped.
- EP does not inject workload credential files. AWX credential delivery
  requires a separate design.

The shapes line up: cold-start pod, per-node image, proxy-through-apiserver data
path, a progress stream, and a cancellation hook.

---

## 3. Mapping AWX → this framework

| AWX concept | This framework | Verdict |
|---|---|---|
| Container-group pod (one per job, reaped) | Cold-start pod per `WorkItem`, reaped | **Same model.** |
| Execution Environment (EE) image | `node_container_images[<type>]` image | **Direct map** — an `ansible-job` node type whose image is an EE. |
| `ansible-runner worker` (stdin/stdout stream) in the pod | gRPC node server in the pod | **Replace** the in-pod process. Keep ansible-runner's *local* run engine; drop worker/transmit/process. |
| transmit zipstream (kwargs + zipped `private_data_dir`) | SDK invocation inputs | **Replace the transport.** Envelope must grow to carry playbook/inventory/extravars/limit. Project delivery is a gap (§4.3). |
| stdout JSON frames: `event`/`status`/`artifacts`/`eof` | gRPC `Result` + streamed `progress` events | **Map** events→progress, final status→Result. Needs volume/throughput attention (§4.4). |
| `work cancel <unit_id>` | SDK `Cancel` RPC | **Map** cooperative cancellation to the runner. |
| receptor `work-kubernetes` → apiserver attach | kube-apiserver port-forward | **Equivalent today**; both proxy through the API server. |
| netceptor mesh (reach a cluster with no direct route) | *(not built)* — proposed mesh transport | **Gap for multi-cluster** (§4.5). Matches our out-of-1803 receptor transport. |
| Secrets as plaintext files in `private_data_dir` on the pod | Workload credential delivery is not implemented | **Deliberately different** — the hard part (§4.1). |

---

## 4. What has to change (each a candidate Jira)

### 4.1 Secrets without touching disk — the central problem

Two independent credential-protection goals:

**Goal A (minimum): secrets never hit the pod's real filesystem.**
ansible-runner fundamentally wants a `private_data_dir` on disk, and
`RunnerConfig` reads `env/envvars`, `env/extravars`, `env/passwords`,
`env/settings` back from files. So "don't write to disk" cannot mean "don't give
ansible-runner files"; it must mean **put those files on a memory-backed
filesystem that never pages out.** This framework already mounts a `tmpfs`
(`emptyDir{medium: Memory}`) and runs with `readOnlyRootFilesystem: true`
(`pod_body`). The design:

1. Define credential resolution, lifetime, and delivery under ANSTRAT-2422.
2. Keep credential values out of ordinary inputs and Job/Pod specifications.
3. The **in-pod node runtime** receives them in memory and writes the
   `private_data_dir` (env files, inventory, extravars) **onto the memory-backed
   volume**, points `ansible-runner` at it, runs locally, then the pod dies and
   the tmpfs evaporates. Enlarge the memory `emptyDir` beyond today's 64Mi for
   real jobs, and keep `medium: Memory` so nothing is persisted.
4. **SSH keys**: reuse ansible-runner's existing FIFO→ssh-agent path
   (`open_fifo_write`) — it already avoids a persistent key file at run time.

This gets us to parity-plus: secrets live only in pod RAM for the job's lifetime,
versus AWX's 0600 files on the pod filesystem.

**Goal B (stretch): no bare-text in transit — which AWX does not do.**
AWX's transmit zipstream is plaintext; confidentiality is the receptor link TLS.
For protection beyond transport TLS, encrypt the
credential payload itself (envelope encryption with a key the node
runtime unwraps in memory) so secrets are ciphertext at the DB, on the wire, and
in transport logs — decrypted only inside the pod. This is optional and layered on
top of Goal A; call it out but don't gate the integration on it.

### 4.2 An `ansible-job` node type and an in-pod runtime

Define a node type (model + `ScriptExecutorParameters`-equivalent) carrying:
`playbook`, `inventory`, `extra_vars`, `limit`, `job_tags`/`skip_tags`, `forks`,
`verbosity`, `credentials` (references). The in-pod runtime is a gRPC node server
that, on `Execute`, assembles an in-memory `private_data_dir` and calls
ansible-runner's **local** `interface.run(...)` (not `streamer='worker'`),
forwarding job events as gRPC progress and the final status/rc/artifacts as the
`Result`. The `transmit`/`worker`/`process` stdin/stdout contract is **not used**.

### 4.3 Project / playbook delivery

AWX ships the entire `project/` (playbook, roles) inside the transmit zip. We
deliver an image and a bounded invocation, rather than a project archive. Options, in rough order of preference:

- **SCM fetch in-pod**: the runtime clones/pulls the project at run time (needs
  egress + an SCM credential — itself a secret via §4.1). Closest to AWX "project
  updates".
- **Project baked into the EE image** (immutable, simplest, least flexible).
- **A dedicated bulk channel** for the project tree (a second gRPC stream or an
  object-store handle), keeping the invocation envelope small.

Pick deliberately; this is a real divergence from "one small envelope".

### 4.4 Job events: one buffer, at the node — streamed straight to gRPC

The design to pursue (and the one the team proposed): rather than run
`ansible-runner worker` and parse its stdout stream, run ansible-runner **locally
in-pod** and register a **custom `event_handler`** (plus `status_handler`,
`artifacts_handler`, `finished_callback`, `cancel_callback` via
`ansible_runner.interface.run(...)`) that pushes each event **straight onto the
gRPC progress stream**. This is sound and is essentially what the `worker` streamer
already is — a built-in `event_handler` that writes JSON to stdout; we substitute
one that encodes a `Progress` event. Findings that shape it:

**Yes, this avoids the on-disk event *artifacts* — same trick the worker uses.**
`Runner` only persists the final `job_events/<counter>-<uuid>.json` file when the
`event_handler` returns truthy (`runner.py` `should_write`). The `worker` streamer's
handler returns `None`, so those files are never written. A custom gRPC handler
that **returns `False`** gets the identical behavior — no final event files.

**But you cannot make it fully disk-free without forking the callback plugin.**
The `awx_display` ansible callback writes a **transient `<uuid>-partial.json`** to
disk *inside the ansible process* for every event (it's the sole transport for the
full event dict); `Runner.event_callback` then reads and immediately deletes it
(`remove_partials=True`). So event data — which can include sensitive task results
unless `no_log` — touches disk briefly. The mitigation is the **same memory-backed
volume from §4.1**: point the `private_data_dir`/artifact dir at the tmpfs so the
partials live in RAM, never on real disk. Eliminating the partial entirely means
modifying `awx_display`'s `IsolatedFileWrite` — a bigger lift, probably not worth it
if the tmpfs answer holds.

**The design law: exactly one buffer, and it lives where the work runs.** We only
want a buffer in the **remote execution location** (the pod). The EP side is
**pass-through, not a staging area** — when the EP worker reads a line of output it
goes **straight to Postgres**, and since the EP consumer is already asyncio it is a
thin async pump, not a second buffer. The one genuinely intractable buffering need
is the **network hop** (gRPC over the apiserver port-forward today; the receptor
mesh for remote clusters, §4.5), so the buffer belongs at the **source** of that
hop, not the destination. If the EP workers fall behind (Postgres slow, EP
overloaded), we do **not** grow a control-side buffer — backpressure flows back
across the hop and accumulates in the **same** node-side buffer, which spills to its
disk tier and only throttles ansible as a last resort.

**Scorecard vs AWX.** AWX has **three** buffers: the execution-node buffer
(workceptor writes the whole stdout stream to disk), a control-node buffer, and the
Redis queue between the callback receiver and the database. This design **keeps 1 of
the 3 and deletes the other 2** — no control-node buffer, no Redis — and the one it
keeps is *better* than workceptor's: it holds the common case in RAM and spills only
the overflow *tail* to disk, instead of writing the entire stream to disk.

The proposed buffer has two bounded tiers:

- a **byte-bounded** in-memory tier; when it fills, append
  new events to a **watermarked append-only spill file** and refill the head of the
  mem tier as the consumer drains (hysteresis, strict FIFO — `job_events` ordering
  must hold);
- `.put()` then succeeds by writing to disk instead of raising `Full` until the
  **disk tier is also full** — only then does it block (backpressure to ansible),
  the correct last resort versus OOM or dropping events.

**Pod-spec consequence (a real infra item).** The spill tier needs a **writable
disk-backed `emptyDir`** mounted for `spill_dir`. Today's pod is read-only root plus
a *memory*-backed `/tmp` tmpfs — memory tmpfs is still RAM, so spilling there
defeats the purpose. Add a dedicated disk-backed volume (`sizeLimit` = `disk_bytes`),
separate from the secrets tmpfs (§4.1), in the workload manifest and cold-start pod specification; keep root FS read-only.

With the buffer settled, the remaining ceilings are the real engineering:

1. **Bound each progress event.** Validate event size against the SDK message
   limits. Split, truncate, or store oversized output separately; a bounded
   queue alone does not bound the size of an individual event.

2. **Don't put aggregate output in the final `Result`.** The terminal `Result` is a
   **single** gRPC message. GB of output
   cannot live there. Model ansible output as the **`Progress` event stream**; the
   `Result` carries only terminal status / rc / stats (small). This is the natural
   shape and sidesteps the single-message limit for the bulk.

3. **Persist job events without a second buffer.** The proposed Ansible-job
   consumer is an **async pump → Postgres**: read event → write a
   `JobEvent`-style row, with **no** control-side buffer of its own (the design law
   above). Because it is the only consumer on the hop, if Postgres slows the pump
   slows, the HTTP/2 window fills, and the node `SpillQueue` absorbs it (mem → disk)
   — "buffers to the same location." A control-side bounded queue is explicitly
   **not** wanted here; that is exactly the AWX Redis buffer we are deleting.

4. **Pod resources.** Today's pod is `512Mi` memory with a `64Mi` memory `/tmp`
   (`pod_body`). Real ansible plus a tmpfs `private_data_dir` (project + transient
   partials) needs materially more — raise both, and keep the volume `medium:
   Memory` so nothing persists. (Overlaps §4.1 and the resource-limits gap.)

**Net:** one buffer, at the node — in-memory with disk overflow; the EP side streams straight to
Postgres with no buffer of its own; and under overload backpressure flows back to
that single node buffer. This is strictly leaner than AWX (3 buffers → 1) and beats
workceptor by spilling only the overflow tail. The remaining engineering is (1)
keeping every event under the SDK message limit (or adding chunking), (2) streaming
bulk output as events rather than in the `Result`, (3) the async pass-through
consumer, and (4) bigger pod limits plus the disk-backed spill volume. None of this
is the "it all buffers in RAM" failure the GB-output worry implies.

Separately, long Ansible runs stress serial-dispatch,
fixed-timeout, and single-port-forward-per-call assumptions — size those for
minutes-to-hours, not seconds.

### 4.5 Multi-cluster reach (the mesh)

For a single reachable cluster, the port-forward is enough — **the same situation as an AWX
container group**, which also reaches its pod through the apiserver. To reach
clusters the control plane has **no route to** (AWX's remote-node / mesh story),
a **receptor/mesh transport** is an option.
Reuse **netceptor** (keep the mesh overlay); **replace workceptor** (our
`WorkItem` + worker-manager is the work-unit layer). This is where the AWX
precedent and our production transport converge.

### 4.6 Cancellation, timeouts, artifacts/fact-cache

- Connect SDK cancellation to the Ansible runner; AWX relies on `work cancel`.
- Per-job timeout vs. the invocation's `timeout_seconds` and the pod's
  `activeDeadlineSeconds` — Ansible jobs need a much larger, configurable budget.
- Decide whether set_stats/fact-cache **artifacts** (AWX streams them back and
  persists them) are in scope; if so, the `Result`/progress contract must carry
  structured artifacts, not just stdout.

### 4.7 Handing job-event output back to the AWX controller

AWX persists job events in the **AWX database** and renders them from there. In this
framework the **EP worker** owns the output stream and has no access to the AWX DB,
so the EP→AWX handoff is unresolved. Two options, to be discussed (a separate future
work item, like §4.4's spill-buffer story):

1. **Output lives in the EP database** — fully moved out of the AWX DB (or a thin
   proxy shims it into the AWX DB for temporary compatibility). Single source of
   truth on the EP side; *the easiest answer and the likely one*, but it changes
   where AWX reads events and needs sign-off.
2. **EP hands the host & port to AWX**, and AWX runs its own output processing
   against the node's gRPC stream. Keeps AWX's DB authoritative but duplicates the
   consumer and re-couples AWX to the node transport.

Lean toward (1); flag (2) so the tradeoff is explicit. Either way this is the EP↔AWX
seam, not an in-pod concern.

---

## 5. Evaluation

**Is this a sane way to run Ansible jobs? Yes — arguably cleaner than today.**

The instinct that "AWX uses a mesh, we use a port-forward, so we're far apart" is
wrong for the case that matters: an AWX **container group** reaches its pod the
same way we do — through the kube-apiserver, not the mesh. The mesh is an
orthogonal concern (reaching unreachable clusters) for multi-cluster deployments. So the architectures rhyme.

The real substitution is **transport, not topology**: replace ansible-runner's
stdin/stdout `transmit`/`worker`/`process` streaming with the gRPC node protocol,
while keeping ansible-runner's local run engine inside the pod. That's a
well-contained swap — the in-pod runtime wraps `ansible_runner.interface.run`
locally and translates its callbacks to gRPC progress. Dropping the three-stage
streamer removes a whole class of framing/keepalive/zipstream complexity.

**The genuinely hard part is secrets, and it's worth doing right because we can
actually beat AWX here.** AWX writes credentials as 0600 plaintext files onto the
pod and ships them plaintext in the zipstream. We already have the ingredients to
do better: a memory-backed volume and a read-only-root pod. The achievable target is **secrets only
ever in pod RAM, never on disk**, and with payload encryption, **ciphertext
everywhere except inside the pod**. The constraint is that
ansible-runner *wants files*, so "no disk" means "tmpfs in RAM", not "no files" —
and credential protection is a
hard prerequisite, not an afterthought. SSH keys are the one place AWX already
does the right thing (FIFO→ssh-agent); reuse it.

**Biggest non-secret risk: the execution *shape*, not the plumbing.** Script nodes
are short, single-result, small-payload. Ansible jobs are long, high-event-volume,
large-project. The assumptions that are fine for the MVP — serial dispatch, one
port-forward per call, result-at-completion, a small invocation envelope, tight
timeouts — all get stressed. Project delivery (§4.3) and incremental event
persistence (§4.4) are where I'd expect the most redesign, more than the transport
swap itself.

**Suggested sequencing** (each its own story, none in 1803):

1. Define the credential contract in ANSTRAT-2422, including whether an
   extension receives credential values over gRPC or through a documented
   runtime mount. EP payload-at-rest encryption is implemented already.
2. `ansible-job` node type + in-pod runtime wrapping local ansible-runner (§4.2),
   secrets materialized on tmpfs through the agreed credential contract (§4.1 Goal A).
3. Project delivery (§4.3), the node-side spill buffer + event streaming (§4.4),
   and the EP↔AWX output handoff (§4.7).
4. Evaluate direct pod mTLS and receptor/mesh transport for
   multi-cluster (§4.5), with optional payload encryption (§4.1 Goal B).

---

## References

- This framework: [cold-start-node-dispatch.md](cold-start-node-dispatch.md),
  [worker-manager.md](worker-manager.md), [integration.md](integration.md),
  [kind-demo-runbook.md](kind-demo-runbook.md).
- AWX (`~/repos/awx/`): `awx/main/tasks/receptor.py` (AWXReceptorJob, pod
  definition, work types, transmit/process), `awx/main/tasks/jobs.py`
  (private_data_dir, credential injection), `awx/main/utils/execution_environments.py`
  (default pod spec), `awx/main/scheduler/kubernetes.py` (PodManager),
  `awx/main/models/ha.py` (container group).
- ansible-runner (`~/repos/ansible-runner/`): `src/ansible_runner/streaming.py`
  (Transmitter/Worker/Processor), `src/ansible_runner/__main__.py` (CLI stages),
  `src/ansible_runner/interface.py` (run dispatch), `src/ansible_runner/utils/`
  (`dump_artifacts`, `stream_dir`/`unstream_dir`, `open_fifo_write`),
  `src/ansible_runner/config/_base.py` (passwords/ssh_key/env handling).
