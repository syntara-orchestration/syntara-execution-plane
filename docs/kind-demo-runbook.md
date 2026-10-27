# Combined AO + EP Kind demo: setup record

**Status:** the script happy path was exercised against the current AO and EP
migration branches on 6 October 2026. AO and EP used separate databases on one
PostgreSQL server; AO submitted through EP's API, and EP dispatched a
Kubernetes Job whose gRPC result reached AO through the authenticated callback.
The local Kind cluster used kindnet, so this run does not validate
NetworkPolicy enforcement. Record a fresh AO/EP commit SHA and node image digest
when repeating the test against updated PR heads.

The test target is one script workflow submitted through AO, accepted by the
separate EP API, run by the EP worker as a cold-start Job, returned over the
SDK gRPC protocol, committed to the EP completion notification state, and delivered to AO
for Temporal completion. AO and EP use distinct databases on the same local
PostgreSQL server.

## Required services and configuration

- AO API, Temporal, AO workers, and PostgreSQL from the Syntara checkout.
- EP API, EP worker, and Alembic migration from the standalone
  `syntara-execution-plane` checkout. Build an EP image from that checkout and
  set `EP_IMAGE` for the AO compose stack.
- A separate `execution_plane` database and EP runtime/migration roles. The AO
  compose bootstrap creates these; apply EP Alembic migrations with the same
  EP payload-encryption key that the API and worker use.
- A local Kind cluster with a CNI that enforces NetworkPolicy. The EP worker
  container must reach the Kubernetes API server and use an AO-managed target
  credential scoped to the workload namespace.
- Valid TLS for the EP API, AO completion callback, AO credential-resolution
  callback, plus the verified Kubernetes API CA for the target. Do not disable
  certificate validation to work around a missing or hostname-mismatched CA.
- A script node image pinned by digest and available to the Kind node.

## Flow to exercise

1. Start PostgreSQL, create the separate EP database/roles, run AO migrations,
   and run EP migrations independently.
2. Start the EP API/worker and AO/Temporal services. Configure AO's EP URL,
   AO JWT issuer/key, EP completion and credential-resolution callback URLs,
   and callback mTLS identity.
3. Create an OpenShift integration through AO's integration API or UI, using a
   ServiceAccount credential scoped to the Kind workload namespace and its
   verified CA. Wait for the integration sync status to report `ready`; do not
   write EP tables directly from AO or the demo harness.
4. Submit a script workflow through AO. Confirm one AO request ID maps to one
   EP WorkItem and one claim-generation Job. Confirm the result fields,
   including stdout/stderr, arrived through gRPC and the AO completion notification completed
   the Temporal activity.
5. Inspect the WorkItem and completion notification state in the EP database, AO's command
   inbox/binding in the AO database, and the worker Job/NetworkPolicy lifecycle.
   Verify both databases remain inaccessible to the other service runtime role.
6. Repeat with completion notification outage, AO/EP restart, cancellation, startup failure,
   and an ambiguous Execute result. An uncertain attempt must remain visible as
   `reconciliation_required` and must not get a second Job automatically.

## Smoke test run record

The happy-path test was run from the AO `backend` directory with the local AO
and EP smoke stack running:

```bash
APP_BASE_URL=https://localhost:18000 \
APP_ADMIN_PASSWORD_PATH=.secrets/admin-password \
APP_SCRIPT_NODES_ENABLED=true \
uv run --no-sync pytest \
  tests/e2e/workflows/test_script_node_gate.py::TestScriptNodeGateEnabled::test_script_activity_executes_normally \
  -q --tb=short -o log_cli=false
```

Expected evidence is a completed AO execution and script activity, an EP
WorkItem with `completed` status and exit code `0`, the returned `stdout` and
`stderr` in the persisted result, an acknowledged completion notification, and completed
resource cleanup. The 6 October run returned `stdout = "gate test\n"`, empty
`stderr`, exit code `0`, and cleanup status `complete`; AO accepted the completion notification
with HTTP `202`. This test covers the service-to-service happy path, not
restart/retry recovery or network-policy enforcement.

The EP persistence regression tests can be run against an isolated EP database
by setting `EP_TEST_DATABASE_URL` to a migration-capable test role and
`EP_CREDENTIAL_ENCRYPTION_KEY_PATH` to the same key used by the EP API and
worker, then running:

```bash
uv run pytest tests/integration/test_postgres_persistence_types.py -q
```

### HTTP executor smoke test

On 6 October 2026, the HTTP executor image was submitted to the standalone EP API
and dispatched by the EP worker as a cold-start Kubernetes Job. The workload used
the SDK gRPC protocol to call a controlled HTTP service in its execution namespace.
EP returned HTTP status `200` and the expected response marker, persisted the
WorkItem as `completed`, and reported resource cleanup as `complete`.

The run used EP commit `88af810` (`sha256:1c8f61aef4c0350b2493ca2183bd1dc60d1e5176086ba16438001f8792b54bc7`)
and the HTTP executor built from `syntara-step-types` commit `0909750`
(`sha256:7d1bbac751931974df19ec337008cbb9f1d830fde0d8e082a196862ef4b67aea`).
The target was a uniquely named namespace in K3d Kubernetes `v1.31.5+k3s1`
(`linux/arm64`), using Flannel. The test created and then removed the namespace;
the EP database and Compose project were isolated and removed afterward.

This was a direct EP WorkItem submission using the first-release API contract,
which currently accepts `workload_type: "script"`; the submitted image was the
HTTP executor. It validates EP's generic Job/gRPC execution path for that image.
It did not run AO's native `http_request` Temporal activity, exercise AO's
workflow-to-EP dispatch or completion notification, or test another EP workload type.
The pod's NetworkPolicy was created with the Job, but Flannel does not enforce
NetworkPolicies, so this run is not evidence for either allowed-versus-blocked
egress enforcement or the deny-egress case. Repeat those checks on Calico or an
enforcing OpenShift CNI.

## NetworkPolicy enforcement status

The smoke Kind cluster currently uses kindnet. It can confirm the Job and
NetworkPolicy lifecycle, but kindnet does not enforce NetworkPolicies, so a
successful or failed connection there cannot establish that an egress rule is
effective. A separate Calico Kind attempt on the current workstation could not
start because containerd exhausted available inotify watchers. Repeat the
allowed and denied destination checks with a working Calico Kind cluster or the
target OpenShift CNI. Also verify the EP service-account Role can `get`
`pods/portforward` while it cannot read `pods/log` or create `pods/exec`.

The current feature-branch Konflux scripts predate the service split: they run
Alembic from an AO backend container, write target rows directly to EP tables,
and configure EP to connect directly to Temporal. They are not valid for this
topology. The migration PRs must replace that harness before it is enabled as a
combined-service test. The correct evidence requires exact AO/EP image SHAs,
node image digest, Kind/Kubernetes version, CNI, and scenario results.
