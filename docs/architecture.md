# Execution Plane architecture

This document describes the current Execution Plane architecture as a set of
logical responsibilities, protocols, and end-to-end flows. It is intentionally
implementation-neutral: component names refer to architectural roles rather than
source files or classes.

## Source initiatives

- [ANSTRAT-1803](https://redhat.atlassian.net/browse/ANSTRAT-1803) defines the Execution Plane as a scalable, secure execution tier for Automation Orchestrator workloads. It focuses on isolated container execution, independently managed targets, and safe control-plane-to-execution-plane communication.
- [ANSTRAT-2422](https://redhat.atlassian.net/browse/ANSTRAT-2422) defines the Plugin SDK and the shared contracts used by workflow step authors. It establishes versioned step definitions, manifest rules, execution placement boundaries, and the gRPC contract consumed by the Execution Plane.
- [ANSTRAT-2523](https://redhat.atlassian.net/browse/ANSTRAT-2523) containerizes existing workflow step types so they can run through the Execution Plane using the Plugin SDK contracts. It preserves existing workflow semantics while moving step execution into isolated, versioned containers.

## Current scope

- The Automation Orchestrator (AO) is the only configured caller.
- The Execution Plane (EP) executes container workloads on Kubernetes.
- OpenShell is represented as a future backend, but the current dispatch path is
  Kubernetes-only.
- Workload credentials are not injected as files in the current release.

## System context

```mermaid
flowchart TB
    Requester([Workflow requester])

    subgraph AO[Automation Orchestrator]
        UserAuth[User authorization]
        Workflow[Workflow state]
        Integration[Integration configuration]
        CredentialSource[Credential resolution]
        CompletionSink[Completion sink]
    end

    subgraph EP[Execution Plane]
        CommandIngress[Command ingress]
        Scheduler[Work scheduler]
        Placement[Target placement]
        WorkerManager[Worker manager]
        CredentialResolver[Credential resolver]
        CompletionNotifier[Completion notifier]
        ResourceMonitor[Resource monitor]
    end

    Database[(PostgreSQL)]

    subgraph TargetCluster[Target cluster]
        KubernetesAPI[Kubernetes API]
        NetworkPolicy[Per-attempt NetworkPolicy]
        WorkloadJob[Isolated workload Job]
        PluginWorkload[Plugin workload]
    end

    Requester --> UserAuth
    UserAuth --> Workflow
    Workflow --> CommandIngress
    CommandIngress --> Scheduler
    Scheduler --> Placement
    Placement --> WorkerManager
    WorkerManager --> CredentialResolver
    WorkerManager --> KubernetesAPI
    WorkerManager --> PluginWorkload
    CredentialResolver --> CredentialSource
    WorkerManager --> CompletionNotifier
    CompletionNotifier --> CompletionSink
    ResourceMonitor --> CredentialResolver
    ResourceMonitor --> KubernetesAPI
    KubernetesAPI --> NetworkPolicy
    KubernetesAPI --> WorkloadJob
    WorkloadJob --> PluginWorkload
    EP --> Database
```

## Execution Plane component topology

```mermaid
flowchart TB
    AO[Automation Orchestrator]
    DB[(PostgreSQL)]
    Kube[Kubernetes API]

    subgraph EP[Execution Plane]
        Ingress[Command ingress]
        Scheduler[Work scheduler]
        Placement[Placement resolver]
        WorkerManager[Worker manager]
        CredentialResolver[Credential resolver]
        ResourceMonitor[Resource monitor]
        DrainMonitor[Drain monitor]
        PolicyReconciler[Orphan policy reconciler]
        CompletionNotifier[Completion notifier]
    end

    AO -- "HTTPS + service JWT" --> Ingress
    Ingress -- "SQL / TLS" --> DB
    DB -- "SQL notify / poll" --> Scheduler
    Scheduler -- "in-process query" --> Placement
    Placement -- "in-process selection" --> WorkerManager
    WorkerManager -- "in-process reference" --> CredentialResolver
    CredentialResolver -- "HTTPS + deployment-configured mTLS" --> AO
    WorkerManager -- "HTTPS + bearer token" --> Kube
    WorkerManager -- "in-process handoff" --> CompletionNotifier
    CompletionNotifier -- "HTTPS + deployment-configured mTLS" --> AO
    ResourceMonitor -- "in-process reference" --> CredentialResolver
    ResourceMonitor -- "HTTPS + bearer token" --> Kube
    ResourceMonitor -- "SQL / TLS" --> DB
    DrainMonitor -- "SQL / TLS" --> DB
    PolicyReconciler -- "in-process reference" --> CredentialResolver
    PolicyReconciler -- "HTTPS + bearer token" --> Kube
```

## Responsibility boundaries

| Participant | Owns |
| --- | --- |
| AO | User authorization, workflow orchestration, user-facing workflow state, integration configuration, credential resolution, and the command that requests work. |
| EP | Durable work state, target and cluster bindings, placement decisions, execution attempts, result persistence, completion delivery, and execution diagnostics. |
| PostgreSQL | Durable state for work, targets, clusters, attempts, and inline completion-delivery state on work items. |
| Kubernetes | Workload scheduling, pod lifecycle, namespace isolation, network policy enforcement, and API-mediated access to the workload. |
| Plugin workload | The workload ABI, execution semantics, progress and result events, and cooperative cancellation. |
| Plugin SDK | The versioned gRPC contract and generated bindings shared by EP and workload images. |

## Protocol and trust boundaries

```mermaid
flowchart LR
    Requester([Requester])
    AO[Automation Orchestrator]
    EP[Execution Plane]
    DB[(PostgreSQL)]
    Kube[Kubernetes API]
    Workload[Plugin workload]

    Requester -- "user session / SSO" --> AO
    AO -- "HTTPS + service JWT<br/>command envelope" --> EP
    EP -- "HTTPS + deployment-configured mTLS<br/>credential.resolve / work.execute.completed" --> AO
    EP -- "TLS + database credentials<br/>encrypted work payloads" --> DB
    EP -- "HTTPS + bearer token<br/>Job, Pod, NetworkPolicy, port-forward" --> Kube
    EP -- "gRPC over Kubernetes API port-forward<br/>Health / Execute / Cancel" --> Workload
    Kube -- "Job and Pod lifecycle" --> Workload
```

Application-layer identity on AO-to-EP ingress is the service JWT. mTLS is a
transport-hardening layer that deployments may enable on top of that contract;
the deployment manifests used by this repository and the AO operator provide
the certificate material for it.

### Command families

| Direction | Command family | Purpose |
| --- | --- | --- |
| AO → EP | `work.execute` | Submit one container workload with a stable UUID. |
| AO → EP | `work.cancel` | Request cooperative cancellation of an existing work item. |
| AO → EP | `cluster.*` | Register, update, or drain a Cluster. |
| AO → EP | `execution_target.*` | Register, update, delete, or health-check an execution target. |
| EP → AO | `credential.resolve` | Resolve an opaque cluster or target credential reference. |
| EP → AO | `work.execute.completed` | Deliver a terminal or uncertain outcome from EP to AO. |

## End-to-end work execution

```mermaid
sequenceDiagram
    autonumber
    participant AO as Automation Orchestrator
    participant EP as Execution Plane
    participant DB as PostgreSQL
    participant Kube as Kubernetes API
    participant Workload as Plugin workload

    AO->>EP: work.execute command with stable UUID
    EP->>DB: persist pending work item with encrypted payload
    EP-->>AO: 202 accepted + resource reference
    EP->>EP: claim work and resolve eligible target
    EP->>AO: credential.resolve callback
    AO-->>EP: short-lived cluster credential
    EP->>Kube: install per-attempt NetworkPolicy
    EP->>Kube: create one-attempt workload Job
    Kube-->>EP: Job and worker Pod ready
    EP->>Kube: open API port-forward
    EP->>Workload: gRPC Health
    Workload-->>EP: ABI and advertised step revision
    EP->>Workload: gRPC Execute
    loop Until terminal result
        Workload-->>EP: progress events
    end
    Workload-->>EP: terminal result or failure
    EP->>DB: persist result before cleanup
    EP->>Kube: delete Job and NetworkPolicy
    EP->>AO: work.execute.completed callback
    AO-->>EP: transport receipt
    EP->>DB: mark completion delivered
```

## Target registration and health

```mermaid
sequenceDiagram
    autonumber
    participant AO as Automation Orchestrator
    participant EP as Execution Plane
    participant DB as PostgreSQL
    participant Kube as Kubernetes API

    AO->>EP: cluster.register
    EP->>DB: persist cluster desired state
    EP-->>AO: accepted resource reference
    AO->>EP: execution_target.register or execution_target.update
    EP->>DB: persist target desired state and placement
    EP-->>AO: accepted resource reference
    EP->>AO: credential.resolve callback
    AO-->>EP: short-lived cluster credential
    EP->>Kube: probe target namespace and API access
    Kube-->>EP: reachable or unreachable
    EP->>DB: persist active or degraded health
    EP-->>AO: execution_target.health result on demand
```

## Completion and uncertain outcomes

```mermaid
flowchart TB
    Start([Terminal result received]) --> Persist[Commit result and status]
    Persist --> Notification[Mark completion notification pending on WorkItem]
    Notification --> Send[Send callback to AO]
    Send --> Receipt{Valid receipt?}
    Receipt -- yes --> Delivered[Mark delivered]
    Receipt -- no --> Retry[Schedule bounded retry]
    Retry --> Send

    Ambiguous([Result lost after Execute may have started]) --> Fence[Stop claim lease and mark reconciliation required]
    Fence --> UnknownNotification[Mark reconciliation_required completion notification]
    UnknownNotification --> Investigate[Operator investigation and explicit new request]
    PreExecute([Failure before Execute submitted]) --> Requeue[Requeue with backoff]
```

## Cancellation

```mermaid
sequenceDiagram
    autonumber
    participant AO as Automation Orchestrator
    participant EP as Execution Plane
    participant Workload as Plugin workload
    participant Kube as Kubernetes API
    participant DB as PostgreSQL

    AO->>EP: work.cancel with work UUID
    EP->>DB: persist cancel-requested state
    EP->>Workload: gRPC Cancel
    alt Cooperative stop confirmed
        Workload-->>EP: terminal cancellation result
        EP->>DB: persist cancelled result
    else Grace period expires
        EP->>EP: close gRPC channel
        EP->>Kube: delete owned workload Job
        Kube-->>EP: workload stopped
        EP->>DB: persist cancelled result
    else Terminal result already received
        Workload-->>EP: final result wins
        EP->>DB: persist received terminal result
    end
    EP->>AO: work.execute.completed callback
```

## Cluster lifecycle

```mermaid
stateDiagram-v2
    [*] --> registering
    registering --> active : registration commits
    active --> draining : deletion requested
    draining --> active : reactivation requested
    draining --> error : drain cannot complete
    draining --> [*] : all targets drained and final deletion succeeds
    error --> [*]
```

## Execution target lifecycle

```mermaid
stateDiagram-v2
    [*] --> registering
    registering --> validating : configuration accepted
    validating --> bootstrapping : connectivity confirmed
    bootstrapping --> active : namespace and access ready
    active --> degraded : health probe fails
    degraded --> active : health probe recovers
    active --> draining : deletion requested
    degraded --> draining : deletion requested
    draining --> failed : drain cannot complete
    registering --> failed : registration fails
    validating --> failed : validation fails
    bootstrapping --> failed : bootstrap fails
    failed --> [*]
```

## Work item lifecycle

```mermaid
stateDiagram-v2
    [*] --> pending
    pending --> claimed : worker claims item
    claimed --> dispatched : target assigned and resources requested
    dispatched --> running : Execute submitted
    running --> completed : successful terminal result
    running --> failed : deliberate or runtime failure
    running --> cancelled : cancellation confirmed
    running --> reconciliation_required : terminal result uncertain
    claimed --> pending : pre-execute retry
    dispatched --> pending : pre-execute retry
    pending --> failed : no eligible target
    completed --> [*]
    failed --> [*]
    cancelled --> [*]
    reconciliation_required --> [*]
```

## Security control inventory

| Boundary | Controls |
| --- | --- |
| AO → EP ingress | HTTPS, ES256 service JWT, issuer and audience validation, client allow-list, command-scope enforcement, strict JSON parsing, body and payload size limits. A deployment may also require or present a client certificate, but the service JWT remains the mandatory application-layer identity. |
| EP → AO callbacks | HTTPS with deployment-configured mTLS, typed command envelopes, receipt correlation, bounded retries. |
| EP → PostgreSQL | TLS with configurable verification and optional client certificates, service-owned database credentials, AES-GCM encryption of work payloads. |
| EP → Kubernetes | TLS with optional custom CA, bearer token from a short-lived resolved credential, no credential persistence, sanitized API errors. |
| Workload isolation | No service-account token in the workload Pod, non-root execution, read-only root filesystem, dropped Linux capabilities, RuntimeDefault seccomp, bounded CPU and memory, memory-backed temporary storage, one completion, no Job backoff, active deadline, TTL cleanup backstop. |
| Workload network | Per-attempt NetworkPolicy, ingress denied, DNS permitted, operator-configured egress allow and deny ranges. |
| Workload ABI | Pinned plugin SDK contract, Health/Execute/Cancel semantics, one invocation per execution attempt, progress and terminal events on a bounded gRPC stream. |
| Result safety | Result committed before resource cleanup, claim-owner and claim-generation fencing, no automatic rerun after Execute may have started. |
| Completion delivery | Durable WorkItems-table state, at-least-once delivery, exponential backoff with jitter, receipt validation. |
| Credential handling | Opaque references only in durable state, short-lived resolution through AO, no raw credential logging or API exposure. |
| Logging and diagnostics | Structured logs, bounded Kubernetes response bodies, sanitized work-item errors, no raw credentials or private payloads in results. |
| Image provenance | Image reference is caller-supplied; EP does not independently enforce registry policy or digest pinning. |

## Current limitations

- Only AO is a configured caller.
- Only the Kubernetes backend is registered for execution.
- Only container workloads are dispatched; other workload types are future work.
- Workload credential-file injection is not implemented.
- EP does not enforce image provenance beyond accepting the caller-selected image reference.
- EP-to-workload control traffic is tunneled through the Kubernetes API rather than exposed as direct pod networking.
- A lost terminal result after Execute may have started is marked for reconciliation rather than automatically retried.
- OpenShell is modelled as a backend type but is not part of the current execution path.
