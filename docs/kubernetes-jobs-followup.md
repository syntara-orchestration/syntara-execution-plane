# Cold-start Kubernetes Job details

The cold-start backend wraps the SDK worker Pod in a deterministic
`batch/v1` Job. The Job is a provisioning detail owned by
`VanillaK8sWorkerManager`; logical WorkItem state remains in EP's database and
does not inherit Job or Pod lifecycle semantics.

Each claim-generation attempt uses one completion, one parallel Pod,
`backoffLimit: 0`, a bounded active deadline, and a one-hour TTL as a backstop.
The worker creates a per-attempt NetworkPolicy first, creates the Job, discovers
the generated Pod by its Job label, verifies its owner UID, and opens the
authenticated gRPC port-forward. Only the node protocol carries application
inputs and returned output.

After a result frame arrives, EP persists it and its completion outbox before
deleting the exclusively owned Job with foreground propagation. Cleanup state
is recorded separately. If the controller loses ownership after Execute may
have been submitted, it leaves the WorkItem in reconciliation and does not
launch a second Job. The existing node runtime cannot replay an uncommitted
result, so Jobs improve resource ownership but do not create exactly-once
execution.

The deterministic name is derived from the logical WorkItem ID and claim
generation. A name conflict is treated as an uncertain prior attempt; the
worker will not attach and invoke again without durable state proving Execute
was never submitted. Job owner UID validation prevents dispatch to an unrelated
Pod with a matching label.

The TTL cleans finished Jobs and owned Pods, but not NetworkPolicies. EP records
the Job UID on the policy and periodically removes aged policies only after
confirming that the Job and matching Pods are absent. A target-cluster CNI must
be tested to confirm the policy is enforced. Port-forward behavior and
NetworkPolicy enforcement must be evaluated together on the chosen cluster.

The resource model remains local to this cold-start implementation. A future
warm-worker manager may attach to an existing worker and must return or
quarantine it; it cannot equate a WorkItem ending with deleting a shared Pod.
