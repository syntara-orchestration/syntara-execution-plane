# Workload image policy

For the built-in Python and Bash script Action identities, EP requires a
digest-pinned image reference that matches the Action's exact approved image
and Cosign public key. EP checks the mapping before it persists a command and
verifies the signature again before creating a Job. An old queued item, direct
API request, or administrator-edited work row cannot bypass the dispatch-time
check. Generic `work.execute` callers retain their existing image-reference
contract and do not use this script-specific policy.

The standalone Kubernetes worker manifest mounts the ConfigMap
`execution-plane-script-image-policy` at `/etc/ep/script-image-policy` and sets
`EP_WORKLOAD_IMAGE_POLICY_PATH`. The operator deployment derives the ConfigMap
name from its `AutomationOrchestrator` resource. The ConfigMap must contain
`policy.json` and the key files named by `key_file` values.
Restrict write access to this ConfigMap: it is the deployment's image and
signer authorization policy, so changing both the digest and public key changes
which workload publisher EP trusts.

Example structure (replace each digest with the authenticated release digest for
the selected upstream or downstream installation):

```json
{
  "version": 1,
  "actions": {
    "syntara.script-python.script-python": {
      "image_reference": "registry.example/ao-python@sha256:<approved-digest>",
      "key_file": "cosign-release.pub"
    },
    "syntara.script-bash.script-bash": {
      "image_reference": "registry.example/ao-bash@sha256:<approved-digest>",
      "key_file": "cosign-release.pub"
    }
  }
}
```

`key_file` must be a filename in the same mounted directory as the policy.
Only one reference is approved per Action in a deployment. Updating a digest or
rotating its key requires an intentional policy update. A tag, different digest,
missing key, unavailable Cosign executable, or failed signature verification
blocks the work item. Cosign stdout and stderr are not included in errors or
logs.

The build installs the Cosign v3.1.4 Linux binary from a Hermeto generic artifact
lockfile and checks its architecture-specific SHA-256 before installation.
Product Security and Release Engineering must provide the production image
digests and approved public keys; no sample values in this document are release
approvals.

The local `tools/submit_work_item.py` helper submits a generic `script.v1`
identity and keeps its existing local-development image default. It is outside
this policy. The digest and signature requirements above apply to the built-in
Python and Bash Action identities only.
