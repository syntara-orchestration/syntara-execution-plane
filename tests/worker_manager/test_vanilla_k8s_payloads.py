"""Contract checks for the cold-start Job and its workload network policy."""

import pytest

from execution_plane.worker_manager.vanilla_k8s.manager import NodeExecutionError, _map_result
from execution_plane.worker_manager.vanilla_k8s.transport import job_body, network_policy_body


def _invocation() -> dict[str, object]:
    return {"timeout_seconds": 60}


def test_job_body_is_a_single_attempt_worker_allocation() -> None:
    job = job_body(
        "syntara-node-attempt",
        "work-item-id",
        "work-item-id-2",
        "registry.example/node@sha256:abc",
        _invocation(),
        startup=90,
        grace=15,
    )

    assert job["kind"] == "Job"
    assert job["spec"]["completions"] == 1
    assert job["spec"]["parallelism"] == 1
    assert job["spec"]["backoffLimit"] == 0
    pod_spec = job["spec"]["template"]["spec"]
    assert pod_spec["restartPolicy"] == "Never"
    assert pod_spec["automountServiceAccountToken"] is False
    assert pod_spec["activeDeadlineSeconds"] == 165
    assert pod_spec["containers"][0]["image"] == "registry.example/node@sha256:abc"


def test_network_policy_allows_dns_and_excludes_forbidden_workload_ranges() -> None:
    policy = network_policy_body(
        "work-item-id",
        "work-item-id-2",
        "syntara-node-attempt",
        ["10.0.0.0/8"],
        ["10.20.0.0/16"],
    )

    assert policy["spec"]["ingress"] == []
    assert policy["spec"]["policyTypes"] == ["Ingress", "Egress"]
    egress = policy["spec"]["egress"]
    assert egress[0]["ports"] == [{"protocol": "UDP", "port": 53}, {"protocol": "TCP", "port": 53}]
    assert egress[1]["to"][0]["ipBlock"]["except"] == ["10.20.0.0/16"]


@pytest.mark.parametrize(
    ("allowed", "forbidden", "expected_allowed"),
    [
        (["10.0.0.0/8"], ["10.0.0.0/8"], []),
        (["10.0.0.0/16"], ["10.0.0.0/8"], []),
        (["10.0.0.0/8"], ["10.20.0.0/16", "10.20.1.0/24"], ["10.0.0.0/8"]),
        (["2001:db8::/32"], ["2001:db8::/24"], []),
        (["2001:db8::/32"], ["2001:db8:1::/48"], ["2001:db8::/32"]),
    ],
)
def test_network_policy_forbidden_network_overlaps_never_reopen_denied_addresses(
    allowed: list[str], forbidden: list[str], expected_allowed: list[str]
) -> None:
    policy = network_policy_body("work-item", "attempt", "worker", allowed, forbidden)
    destinations = policy["spec"]["egress"][1:]

    assert [rule["to"][0]["ipBlock"]["cidr"] for rule in destinations] == expected_allowed


def test_network_policy_removes_nested_redundant_exclusions() -> None:
    policy = network_policy_body(
        "work-item",
        "attempt",
        "worker",
        ["10.0.0.0/8"],
        ["10.20.0.0/16", "10.20.1.0/24"],
    )

    assert policy["spec"]["egress"][1]["to"][0]["ipBlock"]["except"] == ["10.20.0.0/16"]


def test_node_result_preserves_partial_output_on_failure() -> None:
    frame = {
        "result": {
            "StatusCode": 2,
            "StatusMessage": "failed",
            "ErrorMessage": "node exited with failure",
            "Result": {"stdout": "partial", "stderr": "problem", "exit_code": 2},
        },
        "error": {"type": "ScriptExecutionError"},
    }

    with pytest.raises(NodeExecutionError) as exc_info:
        _map_result(frame, {"stdout": "${result.stdout}", "exit_code": "${result.exit_code}"})
    assert exc_info.value.error_type == "ScriptExecutionError"
    assert exc_info.value.output == {"stdout": "partial", "exit_code": 2}
