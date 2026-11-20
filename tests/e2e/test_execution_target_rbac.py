"""Contract for the shared target-cluster dispatcher Role."""

from pathlib import Path

import yaml

RBAC_PATH = Path(__file__).resolve().parents[2] / "deploy" / "kubernetes" / "execution-target" / "rbac.yaml"


def _resources() -> list[dict[object, object]]:
    return [doc for doc in yaml.safe_load_all(RBAC_PATH.read_text()) if doc]


def test_dispatcher_rbac_manifest_exists() -> None:
    kinds = {(doc["kind"], doc["metadata"]["name"]) for doc in _resources()}
    assert kinds == {
        ("Namespace", "execution-plane"),
        ("ServiceAccount", "syntara-dispatcher"),
        ("Role", "syntara-node-dispatcher"),
        ("RoleBinding", "syntara-node-dispatcher"),
        ("ClusterRole", "syntara-resource-monitor"),
        ("ClusterRoleBinding", "syntara-resource-monitor"),
    }


def test_dispatcher_role_matches_vanilla_k8s_transport() -> None:
    role = next(doc for doc in _resources() if doc["kind"] == "Role")
    rules = {(tuple(rule["apiGroups"]), tuple(rule["resources"]), tuple(rule["verbs"])) for rule in role["rules"]}
    assert rules == {
        (("",), ("pods",), ("list",)),
        (("",), ("pods/portforward",), ("get", "create")),
        (("",), ("configmaps",), ("create", "get", "patch", "delete")),
        (("",), ("secrets",), ("create", "patch", "delete")),
        (("batch",), ("jobs",), ("create", "get", "delete")),
        (("networking.k8s.io",), ("networkpolicies",), ("create", "get", "list", "patch", "delete")),
    }
    resources = {resource for _, resource_names, _ in rules for resource in resource_names}
    assert "pods/log" not in resources
    assert "pods/exec" not in resources


def test_resource_monitor_cluster_role_can_get_namespaces() -> None:
    role = next(doc for doc in _resources() if doc["kind"] == "ClusterRole")
    rules = {(tuple(rule["apiGroups"]), tuple(rule["resources"]), tuple(rule["verbs"])) for rule in role["rules"]}
    assert rules == {(("",), ("namespaces",), ("get",))}
