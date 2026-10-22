"""Command domain effects use existing EP resources, never command records."""

from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel import col

from execution_plane.api.auth import ServiceIdentity, get_service_identity
from execution_plane.api.main import create_app
from execution_plane.models.cluster import Cluster
from tests.test_command_api import command_message

pytestmark = pytest.mark.integration


@pytest.fixture
async def command_client(migrated_database):
    engine = create_async_engine(migrated_database)
    app = create_app()
    app.state.ep_engine = engine
    app.state.ep_session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.dependency_overrides[get_service_identity] = lambda: ServiceIdentity(
        client_id="ao",
        scopes=frozenset(
            {
                "work.execute",
                "command.cancel",
                "cluster.register",
                "cluster.update",
                "cluster.delete",
                "clusters:read",
                "work-items:read",
                "execution_target.register",
                "execution_target.update",
                "execution_target.delete",
                "execution-targets:read",
            }
        ),
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://ep.test") as client:
        yield client, app.state.ep_session_factory
    await engine.dispose()


async def test_cluster_commands_provision_and_manage_clusters_directly(command_client):
    client, sessions = command_client
    key = uuid4()
    payload = {
        "resource_key": str(key),
        "desired_revision": 1,
        "name": "test",
        "endpoint": "https://cluster.test",
        "namespace": "ep",
        "credential_ref": str(uuid4()),
    }
    created = await client.post("/v1/commands", json=command_message("cluster.register", payload))
    assert created.status_code == 202, created.text
    cluster_id = UUID(created.json()["resource"]["resource_id"])
    async with sessions() as session:
        cluster = await session.get(Cluster, cluster_id)
        assert cluster.status.value == "active"
        assert cluster.source_revision == 1
        assert "private-secret" not in repr(cluster)

    # Idempotent repeat at same revision.
    repeated = await client.post("/v1/commands", json=command_message("cluster.register", payload))
    assert repeated.status_code == 202
    assert UUID(repeated.json()["resource"]["resource_id"]) == cluster_id

    update = await client.post(
        "/v1/commands",
        json=command_message("cluster.update", {"resource_key": str(key), "desired_revision": 2, "name": "updated"}),
    )
    assert update.status_code == 202

    # Stale register after update → conflict.
    stale = await client.post("/v1/commands", json=command_message("cluster.register", payload))
    assert stale.status_code == 409

    async with sessions() as session:
        cluster = await session.get(Cluster, cluster_id)
        assert cluster.name == "updated"
        assert cluster.source_revision == 2

    deleted = await client.post(
        "/v1/commands", json=command_message("cluster.delete", {"resource_key": str(key), "desired_revision": 3})
    )
    assert deleted.status_code == 202
    read = await client.get(f"/v1/clusters/{cluster_id}")
    assert read.status_code == 200
    assert read.json()["enabled"] is False
    assert read.json()["status"] == "draining"


async def test_work_queries_recover_large_results_without_command_history(command_client):
    from datetime import UTC, datetime

    from execution_plane.models.work_item import WorkItem, WorkItemStatus

    client, sessions = command_client
    work_id, other_id = uuid4(), uuid4()
    result = {"stdout": "x" * (600 * 1024)}
    async with sessions.begin() as session:
        for resource_id, owner in ((work_id, "ao"), (other_id, "other")):
            session.add(
                WorkItem(
                    id=resource_id,
                    client_id=owner,
                    payload={},
                    result=result,
                    status=WorkItemStatus.COMPLETED,
                    created_at=datetime.now(UTC),
                )
            )
    read = await client.get(f"/v1/work-items/{work_id}")
    assert read.status_code == 200
    assert read.json()["result"] == result
    assert read.json()["result_omitted"] is False
    assert (await client.get(f"/v1/work-items/{other_id}")).status_code == 404
    listing = await client.get("/v1/work-items", params={"limit": 100})
    assert str(other_id) not in listing.text
    row = next(item for item in listing.json()["resources"] if item["id"] == str(work_id))
    assert row["result"] is None
    assert row["result_omitted"] is True
    assert "private-secret" not in read.text


async def test_work_command_uses_command_uuid_and_cancel_uses_work_state(command_client):
    client, _ = command_client
    message = command_message(
        "work.execute",
        {
            "image": "node:test",
            "invocation": {
                "inputs": {},
                "credentials": {},
                "workflow_context": {},
                "settings": {},
                "timeout_seconds": 30,
                "max_output_bytes": 1024,
            },
        },
    )
    response = await client.post("/v1/commands", json=message)
    assert response.status_code == 202, response.text
    work_id = UUID(message["command_id"])
    read = await client.get(f"/v1/work-items/{work_id}")
    assert read.json()["status"] == "pending"
    duplicate = await client.post("/v1/commands", json=message)
    assert duplicate.status_code == 409
    cancel = await client.post(
        "/v1/commands", json=command_message("command.cancel", {"target_command_id": str(work_id)})
    )
    assert cancel.status_code == 202
    read = await client.get(f"/v1/work-items/{work_id}")
    assert read.json()["status"] == "cancelled"
    missing = await client.post("/v1/commands", json=command_message())
    assert missing.status_code == 404


async def test_concurrent_cluster_creates_result_in_exactly_one_cluster(command_client):
    import asyncio

    client, sessions = command_client
    key = uuid4()
    payload = {
        "resource_key": str(key),
        "desired_revision": 1,
        "name": "concurrent",
        "endpoint": "https://cluster.test",
        "namespace": "ep",
        "credential_ref": str(uuid4()),
    }
    responses = await asyncio.gather(
        *(client.post("/v1/commands", json=command_message("cluster.register", payload)) for _ in range(2))
    )
    statuses = sorted(r.status_code for r in responses)
    assert statuses in ([202, 202], [202, 409])
    async with sessions() as session:
        result = await session.execute(
            select(Cluster)
            .where(col(Cluster.source_client_id) == "ao")
            .where(col(Cluster.source_integration_id) == key)
        )
        clusters = list(result.scalars().all())
    assert len(clusters) == 1


async def test_cluster_update_preserves_omitted_fields_and_rejects_null_required_fields(command_client):
    client, sessions = command_client
    key = uuid4()
    project = uuid4()
    payload = {
        "resource_key": str(key),
        "desired_revision": 1,
        "name": "test",
        "endpoint": "https://cluster.test",
        "namespace": "ep",
        "credential_ref": str(uuid4()),
        "project_ids": [str(project)],
    }
    created = await client.post("/v1/commands", json=command_message("cluster.register", payload))
    assert created.status_code == 202
    cluster_id = UUID(created.json()["resource"]["resource_id"])

    patch = {"resource_key": str(key), "desired_revision": 2, "project_ids": None}
    assert (await client.post("/v1/commands", json=command_message("cluster.update", patch))).status_code == 202
    async with sessions() as session:
        cluster = await session.get(Cluster, cluster_id)
        assert cluster.project_ids is None
        assert cluster.source_revision == 2

    invalid = await client.post(
        "/v1/commands", json=command_message("cluster.update", {**patch, "desired_revision": 3, "namespace": None})
    )
    assert invalid.status_code == 422
    async with sessions() as session:
        cluster = await session.get(Cluster, cluster_id)
        assert cluster.source_revision == 2


async def test_cluster_commands_cleanse_internal_labels(command_client):
    client, sessions = command_client
    key = uuid4()
    payload = {
        "resource_key": str(key),
        "desired_revision": 1,
        "name": "test",
        "endpoint": "https://cluster.test",
        "namespace": "ep",
        "credential_ref": str(uuid4()),
        "labels": {"team": "platform", "_cluster_id": "spoofed", "_internal": "spoofed"},
    }
    created = await client.post("/v1/commands", json=command_message("cluster.register", payload))
    assert created.status_code == 202
    cluster_id = UUID(created.json()["resource"]["resource_id"])
    async with sessions() as session:
        cluster = await session.get(Cluster, cluster_id)
        assert "team" in cluster.labels
        assert cluster.labels.get("_cluster_id") == str(cluster_id)
        assert "_internal" not in cluster.labels
        assert cluster.labels["team"] == "platform"

    patch = {
        "resource_key": str(key),
        "desired_revision": 2,
        "labels": {"team": "runtime", "_cluster_id": "spoofed", "_internal": "spoofed"},
    }
    assert (await client.post("/v1/commands", json=command_message("cluster.update", patch))).status_code == 202
    async with sessions() as session:
        cluster = await session.get(Cluster, cluster_id)
        assert cluster.labels["team"] == "runtime"
        assert cluster.labels.get("_cluster_id") == str(cluster_id)
        assert "_internal" not in cluster.labels


async def test_target_commands_add_and_preserve_the_target_identity_label(command_client):
    from datetime import UTC, datetime

    from execution_plane.models.cluster import Cluster, ClusterStatus

    client, sessions = command_client
    now = datetime.now(UTC)
    cluster_id, target_id = uuid4(), uuid4()
    async with sessions.begin() as session:
        session.add(
            Cluster(
                id=cluster_id,
                name=str(cluster_id),
                endpoint="https://cluster.test",
                credential_ref=uuid4(),
                source_client_id="ao",
                status=ClusterStatus.ACTIVE,
                created_by=uuid4(),
                updated_by=uuid4(),
                created_at=now,
                updated_at=now,
            )
        )
    payload = {
        "resource_id": str(target_id),
        "cluster_id": str(cluster_id),
        "name": "workers",
        "backend_type": "vanilla_k8s",
        "endpoint": "https://cluster.test",
        "credential_ref": str(uuid4()),
        "placement": {"type": "kubernetes", "namespace": "ep"},
        "labels": {"region": "eu-west", "_execution_target_id": "spoofed", "_internal": "spoofed"},
    }
    created = await client.post("/v1/commands", json=command_message("execution_target.register", payload))
    assert created.status_code == 202, created.text
    read = await client.get(f"/v1/execution-targets/{target_id}")
    assert read.json()["labels"] == {"region": "eu-west", "_execution_target_id": str(target_id)}

    patch = {
        "resource_id": str(target_id),
        "labels": {"region": "us-east", "_execution_target_id": "spoofed", "_internal": "spoofed"},
    }
    assert (
        await client.post("/v1/commands", json=command_message("execution_target.update", patch))
    ).status_code == 202
    read = await client.get(f"/v1/execution-targets/{target_id}")
    assert read.json()["labels"] == {"region": "us-east", "_execution_target_id": str(target_id)}


async def test_targets_enforce_cluster_ownership_defaults_and_domain_lifecycle(command_client):
    from datetime import UTC, datetime

    from execution_plane.models.cluster import Cluster, ClusterStatus

    client, sessions = command_client
    now = datetime.now(UTC)
    cluster_id, foreign_id, target_id, replacement_id = uuid4(), uuid4(), uuid4(), uuid4()
    async with sessions.begin() as session:
        for resource_id, owner in ((cluster_id, "ao"), (foreign_id, "other")):
            session.add(
                Cluster(
                    id=resource_id,
                    name=str(resource_id),
                    endpoint="https://cluster.test",
                    credential_ref=uuid4(),
                    source_client_id=owner,
                    status=ClusterStatus.ACTIVE,
                    created_by=uuid4(),
                    updated_by=uuid4(),
                    created_at=now,
                    updated_at=now,
                )
            )
    payload = {
        "resource_id": str(target_id),
        "cluster_id": str(cluster_id),
        "name": "workers",
        "backend_type": "vanilla_k8s",
        "endpoint": "https://cluster.test",
        "credential_ref": str(uuid4()),
        "placement": {"type": "kubernetes", "namespace": "ep", "node_selectors": ["env=test"]},
    }
    foreign = await client.post(
        "/v1/commands", json=command_message("execution_target.register", {**payload, "cluster_id": str(foreign_id)})
    )
    assert foreign.status_code == 404
    default_created = await client.post(
        "/v1/commands", json=command_message("execution_target.register", {**payload, "is_default": True})
    )
    assert default_created.status_code == 202, default_created.text
    read = await client.get(f"/v1/execution-targets/{target_id}")
    assert read.json()["is_default"] is True
    replacement_payload = {**payload, "resource_id": str(replacement_id), "name": "replacement"}
    created = await client.post("/v1/commands", json=command_message("execution_target.register", replacement_payload))
    assert created.status_code == 202, created.text
    read = await client.get(f"/v1/execution-targets/{replacement_id}")
    assert read.status_code == 200
    assert read.json()["status"] == "active"
    assert read.json()["is_default"] is False
    assert "secret" not in read.text
    update = await client.post(
        "/v1/commands",
        json=command_message(
            "execution_target.update",
            {"resource_id": str(target_id), "placement": {"type": "kubernetes", "namespace": "new"}},
        ),
    )
    assert update.status_code == 202
    read = await client.get(f"/v1/execution-targets/{target_id}")
    assert read.json()["placement"]["namespace"] == "new"
    assert read.json()["placement"]["node_selectors"] == ["env=test"]
    promoted = await client.post(
        "/v1/commands",
        json=command_message("execution_target.update", {"resource_id": str(replacement_id), "is_default": True}),
    )
    assert promoted.status_code == 202
    read = await client.get(f"/v1/execution-targets/{target_id}")
    assert read.json()["is_default"] is False
    read = await client.get(f"/v1/execution-targets/{replacement_id}")
    assert read.json()["is_default"] is True
    demoted = await client.post(
        "/v1/commands",
        json=command_message("execution_target.update", {"resource_id": str(replacement_id), "is_default": False}),
    )
    assert demoted.status_code == 409
    delete = await client.post(
        "/v1/commands", json=command_message("execution_target.delete", {"resource_id": str(target_id)})
    )
    assert delete.status_code == 202
    read = await client.get(f"/v1/execution-targets/{target_id}")
    assert read.json()["status"] == "draining"
    assert read.json()["enabled"] is False


async def test_default_target_delete_is_rejected(command_client):
    from datetime import UTC, datetime

    from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
    from execution_plane.models.cluster import Cluster, ClusterStatus
    from execution_plane.models.execution_target import BackendType
    from execution_plane.models.execution_target_placement import KubernetesPlacement

    client, sessions = command_client
    now, cluster_id = datetime.now(UTC), uuid4()
    async with sessions.begin() as session:
        session.add(
            Cluster(
                id=cluster_id,
                name=str(cluster_id),
                endpoint="https://cluster.test",
                credential_ref=uuid4(),
                source_client_id="ao",
                status=ClusterStatus.ACTIVE,
                created_by=uuid4(),
                updated_by=uuid4(),
                created_at=now,
                updated_at=now,
            )
        )
    async with sessions.begin() as session:
        target_store = ExecutionTargetStore.from_transaction(session)
        default = await target_store.create(
            cluster_id=cluster_id,
            name="default",
            backend_type=BackendType.VANILLA_K8S,
            endpoint="https://cluster.test",
            credential_ref=uuid4(),
            is_default=True,
            created_by=uuid4(),
            placement=KubernetesPlacement(namespace="ep"),
        )
    delete = await client.post(
        "/v1/commands", json=command_message("execution_target.delete", {"resource_id": str(default.id)})
    )
    assert delete.status_code == 409


async def test_failed_target_creation_rolls_back(command_client, monkeypatch):
    from datetime import UTC, datetime

    from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
    from execution_plane.models.cluster import Cluster, ClusterStatus
    from execution_plane.models.execution_target import ExecutionTarget

    client, sessions = command_client
    now, cluster_id, target_id = datetime.now(UTC), uuid4(), uuid4()
    async with sessions.begin() as session:
        session.add(
            Cluster(
                id=cluster_id,
                name=str(cluster_id),
                endpoint="https://cluster.test",
                credential_ref=uuid4(),
                source_client_id="ao",
                status=ClusterStatus.ACTIVE,
                created_by=uuid4(),
                updated_by=uuid4(),
                created_at=now,
                updated_at=now,
            )
        )

    async def fail_activation(self, target_id, actor) -> None:
        message = "simulated activation failure"
        raise RuntimeError(message)

    monkeypatch.setattr(ExecutionTargetStore, "activate", fail_activation)
    payload = {
        "resource_id": str(target_id),
        "cluster_id": str(cluster_id),
        "name": "independent",
        "backend_type": "vanilla_k8s",
        "endpoint": "https://cluster.test",
        "credential_ref": str(uuid4()),
        "placement": {"type": "kubernetes", "namespace": "ep"},
    }
    with pytest.raises(RuntimeError, match="simulated activation failure"):
        await client.post("/v1/commands", json=command_message("execution_target.register", payload))
    async with sessions() as session:
        assert await session.get(ExecutionTarget, target_id) is None


def _cluster_register_payload(key) -> dict[str, object]:
    return {
        "resource_key": str(key),
        "desired_revision": 1,
        "name": "test",
        "endpoint": "https://cluster.test",
        "namespace": "ep",
        "credential_ref": str(uuid4()),
    }


@pytest.mark.parametrize("omitted", ["name", "endpoint", "namespace"])
async def test_cluster_register_requires_each_shared_config_field(command_client, omitted):
    """Every required field in the shared ClusterConfigBase shape is enforced end to end."""
    from sqlalchemy import select
    from sqlmodel import col

    client, sessions = command_client
    key = uuid4()
    payload = _cluster_register_payload(key)
    del payload[omitted]
    response = await client.post("/v1/commands", json=command_message("cluster.register", payload))
    assert response.status_code == 422, response.text
    assert "private-secret" not in response.text
    async with sessions() as session:
        result = await session.execute(
            select(Cluster)
            .where(col(Cluster.source_client_id) == "ao")
            .where(col(Cluster.source_integration_id) == key)
        )
        assert result.scalar_one_or_none() is None


async def test_cluster_register_persists_the_full_shared_shape(command_client):
    """A valid command provisions a cluster with the expected fields."""
    client, sessions = command_client
    key, project = uuid4(), uuid4()
    payload = {
        **_cluster_register_payload(key),
        "project_ids": [str(project)],
        "labels": {"team": "platform"},
    }
    created = await client.post("/v1/commands", json=command_message("cluster.register", payload))
    assert created.status_code == 202, created.text
    cluster_id = UUID(created.json()["resource"]["resource_id"])
    async with sessions() as session:
        cluster = await session.get(Cluster, cluster_id)
    assert cluster.name == "test"
    assert cluster.endpoint == "https://cluster.test"
    assert cluster.source_integration_id == key
    assert cluster.project_ids == [project]
    assert cluster.enabled is True


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("endpoint", "http://cluster.test"),
        ("endpoint", "https://user:pw@cluster.test"),
        ("ca_certificate", "not-a-valid-pem"),
    ],
)
async def test_cluster_register_rejects_invalid_field_values(command_client, field, value):
    """The field and model validators still fire."""
    from sqlalchemy import select
    from sqlmodel import col

    client, sessions = command_client
    key = uuid4()
    payload = {**_cluster_register_payload(key), field: value}
    response = await client.post("/v1/commands", json=command_message("cluster.register", payload))
    assert response.status_code == 422, response.text
    assert "private-secret" not in response.text
    async with sessions() as session:
        result = await session.execute(
            select(Cluster)
            .where(col(Cluster.source_client_id) == "ao")
            .where(col(Cluster.source_integration_id) == key)
        )
        assert result.scalar_one_or_none() is None


async def test_enabled_cluster_binding_requires_a_credential(command_client):
    """The cross-field model validator rejects an enabled register without a credential."""
    from sqlalchemy import select
    from sqlmodel import col

    client, sessions = command_client
    key = uuid4()
    payload = _cluster_register_payload(key)
    del payload["credential_ref"]  # enabled defaults to True
    response = await client.post("/v1/commands", json=command_message("cluster.register", payload))
    assert response.status_code == 422, response.text
    async with sessions() as session:
        result = await session.execute(
            select(Cluster)
            .where(col(Cluster.source_client_id) == "ao")
            .where(col(Cluster.source_integration_id) == key)
        )
        assert result.scalar_one_or_none() is None
