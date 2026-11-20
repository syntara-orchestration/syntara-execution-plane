"""Real command ingress commits bulk intent atomically without changing outcomes."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import event, select, text

from execution_plane.models.work_item import WorkItem
from execution_plane.work_item.work_item_store import WorkStore
from execution_plane_client.commands.bulk_work import WorkItemSelector
from execution_plane_client.models.work_item import WorkItemStatus
from tests.integration.postgres import test_commands
from tests.test_command_api import command_message

bulk_client = test_commands.command_client


pytestmark = pytest.mark.integration


@pytest.mark.parametrize("command", ["work.cancel_bulk", "work.delete_bulk"])
@pytest.mark.parametrize("field", ["group", "region", "label", "work_item_ids"])
async def test_bulk_command_commits_all_owned_matches_and_preserves_outcomes(bulk_client, command, field):
    client, sessions = bulk_client
    owner_id = uuid4()
    states = list(WorkItemStatus)
    ids = [uuid4() for _ in states]
    foreign, other, missing = uuid4(), uuid4(), uuid4()
    async with sessions.begin() as session:
        for item_id, state in zip(ids, states, strict=True):
            session.add(
                WorkItem(
                    created_at=datetime.now(UTC),
                    id=item_id,
                    client_id="ao",
                    status=state,
                    runtime_context={field: str(owner_id)},
                    result={"original": True},
                )
            )
        session.add(
            WorkItem(
                created_at=datetime.now(UTC), id=foreign, client_id="foreign", runtime_context={field: str(owner_id)}
            )
        )
        session.add(
            WorkItem(created_at=datetime.now(UTC), id=other, client_id="ao", runtime_context={field: str(uuid4())})
        )
        session.add(WorkItem(created_at=datetime.now(UTC), id=missing, client_id="ao"))
    selector = (
        {field: [str(item) for item in [*ids, foreign, uuid4(), ids[0]]]}
        if field == "work_item_ids"
        else {"runtime_context": {field: str(owner_id)}}
    )
    message = command_message(command, {"selector": selector})
    for _ in range(2):
        response = await client.post("/v1/commands", json=message)
        assert response.status_code == 202, response.text
        assert response.json()["message_id"] == message["message_id"]
        # A separate session sees committed effects immediately after the receipt.
        async with sessions() as session:
            for item_id, state in zip(ids, states, strict=True):
                item = await session.get(WorkItem, item_id)
                assert item.cancellation_requested is True
                assert item.deletion_requested is (command == "work.delete_bulk")
                assert item.status == state
                assert item.result == {"original": True}
                assert item.completed_at is None
            for item_id in (foreign, other, missing):
                item = await session.get(WorkItem, item_id)
                assert not item.cancellation_requested
                assert not item.deletion_requested
    empty = await client.post(
        "/v1/commands", json=command_message(command, {"selector": {"runtime_context": {"run": str(uuid4())}}})
    )
    assert empty.status_code == 202


async def test_filter_has_no_batch_limit_and_uses_one_update(bulk_client):
    client, sessions = bulk_client
    owner_id = uuid4()
    async with sessions.begin() as session:
        session.add_all(
            [
                WorkItem(
                    created_at=datetime.now(UTC),
                    id=uuid4(),
                    client_id="ao",
                    runtime_context={"group": str(owner_id)},
                )
                for _ in range(250)
            ]
        )
    statements = []
    engine = sessions.kw["bind"]

    def record(
        _conn: object, _cursor: object, statement: str, _parameters: object, _context: object, _many: object
    ) -> None:
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", record)
    try:
        response = await client.post(
            "/v1/commands",
            json=command_message("work.delete_bulk", {"selector": {"runtime_context": {"group": str(owner_id)}}}),
        )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", record)
    assert response.status_code == 202, response.text
    assert sum(statement.startswith("UPDATE ") for statement in statements) == 1
    assert sum("pg_notify" in statement for statement in statements) == 1
    async with sessions() as session:
        items = (
            (
                await session.execute(
                    select(WorkItem).where(
                        WorkItem.deletion_requested.is_(True),
                        WorkItem.runtime_context.contains({"group": str(owner_id)}),
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(items) == 250
        await session.execute(text("SET LOCAL enable_seqscan=off"))
        plan = (
            await session.execute(
                text(
                    "EXPLAIN SELECT id FROM execution_plane.work_items WHERE runtime_context @> CAST(:context AS jsonb)"
                ),
                {"context": '{"group":"' + str(owner_id) + '"}'},
            )
        ).all()
        assert "ix_work_items_runtime_context" in str(plan)


async def test_notification_failure_rolls_back_entire_update(bulk_client):
    _, sessions = bulk_client
    ids = [uuid4(), uuid4()]
    async with sessions.begin() as session:
        session.add_all([WorkItem(created_at=datetime.now(UTC), id=item_id, client_id="ao") for item_id in ids])
    engine = sessions.kw["bind"]

    def fail_notify(
        _conn: object, _cursor: object, statement: str, _parameters: object, _context: object, _many: object
    ) -> None:
        if "pg_notify" in statement:
            message = "injected failure"
            raise RuntimeError(message)

    event.listen(engine.sync_engine, "before_cursor_execute", fail_notify)
    try:
        with pytest.raises(RuntimeError, match="injected failure"):
            await WorkStore.from_engine(engine).request_bulk_control(
                WorkItemSelector(work_item_ids=ids), client_id="ao", delete=True
            )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", fail_notify)
    async with sessions() as session:
        for item_id in ids:
            item = await session.get(WorkItem, item_id)
            assert not item.cancellation_requested
            assert not item.deletion_requested


@pytest.mark.parametrize("command", ["work.cancel_bulk", "work.delete_bulk"])
async def test_generic_context_filter_uses_jsonb_containment(bulk_client, command):
    client, sessions = bulk_client
    ids = [uuid4() for _ in range(5)]
    contexts = [
        {"batch": {"label": "nightly", "extra": True}, "priority": 3, "labels": ["a", "b"]},
        {"batch": {"label": "nightly"}, "priority": 3, "labels": ["a"]},
        {"batch": {"label": "nightly"}, "priority": "3", "labels": ["a"]},
        {"batch": {"label": "daily"}, "priority": 3, "labels": ["a"]},
        {"batch": {"label": "nightly"}, "priority": 3, "labels": ["b"]},
    ]
    async with sessions.begin() as session:
        session.add_all(
            [
                WorkItem(created_at=datetime.now(UTC), id=item_id, client_id="ao", runtime_context=context)
                for item_id, context in zip(ids, contexts, strict=True)
            ]
        )
    response = await client.post(
        "/v1/commands",
        json=command_message(
            command, {"selector": {"runtime_context": {"batch": {"label": "nightly"}, "priority": 3, "labels": ["a"]}}}
        ),
    )
    assert response.status_code == 202, response.text
    async with sessions() as session:
        for index, item_id in enumerate(ids):
            item = await session.get(WorkItem, item_id)
            assert item.cancellation_requested is (index < 2)
            assert item.deletion_requested is (index < 2 and command == "work.delete_bulk")
