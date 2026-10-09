"""Tests for JSON-backed model types used by EP persistence."""

import uuid

import pytest
from sqlalchemy.dialects import postgresql

from execution_plane.models.cluster import Cluster
from execution_plane.models.cluster_binding import ClusterBinding
from execution_plane.models.sqlmodel_types import UUIDListJSONB
from execution_plane.models.work_item import WorkItem, WorkItemStatus


@pytest.mark.parametrize("model", [Cluster, ClusterBinding])
def test_project_scope_column_serializes_and_restores_uuid_lists(model: type) -> None:
    project_ids = [uuid.uuid4(), uuid.uuid4()]
    column_type = model.__table__.c.project_ids.type
    dialect = postgresql.dialect()

    stored_value = column_type.process_bind_param(project_ids, dialect)
    restored_value = column_type.process_result_value(stored_value, dialect)

    assert stored_value == [str(project_id) for project_id in project_ids]
    assert restored_value == project_ids


def test_project_scope_none_uses_sql_null_and_empty_list_remains_empty() -> None:
    column_type = Cluster.__table__.c.project_ids.type
    dialect = postgresql.dialect()

    assert isinstance(column_type, UUIDListJSONB)
    assert column_type.load_dialect_impl(dialect).none_as_null is True
    assert column_type.process_bind_param(None, dialect) is None
    assert column_type.process_bind_param([], dialect) == []
    assert column_type.process_result_value(None, dialect) is None


def test_work_item_status_column_hydrates_persisted_strings_as_enum_values() -> None:
    status_type = WorkItem.__table__.c.status.type
    result_processor = status_type.result_processor(postgresql.dialect(), None)

    assert result_processor is not None
    assert result_processor("pending") is WorkItemStatus.PENDING
    assert result_processor("claimed") is WorkItemStatus.CLAIMED
