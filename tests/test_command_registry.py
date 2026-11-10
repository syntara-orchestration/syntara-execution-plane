"""EP command installation maps each command to a dedicated handler."""

from execution_plane.commands.clusters import (
    ClusterDeleteCommandHandler,
    ClusterRegisterCommandHandler,
    ClusterUpdateCommandHandler,
)
from execution_plane.commands.registry import build_ep_registry
from execution_plane.commands.targets import (
    TargetDeleteCommandHandler,
    TargetHealthCommandHandler,
    TargetRegisterCommandHandler,
    TargetUpdateCommandHandler,
)


def test_each_resource_command_has_a_dedicated_handler() -> None:
    registry = build_ep_registry()

    expected_handlers = {
        "cluster.register": ClusterRegisterCommandHandler,
        "cluster.update": ClusterUpdateCommandHandler,
        "cluster.delete": ClusterDeleteCommandHandler,
        "execution_target.register": TargetRegisterCommandHandler,
        "execution_target.update": TargetUpdateCommandHandler,
        "execution_target.delete": TargetDeleteCommandHandler,
        "execution_target.health": TargetHealthCommandHandler,
    }

    for command_type, handler_type in expected_handlers.items():
        handler = registry.get(command_type, 1)
        assert type(handler) is handler_type


def test_provision_command_is_removed_without_an_alias() -> None:
    import pytest

    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.errors import UnsupportedCommand

    with pytest.raises(UnsupportedCommand):
        CommandCatalog().definition("cluster.provision", 1)
    with pytest.raises(UnsupportedCommand):
        build_ep_registry().get("cluster.provision", 1)
