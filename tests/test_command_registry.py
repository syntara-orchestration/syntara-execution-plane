"""EP command installation maps each command to a dedicated handler."""

from execution_plane.commands.clusters import (
    ClusterDeleteCommandHandler,
    ClusterProvisionCommandHandler,
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
        "cluster.provision": ClusterProvisionCommandHandler,
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
