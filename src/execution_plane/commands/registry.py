"""Install only the commands handled by EP."""

from execution_plane.commands.clusters import (
    ClusterDeleteCommandHandler,
    ClusterRegisterCommandHandler,
    ClusterUpdateCommandHandler,
)
from execution_plane.commands.context import EPCommandContext
from execution_plane.commands.control import WorkCancelHandler
from execution_plane.commands.targets import (
    TargetDeleteCommandHandler,
    TargetHealthCommandHandler,
    TargetRegisterCommandHandler,
    TargetUpdateCommandHandler,
)
from execution_plane.commands.work import WorkCommandHandler
from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.registry import CommandRegistry


def build_ep_registry() -> CommandRegistry[EPCommandContext]:
    """Construct the stateless handler registry without opening a database."""
    catalog = CommandCatalog()
    registry = CommandRegistry[EPCommandContext]()
    registry.register(catalog.definition("work.execute", 2), WorkCommandHandler())
    registry.register(catalog.definition("work.cancel", 1), WorkCancelHandler())
    registry.register(catalog.definition("cluster.register", 1), ClusterRegisterCommandHandler())
    registry.register(catalog.definition("cluster.update", 1), ClusterUpdateCommandHandler())
    registry.register(catalog.definition("cluster.delete", 1), ClusterDeleteCommandHandler())
    registry.register(catalog.definition("execution_target.register", 1), TargetRegisterCommandHandler())
    registry.register(catalog.definition("execution_target.update", 1), TargetUpdateCommandHandler())
    registry.register(catalog.definition("execution_target.delete", 1), TargetDeleteCommandHandler())
    registry.register(catalog.definition("execution_target.health", 1), TargetHealthCommandHandler())
    return registry
