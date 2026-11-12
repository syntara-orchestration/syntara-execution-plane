"""Describe typed command messages while ingress retains its bounded JSON parser."""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

from fastapi.encoders import jsonable_encoder
from fastapi.openapi.models import Schema
from pydantic.json_schema import JsonSchemaValue, models_json_schema

from execution_plane_client.protocol import MessageEnvelope

if TYPE_CHECKING:
    from fastapi import FastAPI
    from pydantic import BaseModel

    from execution_plane_client.catalog import CommandCatalog, CommandDefinition
    from execution_plane_client.protocol import CapabilitiesRead


def _ref(name: str) -> str:
    return f"#/components/schemas/{name}"


def _replace_references(value: object, replacements: dict[str, str]) -> None:
    """Rewrite both schema references and discriminator mappings together."""
    if isinstance(value, dict):
        for key, child in value.items():
            if isinstance(child, str):
                value[key] = replacements.get(child, child)
            else:
                _replace_references(child, replacements)
    elif isinstance(value, list):
        for child in value:
            _replace_references(child, replacements)


def _command_schemas(
    incoming: list[CommandDefinition],
    completion: CommandDefinition,
    outbound: list[CommandDefinition],
) -> dict[str, JsonSchemaValue]:
    definitions = [*incoming, completion, *outbound]
    models: list[type[BaseModel]] = [MessageEnvelope, *(definition.payload_schema for definition in definitions)]
    references, document = models_json_schema(
        [(model, "validation") for model in models], ref_template="#/components/schemas/{model}"
    )
    schemas: dict[str, JsonSchemaValue] = document["$defs"]
    command_template = schemas["Command"]
    mapping: dict[str, str] = {}
    for definition in definitions:
        name = f"{definition.payload_schema.__name__.removesuffix('Command')}CommandV{definition.revision}"
        command = deepcopy(command_template)
        command["title"] = name
        command["properties"]["command_type"] = {"type": "string", "const": definition.command_type}
        command["properties"]["command_revision"] = {"type": "integer", "const": definition.revision}
        command["properties"]["payload"] = references[definition.payload_schema, "validation"]
        if definition.payload_schema.requires_parent:
            command["properties"]["parent_command_id"] = {"type": "string", "format": "uuid"}
            command["required"].append("parent_command_id")
        schemas[name] = command
        if definition not in outbound:
            mapping[definition.command_type] = _ref(name)
    completion_ref = mapping.pop(completion.command_type)
    schemas["Command"] = {
        "title": "Command",
        "description": "A command type and revision paired with its corresponding payload.",
        "oneOf": [{"$ref": reference} for reference in mapping.values()],
        "discriminator": {"propertyName": "command_type", "mapping": mapping},
    }
    completion_envelope = deepcopy(schemas["MessageEnvelope"])
    completion_envelope["title"] = "CompletionMessageEnvelope"
    completion_envelope["description"] = "Work completion sent to the configured caller's command ingress."
    completion_envelope["properties"]["command"] = {"$ref": completion_ref}
    schemas["CompletionMessageEnvelope"] = completion_envelope
    return schemas


def _merge_schemas(existing: dict[str, JsonSchemaValue], generated: dict[str, JsonSchemaValue]) -> dict[str, str]:
    """Share identical components and distinguish client and domain placement models."""
    normalized = {
        name: jsonable_encoder(Schema(**schema), by_alias=True, exclude_none=True) for name, schema in generated.items()
    }
    names = {
        name: f"Client{name}" if name in existing and existing[name] != schema else name
        for name, schema in normalized.items()
    }
    _replace_references(normalized, {_ref(name): _ref(mapped) for name, mapped in names.items()})
    for name, schema in normalized.items():
        existing[names[name]] = schema
    return names


def configure_command_openapi(
    app: FastAPI,
    catalog: CommandCatalog,
    capabilities: CapabilitiesRead,
    outbound: list[CommandDefinition],
) -> None:
    """Add reusable command components and the outbound completion webhook to OpenAPI."""
    incoming = [
        catalog.definition(kind, revision)
        for kind, revisions in capabilities.commands.items()
        for revision in revisions
    ]
    completion = catalog.definition("work.execute.completed", 1)
    original_openapi = app.openapi

    def command_openapi() -> JsonSchemaValue:
        if app.openapi_schema is not None:
            return app.openapi_schema
        document = original_openapi()
        names = _merge_schemas(document["components"]["schemas"], _command_schemas(incoming, completion, outbound))
        document["paths"]["/v1/commands"]["post"]["requestBody"] = {
            "required": True,
            "content": {"application/json": {"schema": {"$ref": _ref(names["MessageEnvelope"])}}},
        }
        document["webhooks"] = {
            "work.execute.completed": {
                "post": {
                    "summary": "Notify the caller of a work outcome",
                    "description": "EP sends this command to EP_COMPLETION_CALLBACK_URL. "
                    "The caller acknowledges it with a receipt containing the same message_id.",
                    "operationId": "notify_work_completion",
                    "requestBody": {
                        "required": True,
                        "content": {"application/json": {"schema": {"$ref": _ref(names["CompletionMessageEnvelope"])}}},
                    },
                    "responses": {
                        "2XX": {
                            "description": "The completion command was handled.",
                            "content": {"application/json": {"schema": {"$ref": _ref("TransportReceipt")}}},
                        },
                    },
                },
            },
        }
        return document

    app.openapi = command_openapi  # type: ignore[method-assign]
