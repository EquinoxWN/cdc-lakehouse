"""Load Avro schemas from an Apicurio registry, including the types they reference.

Debezium's records nest named types (for example io.debezium.connector.postgresql.Source). The
Apicurio converter registers each named type as its own artifact and stores the outer schema with
references to them, so a schema fetched on its own cannot be parsed until its references are.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from urllib.parse import quote

GetJson = Callable[[str], Any]


def _version_path(ref: dict[str, Any]) -> str:
    """Registry path of one referenced artifact version."""
    group = quote(ref.get("groupId") or "default", safe="")
    artifact = quote(ref["artifactId"], safe="")
    return f"/groups/{group}/artifacts/{artifact}/versions/{quote(str(ref['version']), safe='')}"


def _define_references(
    get_json: GetJson, refs_path: str, named: dict[str, Any], depth: int
) -> None:
    """Parse every referenced schema, deepest first, into named."""
    import fastavro

    if depth > 32:
        raise ValueError("schema references nest deeper than 32 levels")
    for ref in get_json(refs_path) or []:
        if ref.get("name") in named:
            continue
        path = _version_path(ref)
        _define_references(get_json, f"{path}/references", named, depth + 1)
        fastavro.parse_schema(get_json(path), named_schemas=named)


def load_schema(get_json: GetJson, content_id: int) -> Any:
    """Parsed schema for a content id, with its referenced types defined."""
    import fastavro

    named: dict[str, Any] = {}
    _define_references(get_json, f"/ids/contentIds/{content_id}/references", named, 0)
    return fastavro.parse_schema(get_json(f"/ids/contentIds/{content_id}"), named_schemas=named)
