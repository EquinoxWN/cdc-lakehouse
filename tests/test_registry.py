import io

import fastavro
import pytest

from cdc_lakehouse.registry import load_schema

SOURCE = {
    "type": "record",
    "name": "Source",
    "namespace": "io.debezium.connector.postgresql",
    "fields": [{"name": "ts_ms", "type": "long"}, {"name": "pos", "type": "io.debezium.Position"}],
}
POSITION = {
    "type": "record",
    "name": "Position",
    "namespace": "io.debezium",
    "fields": [{"name": "lsn", "type": "long"}],
}
VALUE = {
    "type": "record",
    "name": "Value",
    "namespace": "shop.public.customers",
    "fields": [{"name": "id", "type": "int"}, {"name": "email", "type": "string"}],
}
ENVELOPE = {
    "type": "record",
    "name": "Envelope",
    "namespace": "shop.public.customers",
    "fields": [
        {"name": "before", "type": ["null", "shop.public.customers.Value"], "default": None},
        {"name": "after", "type": ["null", "shop.public.customers.Value"], "default": None},
        {"name": "source", "type": "io.debezium.connector.postgresql.Source"},
        {"name": "op", "type": "string"},
    ],
}


def ref(artifact: str, name: str) -> dict:
    return {"groupId": None, "artifactId": artifact, "version": "1", "name": name}


# What Apicurio returns for each path: the envelope references Value and Source, Source references Position.
REGISTRY = {
    "/ids/contentIds/3": ENVELOPE,
    "/ids/contentIds/3/references": [
        ref("shop.public.customers.Value", "shop.public.customers.Value"),
        ref("io.debezium.connector.postgresql.Source", "io.debezium.connector.postgresql.Source"),
    ],
    "/groups/default/artifacts/shop.public.customers.Value/versions/1": VALUE,
    "/groups/default/artifacts/shop.public.customers.Value/versions/1/references": [],
    "/groups/default/artifacts/io.debezium.connector.postgresql.Source/versions/1": SOURCE,
    "/groups/default/artifacts/io.debezium.connector.postgresql.Source/versions/1/references": [
        ref("io.debezium.Position", "io.debezium.Position")
    ],
    "/groups/default/artifacts/io.debezium.Position/versions/1": POSITION,
    "/groups/default/artifacts/io.debezium.Position/versions/1/references": [],
}


def test_the_outer_schema_alone_cannot_be_parsed():
    with pytest.raises(fastavro.schema.UnknownType):
        fastavro.parse_schema(ENVELOPE)


def test_references_are_resolved_and_a_debezium_event_decodes():
    schema = load_schema(REGISTRY.__getitem__, 3)
    event = {
        "before": None,
        "after": {"id": 1, "email": "ada@example.com"},
        "source": {"ts_ms": 7, "pos": {"lsn": 42}},
        "op": "c",
    }
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, event)
    buf.seek(0)
    assert fastavro.schemaless_reader(buf, schema) == event


def test_a_type_referenced_twice_is_fetched_once():
    calls = []
    registry = dict(REGISTRY)
    registry["/ids/contentIds/3/references"] = REGISTRY["/ids/contentIds/3/references"] * 2

    def get(path):
        calls.append(path)
        return registry[path]

    load_schema(get, 3)
    assert calls.count("/groups/default/artifacts/shop.public.customers.Value/versions/1") == 1


def test_reference_cycles_are_refused():
    loop = {
        "/ids/contentIds/1/references": [ref("a", "a")],
        "/groups/default/artifacts/a/versions/1/references": [ref("a", "b")],
    }
    with pytest.raises(ValueError, match="deeper than 32"):
        load_schema(loop.__getitem__, 1)
