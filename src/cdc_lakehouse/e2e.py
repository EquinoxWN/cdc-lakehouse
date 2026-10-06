"""End-to-end check against the running compose stack.

Registers the connector, changes rows in Postgres, consumes the Avro events from Kafka, decodes
them with schemas fetched from the registry, and fails unless every expected change arrives.
Needs the `stream` extra (psycopg, confluent-kafka, fastavro) and `docker compose up`.

Usage:
    python -m cdc_lakehouse.e2e
"""

from __future__ import annotations

import io
import json
import os
import sys
import time
import urllib.request
from collections import Counter
from functools import cache
from typing import Any

from cdc_lakehouse import connector, wire
from cdc_lakehouse.events import ChangeEvent, from_envelope, lag_ms, percentile

CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
REGISTRY_URL = os.environ.get("REGISTRY_URL", "http://localhost:8080/apis/registry/v2")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
PG_DSN = os.environ.get("PG_DSN", "postgresql://postgres:postgres@localhost:5432/shop")
TOPIC = "shop.public.customers"


@cache
def _schema(content_id: int) -> Any:
    """Fetch and parse the Avro schema for a content id."""
    import fastavro

    return fastavro.parse_schema(_get_json(f"{REGISTRY_URL}/ids/contentIds/{content_id}"))


def _get_json(url: str) -> Any:
    """GET an http(s) URL and decode its JSON body."""
    with urllib.request.urlopen(connector.http_url(url), timeout=10) as resp:  # noqa: S310 (scheme checked)
        return json.loads(resp.read())


def _decode(message: bytes | None) -> dict[str, Any] | None:
    """Decode one framed Avro message, or pass a tombstone through."""
    if message is None:
        return None
    import fastavro

    schema_id, payload = wire.split(message)
    return fastavro.schemaless_reader(io.BytesIO(payload), _schema(schema_id))


def _make_changes() -> None:
    """Insert, update and delete customers so every live operation type is produced."""
    import psycopg

    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        conn.execute("INSERT INTO customers (email, name) VALUES ('grace@example.com', 'Grace')")
        conn.execute(
            "UPDATE customers SET name = 'Ada Lovelace', updated_at = now() WHERE email = 'ada@example.com'"
        )
        conn.execute("INSERT INTO customers (email, name) VALUES ('temp@example.com', 'Temp')")
        conn.execute("DELETE FROM customers WHERE email = 'temp@example.com'")


def annotation(level: str, title: str, text: str) -> str:
    """A GitHub Actions workflow command that shows text as a run annotation."""
    body = text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    title = title.replace("%", "%25").replace(",", "%2C").replace("::", ": :")
    return f"::{level} title={title}::{body}"


def _wait_for_topic(topic: str, timeout_s: float) -> None:
    """Block until the snapshot has created the topic."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if topic in admin.list_topics(timeout=5).topics:
            return
        time.sleep(1)
    raise TimeoutError(f"topic {topic} was not created within {timeout_s}s")


def _consume(
    expected: Counter[str], timeout_s: float, problems: list[str]
) -> tuple[list[ChangeEvent], list[int]]:
    """Read the customers topic from the start until the expected op counts are reached."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"e2e-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            # A topic created after subscribing is otherwise only noticed every 5 minutes.
            "topic.metadata.refresh.interval.ms": 2000,
        }
    )
    consumer.subscribe([TOPIC])
    events: list[ChangeEvent] = []
    lags: list[int] = []
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                problems.append(f"consumer error: {msg.error()}")
                continue
            try:
                event = from_envelope(_decode(msg.key()), _decode(msg.value()))
            except (ValueError, KeyError) as err:  # report, keep reading
                value = msg.value() or b""
                headers = [h[0] for h in msg.headers() or []]
                problems.append(
                    f"{type(err).__name__}: {err} (first bytes {value[:5].hex()}, headers {headers})"
                )
                continue
            if event is None:
                continue
            events.append(event)
            if event.op != "snapshot":
                lags.append(lag_ms(event, int(time.time() * 1000)))
            if not expected - Counter(e.op for e in events):
                break
    finally:
        consumer.close()
    return events, lags


def main() -> int:
    """Run the check, report it, and turn any crash into a readable annotation in CI."""
    try:
        return _check()
    except Exception as err:
        if os.environ.get("GITHUB_ACTIONS"):
            print(annotation("error", "e2e crashed", f"{type(err).__name__}: {err}"))
        raise


def _check() -> int:
    """Run the check and print a Markdown report; exit 1 on failure."""
    settings = connector.SourceSettings(
        db_password=os.environ.get("DEBEZIUM_PASSWORD", "debezium"),
        registry_url="http://registry:8080/apis/registry/v2",
    )
    connector.register(CONNECT_URL, "shop-postgres", connector.connector_config(settings))
    status = connector.wait_running(CONNECT_URL, "shop-postgres")
    _wait_for_topic(TOPIC, timeout_s=90)
    _make_changes()
    expected = Counter({"snapshot": 2, "insert": 2, "update": 1, "delete": 1})
    problems: list[str] = []
    events, lags = _consume(expected, timeout_s=120, problems=problems)
    got = Counter(e.op for e in events)

    artifacts = _get_json(f"{REGISTRY_URL}/search/artifacts?limit=100")["count"]

    ok = not (expected - got) and artifacts > 0
    lines = [
        "## CDC end-to-end check",
        "",
        "| Check | Result |",
        "|---|---|",
        f"| Connector state | {status['connector']['state']} ({len(status['tasks'])} task) |",
        f"| Events by operation | {dict(sorted(got.items()))} |",
        f"| Expected at least | {dict(sorted(expected.items()))} |",
        f"| Avro schemas in registry | {artifacts} |",
        f"| Live-change lag p50 / p95 | {percentile(lags, 50) if lags else 'n/a'} ms / {percentile(lags, 95) if lags else 'n/a'} ms |",
        f"| Messages that could not be decoded | {len(problems)}{' (first: ' + problems[0] + ')' if problems else ''} |",
        f"| Result | {'PASS' if ok else 'FAIL'} |",
    ]
    report = "\n".join(lines)
    print(report)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a", encoding="utf-8") as f:
            f.write(report + "\n")
    if os.environ.get("GITHUB_ACTIONS"):  # annotations are readable without signing in, unlike logs
        print(annotation("notice" if ok else "error", "CDC end-to-end check", report))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
