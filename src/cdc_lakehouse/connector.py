"""Build and register the Debezium Postgres source connector through the Kafka Connect REST API."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

AVRO_CONVERTER = "io.apicurio.registry.utils.converter.AvroConverter"


@dataclass(frozen=True)
class SourceSettings:
    """Where the source database is and how events are named and encoded."""

    db_host: str = "postgres"
    db_port: int = 5432
    db_name: str = "shop"
    db_user: str = "debezium"
    db_password: str = ""
    topic_prefix: str = "shop"
    tables: tuple[str, ...] = ("public.customers", "public.orders")
    publication: str = "dbz_publication"
    slot: str = "dbz_shop"
    registry_url: str = "http://registry:8080/apis/registry/v2"

    def __repr__(self) -> str:  # never print the password
        return f"SourceSettings(db={self.db_user}@{self.db_host}:{self.db_port}/{self.db_name}, topics={self.topic_prefix}.*)"


def _converter(prefix: str, registry_url: str) -> dict[str, str]:
    """Avro converter settings: auto-register schemas, Confluent wire format, content ids."""
    return {
        prefix: AVRO_CONVERTER,
        f"{prefix}.apicurio.registry.url": registry_url,
        f"{prefix}.apicurio.registry.auto-register": "true",
        f"{prefix}.apicurio.registry.find-latest": "true",
        f"{prefix}.apicurio.registry.as-confluent": "true",
        f"{prefix}.apicurio.registry.use-id": "contentId",
        # Put magic byte + schema id in the payload (Confluent framing), not in Kafka record headers.
        f"{prefix}.apicurio.registry.headers.enabled": "false",
    }


def connector_config(s: SourceSettings) -> dict[str, str]:
    """Return the connector configuration for Kafka Connect."""
    if not s.db_password:
        raise ValueError("db_password is required")
    if not s.tables:
        raise ValueError("at least one table is required")
    config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": s.db_host,
        "database.port": str(s.db_port),
        "database.user": s.db_user,
        "database.password": s.db_password,
        "database.dbname": s.db_name,
        "topic.prefix": s.topic_prefix,
        "table.include.list": ",".join(s.tables),
        # pgoutput is built into Postgres 10+; no server plugin to install.
        "plugin.name": "pgoutput",
        "publication.name": s.publication,
        "publication.autocreate.mode": "disabled",  # the DBA owns the publication (sql/init.sql)
        "slot.name": s.slot,
        "snapshot.mode": "initial",
        "tombstones.on.delete": "true",
        "decimal.handling.mode": "string",
        "heartbeat.interval.ms": "10000",  # keeps the slot advancing on idle tables
    }
    config.update(_converter("key.converter", s.registry_url))
    config.update(_converter("value.converter", s.registry_url))
    return config


class ConnectError(RuntimeError):
    """Kafka Connect rejected a request or the connector failed."""


def http_url(url: str) -> str:
    """Return the URL if it is http(s); refuse file: and other schemes."""
    if urllib.parse.urlsplit(url).scheme not in {"http", "https"}:
        raise ValueError(f"only http(s) URLs are allowed, got {url!r}")
    return url


def _request(method: str, url: str, body: dict[str, Any] | None = None, timeout: float = 10) -> Any:
    """Send one JSON request and return the decoded response."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(  # noqa: S310 (scheme checked by http_url)
        http_url(url), data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (scheme checked)
            text = resp.read().decode()
            return json.loads(text) if text else None
    except urllib.error.HTTPError as err:
        raise ConnectError(
            f"{method} {url} -> {err.code}: {err.read().decode(errors='replace')}"
        ) from err


def register(connect_url: str, name: str, config: dict[str, str]) -> None:
    """Create or update the connector; PUT on /config is idempotent."""
    _request("PUT", f"{connect_url.rstrip('/')}/connectors/{name}/config", config)


def wait_running(
    connect_url: str, name: str, timeout_s: float = 120, poll_s: float = 2
) -> dict[str, Any]:
    """Block until the connector and all tasks are RUNNING; fail fast on FAILED."""
    deadline = time.monotonic() + timeout_s
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            last = _request("GET", f"{connect_url.rstrip('/')}/connectors/{name}/status")
        except (ConnectError, OSError):
            time.sleep(poll_s)
            continue
        states = [last["connector"]["state"]] + [t["state"] for t in last.get("tasks", [])]
        if "FAILED" in states:
            raise ConnectError(f"connector {name} failed: {json.dumps(last)[:2000]}")
        if last.get("tasks") and all(s == "RUNNING" for s in states):
            return last
        time.sleep(poll_s)
    raise ConnectError(f"connector {name} not running after {timeout_s}s: {last}")
