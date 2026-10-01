"""Connector configuration and registration against a fake Kafka Connect REST server."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import ClassVar

import pytest

from cdc_lakehouse import connector

SETTINGS = connector.SourceSettings(db_password="s3cret")


def test_config_uses_pgoutput_and_existing_publication():
    cfg = connector.connector_config(SETTINGS)
    assert cfg["connector.class"] == "io.debezium.connector.postgresql.PostgresConnector"
    assert cfg["plugin.name"] == "pgoutput"
    assert cfg["publication.name"] == "dbz_publication"
    assert cfg["publication.autocreate.mode"] == "disabled"
    assert cfg["table.include.list"] == "public.customers,public.orders"
    assert cfg["topic.prefix"] == "shop"


@pytest.mark.parametrize("side", ["key", "value"])
def test_both_sides_use_avro_with_registry(side):
    cfg = connector.connector_config(SETTINGS)
    assert cfg[f"{side}.converter"] == connector.AVRO_CONVERTER
    assert cfg[f"{side}.converter.apicurio.registry.url"].endswith("/apis/registry/v2")
    assert cfg[f"{side}.converter.apicurio.registry.auto-register"] == "true"
    assert cfg[f"{side}.converter.apicurio.registry.as-confluent"] == "true"


def test_all_values_are_strings():
    """Kafka Connect expects string values in the REST payload."""
    assert all(isinstance(v, str) for v in connector.connector_config(SETTINGS).values())


def test_missing_password_is_rejected():
    with pytest.raises(ValueError, match="password"):
        connector.connector_config(connector.SourceSettings())


def test_repr_hides_password():
    assert "s3cret" not in repr(SETTINGS)


class FakeConnect(BaseHTTPRequestHandler):
    """Records PUTs and answers status requests with a scripted sequence."""

    puts: ClassVar[list[tuple[str, dict]]] = []
    statuses: ClassVar[list[dict]] = []

    def do_PUT(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        FakeConnect.puts.append((self.path, body))
        self._reply(201, {"name": "shop-postgres"})

    def do_GET(self):
        status = (
            FakeConnect.statuses.pop(0)
            if len(FakeConnect.statuses) > 1
            else FakeConnect.statuses[0]
        )
        self._reply(200, status)

    def _reply(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


@pytest.fixture
def fake_connect():
    FakeConnect.puts, FakeConnect.statuses = [], []
    server = HTTPServer(("127.0.0.1", 0), FakeConnect)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


def status(connector_state, *task_states):
    return {
        "connector": {"state": connector_state},
        "tasks": [{"id": i, "state": s} for i, s in enumerate(task_states)],
    }


def test_register_puts_config_idempotently(fake_connect):
    cfg = connector.connector_config(SETTINGS)
    connector.register(fake_connect, "shop-postgres", cfg)
    connector.register(fake_connect, "shop-postgres", cfg)
    assert [p for p, _ in FakeConnect.puts] == ["/connectors/shop-postgres/config"] * 2
    assert FakeConnect.puts[0][1] == cfg


def test_wait_running_polls_until_tasks_run(fake_connect):
    FakeConnect.statuses = [
        status("RUNNING"),
        status("RUNNING", "UNASSIGNED"),
        status("RUNNING", "RUNNING"),
    ]
    result = connector.wait_running(fake_connect, "shop-postgres", timeout_s=10, poll_s=0.01)
    assert result["tasks"][0]["state"] == "RUNNING"


def test_wait_running_fails_fast_on_failed_task(fake_connect):
    FakeConnect.statuses = [status("RUNNING", "FAILED")]
    with pytest.raises(connector.ConnectError, match="failed"):
        connector.wait_running(fake_connect, "shop-postgres", timeout_s=10, poll_s=0.01)


def test_wait_running_times_out(fake_connect):
    FakeConnect.statuses = [status("RUNNING", "UNASSIGNED")]
    with pytest.raises(connector.ConnectError, match="not running"):
        connector.wait_running(fake_connect, "shop-postgres", timeout_s=0.2, poll_s=0.01)


@pytest.mark.parametrize("url", ["file:///etc/passwd", "ftp://example.com/x", "localhost:8083"])
def test_only_http_urls_are_requested(url):
    with pytest.raises(ValueError, match="only http"):
        connector.register(url, "shop-postgres", {})
