"""Wire-format framing and Debezium envelope handling."""

import pytest

from cdc_lakehouse import wire
from cdc_lakehouse.events import from_envelope, lag_ms, percentile


def test_frame_roundtrip():
    assert wire.split(wire.frame(42, b"\x02\x04")) == (42, b"\x02\x04")


def test_schema_id_is_big_endian():
    assert wire.split(b"\x00\x00\x00\x01\x00rest") == (256, b"rest")


@pytest.mark.parametrize("bad", [b"", b"\x00\x00\x01", b"\x01\x00\x00\x00\x01payload"])
def test_rejects_unframed_bytes(bad):
    with pytest.raises(wire.WireFormatError):
        wire.split(bad)


def envelope(op, before=None, after=None, ts=1_000):
    return {
        "op": op,
        "before": before,
        "after": after,
        "source": {"schema": "public", "table": "customers", "lsn": 123, "ts_ms": ts},
        "ts_ms": ts + 50,
    }


@pytest.mark.parametrize(
    ("op", "name"), [("c", "insert"), ("u", "update"), ("d", "delete"), ("r", "snapshot")]
)
def test_operations_are_named(op, name):
    event = from_envelope({"id": 1}, envelope(op, after={"id": 1}))
    assert event.op == name
    assert event.table == "public.customers"
    assert event.lsn == 123


def test_update_keeps_before_and_after_images():
    event = from_envelope(
        {"id": 1}, envelope("u", before={"name": "Ada"}, after={"name": "Ada Lovelace"})
    )
    assert event.before == {"name": "Ada"}
    assert event.after == {"name": "Ada Lovelace"}


def test_delete_tombstone_is_skipped():
    assert from_envelope({"id": 1}, None) is None


def test_non_debezium_value_is_rejected():
    with pytest.raises(ValueError, match="not a Debezium change envelope"):
        from_envelope(None, {"hello": "world"})


def test_lag_is_consumer_time_minus_commit_time():
    event = from_envelope({"id": 1}, envelope("c", after={"id": 1}, ts=10_000))
    assert lag_ms(event, 10_750) == 750


def test_percentile_nearest_rank():
    data = list(range(1, 101))
    assert percentile(data, 50) == 50
    assert percentile(data, 95) == 95
    assert percentile(data, 100) == 100
    assert percentile([7], 95) == 7


def test_percentile_rejects_bad_input():
    with pytest.raises(ValueError, match="no values"):
        percentile([], 50)
    with pytest.raises(ValueError, match="p must be in"):
        percentile([1], 0)
