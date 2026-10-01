"""Normalise Debezium change envelopes and measure end-to-end lag."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

OPS = {
    "c": "insert",
    "u": "update",
    "d": "delete",
    "r": "snapshot",
    "t": "truncate",
    "m": "message",
}


@dataclass(frozen=True)
class ChangeEvent:
    """One row change as analytics code wants to see it."""

    table: str
    op: str
    key: Mapping[str, Any] | None
    before: Mapping[str, Any] | None
    after: Mapping[str, Any] | None
    lsn: int | None
    source_ts_ms: int
    emitted_ts_ms: int


def from_envelope(
    key: Mapping[str, Any] | None, value: Mapping[str, Any] | None
) -> ChangeEvent | None:
    """Turn a decoded Debezium key/value pair into a ChangeEvent; None for delete tombstones."""
    if value is None:
        return None  # Kafka tombstone that follows a delete, for log compaction
    op = value.get("op")
    if op not in OPS:
        raise ValueError(f"not a Debezium change envelope (op={op!r})")
    source = value.get("source") or {}
    return ChangeEvent(
        table=f"{source.get('schema', '?')}.{source.get('table', '?')}",
        op=OPS[op],
        key=key,
        before=value.get("before"),
        after=value.get("after"),
        lsn=source.get("lsn"),
        source_ts_ms=int(source.get("ts_ms", 0)),
        emitted_ts_ms=int(value.get("ts_ms", 0)),
    )


def percentile(values: Iterable[float], p: float) -> float:
    """Nearest-rank percentile of values, 0 < p <= 100."""
    data = sorted(values)
    if not data:
        raise ValueError("no values")
    if not 0 < p <= 100:
        raise ValueError("p must be in (0, 100]")
    return data[max(0, math.ceil(p / 100 * len(data)) - 1)]


def lag_ms(event: ChangeEvent, consumed_ts_ms: int) -> int:
    """Commit-to-consumer lag; snapshot rows are excluded by callers because they are not live."""
    return consumed_ts_ms - event.source_ts_ms
