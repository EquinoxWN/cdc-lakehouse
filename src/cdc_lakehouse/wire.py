"""Confluent wire format: magic byte 0, a 4-byte big-endian schema id, then the Avro body."""

from __future__ import annotations

import struct

MAGIC = 0


class WireFormatError(ValueError):
    """The bytes are not a framed Avro message."""


def split(message: bytes) -> tuple[int, bytes]:
    """Return (schema id, Avro payload) from one framed message."""
    if len(message) < 5:
        raise WireFormatError(f"message too short: {len(message)} bytes")
    if message[0] != MAGIC:
        raise WireFormatError(f"unknown magic byte {message[0]}")
    (schema_id,) = struct.unpack(">I", message[1:5])
    return schema_id, message[5:]


def frame(schema_id: int, payload: bytes) -> bytes:
    """Build a framed message; used by tests."""
    return bytes([MAGIC]) + struct.pack(">I", schema_id) + payload
