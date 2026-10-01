# ADR 0002: Apicurio schema registry, Confluent wire format

- **Status:** Accepted

## Context

Events must carry Avro schemas from a registry so consumers know the exact shape of every
version. The official Debezium Connect image ships the Apicurio Avro converter but not
Confluent's; adding Confluent's means building a custom image with a dozen extra jars. At the
same time, most Kafka tooling (and the Iceberg sink in M2) expects the Confluent wire format:
a zero byte, a 4-byte schema id, then the Avro body.

## Decision

- Use Apicurio Registry (in-memory, local) and the Apicurio converter bundled with
  `quay.io/debezium/connect` (`ENABLE_APICURIO_CONVERTERS=true`). No custom image.
- Set `apicurio.registry.as-confluent=true` and `use-id=contentId`, so messages use the Confluent
  framing, and consumers resolve schemas by content id from the registry.
- `decimal.handling.mode=string`, so money amounts survive every consumer exactly.

## Consequences

- No custom image to build, scan or keep patched.
- Consumers need only the standard framing (`cdc_lakehouse.wire`) and one registry lookup.
- The M2 Iceberg sink must use the same converter settings, or a Confluent-compatible API
  (`/apis/ccompat/v7`) with matching ids. That is verified when the sink lands.
- The in-memory registry forgets schemas on restart; fine locally, not for the AWS variant.
