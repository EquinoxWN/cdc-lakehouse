# RFC 0001: cdc-lakehouse design

- **Status:** Accepted (M1 implemented)
- **Author:** AUTHOR_NAME
- **Created:** 2026

## Problem

Analytics teams want fresh data, but running reports against the production database slows
customer traffic, and nightly batch exports are a day stale. Change data capture reads the
database's own write-ahead log and streams every insert, update and delete elsewhere, with no
extra query load and no application changes. This project builds that pipeline into a queryable
lakehouse, seconds behind live.

## Goals

- **M1:** Debezium reads Postgres's WAL through logical replication (`pgoutput`) and emits one
  event per insert, update or delete into Kafka, with Avro schemas in a schema registry.
- **M2:** an Iceberg sink writes the changes into Iceberg tables on S3-compatible storage (MinIO),
  applying upserts and deletes by primary key; scheduled compaction and snapshot expiry.
- **M3:** Trino queries with time travel; a schema-evolution demo; a reconciliation job
  comparing row counts and checksums between source and lake; an end-to-end lag chart.

## Non-goals

- Changing the application or adding triggers; capture must be log-based.
- Exactly-once delivery into the lake in M1; Kafka's at-least-once plus primary-key upserts in
  M2 give the same end state.
- Running as a hosted production service. A Terraform AWS variant is optional later.

## Proposed design

![architecture](../architecture.png)

```
Postgres 16 (wal_level=logical, publication dbz_publication, role debezium: REPLICATION + SELECT)
   │ pgoutput logical replication slot dbz_shop
   ▼
Debezium Postgres connector (Kafka Connect 2.7.4)
   │ Avro, Confluent framing, schemas auto-registered ──► Apicurio Registry
   ▼
Kafka (KRaft, no ZooKeeper)  topics shop.public.customers, shop.public.orders
   ▼
M2: Iceberg sink ─► Iceberg on MinIO ─► M3: Trino
```

- **Least privilege:** Debezium connects as `debezium` with `REPLICATION` and `SELECT` only. The
  publication is created by `sql/init.sql`, not by the connector (`publication.autocreate.mode=disabled`).
- **Full before-images:** `REPLICA IDENTITY FULL`, so updates and deletes carry the old row.
- **Secrets:** the Debezium password reaches `init.sql` as a psql variable from the environment,
  and `SourceSettings` never prints it.
- **Idle slots:** `heartbeat.interval.ms` keeps the replication slot advancing so WAL does not pile
  up on quiet tables.
- **Proof:** unit tests for configuration, the Connect REST client, the wire format and event
  normalisation; `cdc_lakehouse.e2e` runs the whole stack in CI.

## Alternatives considered

| Option | Why not (yet) |
|---|---|
| Query-based polling (`updated_at > last`) | Misses deletes, loads the database, and cannot see intermediate updates. |
| Triggers writing to an audit table | Adds write latency and schema changes to the application database. |
| JSON instead of Avro | No enforced schema; consumers break silently when a column changes. |
| Confluent Schema Registry and converter | Needs a custom Connect image with extra jars. The Apicurio converter ships in the Debezium image and can emit Confluent framing (ADR 0002). |
| `wal2json` / `decoderbufs` plugins | Must be installed in the database server; `pgoutput` is built in since Postgres 10. |
| Kafka with ZooKeeper | Deprecated; KRaft is the default in Kafka 3.x and 4.0. |

## Measurement plan

- M1: event counts per operation and commit-to-consumer lag (p50, p95) from the `e2e` job.
- M3: end-to-end lag p95 chart from Postgres commit to Trino visibility, schema-evolution demo,
  and a reconciliation report showing zero drift.

## Milestones

- **M1 (done):** compose stack, least-privilege replication, Debezium with Avro and registry, unit
  tests, end-to-end check in CI.
- **M2:** Iceberg sink with upserts and deletes on MinIO; compaction and snapshot expiry.
- **M3:** Trino with time travel, schema evolution, reconciliation, lag chart.

## Risks and open questions

- A stopped connector keeps its replication slot, and Postgres retains WAL until disk fills.
  M2 adds slot-lag monitoring and `max_slot_wal_keep_size`.
- `REPLICA IDENTITY FULL` makes updates on wide tables write more WAL; revisit per table.
- The local registry is in-memory; restarting it loses schema ids that existing messages refer to.
