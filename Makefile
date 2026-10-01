.PHONY: setup lint test up down e2e bench ci audit

setup:
	python -m pip install -e ".[dev]"

lint:
	python -m ruff check .
	python -m ruff format --check .

# Unit tests: connector config, Connect REST handling, wire format, Debezium envelopes.
test:
	python -m pytest -q

# Start Postgres, Kafka (KRaft), the schema registry and Debezium Connect (needs Docker).
up:
	docker compose up -d --wait postgres kafka registry
	docker compose up -d connect

down:
	docker compose down -v

# End-to-end: register the connector, change rows, consume and decode the Avro events.
e2e:
	python -m pip install -e ".[stream]"
	python -m cdc_lakehouse.e2e

bench: e2e

# Known vulnerabilities in the installed dependencies.
audit:
	python -m pip_audit --skip-editable

ci: setup lint test
