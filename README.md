Aegis Banking
=============

Aegis-Banking is a demonstrator banking system composed of:

- gateway: Java Spring Boot (API entrypoint)
- ledger-core: Go gRPC service (ledger + persistence)
- observability: OpenTelemetry Collector, Prometheus, Grafana, Jaeger (dev stack)

This repository contains the service sources, observability configurations and a Docker Compose file to run the observability stack locally.

Quick start (development)
-------------------------

Requirements
- Docker & Docker Compose
- Java 21 (for running the gateway locally)
- Go (if running ledger-core locally)

Run the observability stack (from the repository root):

```bash
docker compose up -d jaeger otel-collector prometheus grafana
```

Run services (choose local or containerized)

- Run gateway locally (recommended for iterative development):

```bash
cd gateway
./gradlew clean bootRun
```

- Run ledger-core locally:

```bash
cd ledger-core
go run ./cmd/server/main.go
```

UIs
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (default: admin/admin)
- Jaeger: http://localhost:16686

Observability
-------------

- Prometheus is configured (observability/prometheus/prometheus.yml) to scrape metrics from the host for services run locally and from container hostnames when services are containerized.
- The collector configuration is at `observability/otel-collector/otel-collector.yaml` and exposes OTLP receivers used by the services.

Configuration notes
-------------------

- The gateway exposes Prometheus metrics via Spring Actuator at `/actuator/prometheus` when management is enabled (default port 2112 for local runs).
- The ledger-core exposes Prometheus metrics on port 2113 when run locally.

Troubleshooting highlights
-------------------------

- If metrics do not appear in Grafana/Prometheus, check Prometheus targets at http://localhost:9090/targets and ensure the scraped endpoints return 200.
- For traces, open Jaeger (http://localhost:16686) and search by service name.
- When running a mixed local + Docker environment, Prometheus resolves host services using `host.docker.internal` (configured in `docker-compose.yaml`).

Repository layout (top-level)
---------------------------

- gateway/        Java Spring Boot service
- ledger-core/    Go gRPC ledger service
- observability/  Prometheus, Grafana, OTel Collector configurations
- docker-compose.yaml    Services to run the observability stack (and optional service entries)

Contributing
------------

Please open issues or PRs for bugs, improvements or documentation updates. If you change the observability stack, keep YAML schema compatibility with the collector image tag pinned in `docker-compose.yaml`.

License
-------

This repository is provided for demonstration and development purposes.
