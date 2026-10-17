# Implementation Plan: Three-Vantage Kafka Dev Performance Benchmark

**Branch**: `feat/002-k8s-dev-perf-benchmark` | **Date**: 2026-10-17 | **Spec**: `specs/002-k8s-dev-perf-benchmark/spec.md`

## Summary

Build an inventory-driven 3-vantage benchmark tool under `kafka_realtime_check/` (`benchmark_kafka_vantages.sh` + `lib/benchmark_parse.py` + `lib/test_benchmark_parse.py`) that provisions `namespace=kafkausage-test` and two Kafka client pods (`kafka-producer-client`, `kafka-consumer-client`) on Kubernetes `Dev`, stages SASL credentials via a Kubernetes Secret without printing secrets, and benchmarks:
1. Topic list latency (`kafka-topics.sh --list` across 3 runs + native metadata probe where available).
2. Concurrent produce (`kafkausage-bench-write`) and consume (`kafkausage-bench-read`) using `kafka-producer-perf-test.sh` and `kafka-consumer-perf-test.sh` across Kubernetes (`kafkausage-test`), operator laptop, and broker VM (`10.104.120.17`).

## Technical Context

- **Language/Version**: Bash + Python 3 (stdlib only, matching `kafka_mirrormaker/lib/sync_parse.py`)
- **Primary Dependencies**: `kubectl`, `ssh`, Kafka CLI (`kafka-topics.sh`, `kafka-producer-perf-test.sh`, `kafka-consumer-perf-test.sh`), `python3`
- **Target Cluster**: `config/clusters/devkafka.env` (`10.104.120.17:9094`, `SASL_PLAINTEXT`, `SCRAM-SHA-512`)
- **Kubernetes Target**: Context `Dev`, namespace `kafkausage-test`, image `dockerhub.charisma.tech/bitnami/kafka:3.4.0-debian-11-r15`
- **Testing**: `python3 -m unittest discover -s kafka_realtime_check/lib -p 'test_*.py'`

## Constitution Check

- **I. Inventory-Driven Operations**: PASS — reads `KAFKA_BOOTSTRAP`, `BROKER_HOSTS`, `SSH_PORT`, `KAFKA_BIN`, `KAFKA_COMMAND_CONFIG` from `-c <inventory.env>`.
- **II. Dry-Run Default and Safe Mutation**: PASS — prints planned actions unless `--apply -y` is provided.
- **III. Secrets and Dual-Remote Hygiene**: PASS — `admin.properties` piped directly over SSH into K8s Secret and `0600` temp file on laptop; never echoed or committed.
- **IV. Spec-First Delivery**: PASS — `.specify/feature.json` and `specs/002-k8s-dev-perf-benchmark/{spec,plan,tasks}.md` created before code.
- **V. Verification Before Done**: PASS — unit tests in `kafka_realtime_check/lib/test_benchmark_parse.py` + `graphify update .`.

## Project Structure

```text
kafka_realtime_check/
├── benchmark_kafka_vantages.sh
└── lib/
    ├── benchmark_parse.py
    └── test_benchmark_parse.py
specs/002-k8s-dev-perf-benchmark/
├── spec.md
├── plan.md
└── tasks.md
```
