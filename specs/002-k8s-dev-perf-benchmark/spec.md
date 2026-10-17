# Feature Specification: Three-Vantage Kafka Dev Performance Benchmark

**Feature Branch**: `feat/002-k8s-dev-perf-benchmark`

**Created**: 2026-10-17

**Status**: Approved

**Input**: Create `namespace=kafkausage-test` on Kubernetes Dev, deploy a producer and consumer pod targeting `devkafka` (`10.104.120.17:9094`), measure topic listing latency, produce data to one topic while concurrently consuming from another topic, and compare performance across three vantages: Kubernetes (`kafkausage-test`), local laptop, and the Kafka broker VM itself (`10.104.120.17`). Retain `kafkausage-test` and test topics after execution.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Three-Vantage Topic Listing Latency (Priority: P1)

An operator wants to know how long listing all Kafka topics takes from inside a Kubernetes Dev pod (`namespace=kafkausage-test`), from the operator laptop, and directly on the Kafka broker VM, separating pure metadata RTT from JVM CLI startup overhead.

**Why this priority**: Establishes baseline control-plane connectivity, SASL authentication latency, and metadata fetch time before running data-plane load.

**Independent Test**: Run `benchmark_kafka_vantages.sh -c config/clusters/devkafka.env --apply -y` (or dry-run without `--apply`) and verify topic count plus cold/warm/3-run timing table for all three vantages.

**Acceptance Scenarios**:

1. **Given** `devkafka.env` inventory and Kubernetes context `Dev`, **When** topic-list benchmark runs across K8s pod, laptop, and broker VM, **Then** each vantage reports total topic count and min/avg/max wall-clock latency in milliseconds.
2. **Given** `kcat` on the laptop, **When** metadata query runs, **Then** pure native protocol metadata RTT is reported alongside JVM `kafka-topics.sh --list` time.

---

### User Story 2 - Concurrent Produce & Consume on Separate Topics Across Three Vantages (Priority: P1)

An operator runs a concurrent workload: a producer writes a batch of records to a write topic (`kafkausage-bench-write`) while a consumer simultaneously reads a pre-seeded batch of records from a read topic (`kafkausage-bench-read`), plus an end-to-end stream check, comparing throughput (records/sec, MB/sec) and latency (avg, max, p50, p95, p99, p99.9) across K8s (`kafkausage-test`), laptop, and broker VM.

**Why this priority**: Measures realistic simultaneous read/write data-plane capacity and network/hop overhead between K8s worker nodes (`10.104.142.x`), laptop VPN/LAN path, and local broker loopback.

**Independent Test**: Feed sample `kafka-producer-perf-test.sh` and `kafka-consumer-perf-test.sh` outputs into `benchmark_parse.py` unit tests; execute live 3-vantage run and verify structured JSON/table metrics.

**Acceptance Scenarios**:

1. **Given** `namespace=kafkausage-test` on Kubernetes `Dev` with `kafka-producer-client` and `kafka-consumer-client` pods, **When** concurrent produce+consume runs, **Then** producer writes to `kafkausage-bench-write` and consumer reads from `kafkausage-bench-read` simultaneously and emits full throughput/latency metrics.
2. **Given** `--keep` flag (or default retention per operator request), **When** benchmark completes, **Then** `namespace=kafkausage-test`, client pods, and benchmark topics remain available for manual inspection.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: `benchmark_kafka_vantages.sh` MUST read cluster connection parameters from inventory (`-c config/clusters/*.env`) without hardcoding broker IPs or credentials in source code.
- **FR-002**: `benchmark_kafka_vantages.sh` MUST default to dry-run and only mutate Kubernetes or Kafka state when `--apply -y` is passed (Constitution Principle II).
- **FR-003**: `benchmark_parse.py` MUST parse both `kafka-producer-perf-test.sh` summary lines (throughput records/s, MB/s, avg/max/p50/p95/p99/p99.9 latency) and `kafka-consumer-perf-test.sh` CSV lines (MB/s, records/s, rebalance_ms, fetch_ms) and be covered by unit tests (Constitution Principle V).
- **FR-004**: Credentials from `KAFKA_COMMAND_CONFIG` MUST be staged securely (`0600` temp file / K8s Secret) and MUST NEVER be printed or committed (Constitution Principle III).
- **FR-005**: The runner MUST support `--keep` so `namespace=kafkausage-test`, producer/consumer pods, and test topics remain intact after benchmarking.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Unit tests in `kafka_realtime_check/lib/test_benchmark_parse.py` pass (`python3 -m unittest`).
- **SC-002**: `namespace=kafkausage-test` is active on Kubernetes `Dev` with running `kafka-producer-client` and `kafka-consumer-client` pods.
- **SC-003**: Complete 3-vantage comparison report covering topic list latency and concurrent produce/consume throughput & latency percentiles is generated.
