# Tasks: Three-Vantage Kafka Dev Performance Benchmark

**Input**: Design documents from `/specs/002-k8s-dev-perf-benchmark/`

## Phase 1: Spec Kit & Parser Foundation

- [x] T001 Initialize `specs/002-k8s-dev-perf-benchmark/{spec,plan,tasks}.md` and update `.specify/feature.json`
- [x] T002 Create `kafka_realtime_check/lib/benchmark_parse.py` to parse `kafka-producer-perf-test.sh` and `kafka-consumer-perf-test.sh` outputs and wall-clock topic-list timings into JSON/markdown summary
- [x] T003 Create unit tests in `kafka_realtime_check/lib/test_benchmark_parse.py` covering producer perf lines, consumer perf CSV lines (Kafka 3.x and 4.x formats), and timing aggregation

## Phase 2: Benchmark Runner Script

- [x] T004 Create `kafka_realtime_check/benchmark_kafka_vantages.sh` with inventory loading (`-c`), dry-run default (`--apply -y` required to mutate), `--k8s-context`, `--namespace`, `--records`, `--record-size`, and `--keep` flags

## Phase 3: Live Execution on Dev (`kafkausage-test`, Laptop, Broker VM)

- [x] T005 Provision `namespace=kafkausage-test`, Secret `kafka-client-config`, and pods `kafka-producer-client` & `kafka-consumer-client` on Kubernetes `Dev`
- [x] T006 Run 3-vantage topic listing latency benchmark and concurrent produce/consume benchmark (`kafkausage-bench-write` + `kafkausage-bench-read`) across K8s pod, Laptop, and Broker VM, keeping resources after run

## Phase 4: Verification, Graphify & Git Delivery

- [x] T007 Run unit tests, update `graphify update .`, record internal benchmark report in `plans/` (Azure-only), and complete feature branch PR + merge + dual-remote push
