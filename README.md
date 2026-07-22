# Kafka Administration Toolkit

Bash (+ Jolokia Python) suite to health-check a Kafka broker the same way we
investigated `devkafka`: host resources, cluster CLI, connections, JMX exporter,
Jolokia latency histograms, and log signals.

## Layout

```
env.example                 # copy to env.sh and edit
lib/common.sh               # shared helpers
lib/jolokia_latency.py      # request latency percentiles via Jolokia
scripts/01_host_resources.sh
scripts/02_service_status.sh
scripts/03_cluster_health.sh
scripts/04_admin_ops_timing.sh
scripts/05_broker_config.sh
scripts/06_connections.sh
scripts/07_jmx_exporter.sh
scripts/08_jolokia_latency.sh
scripts/09_log_signals.sh
scripts/11_capacity_estimate.sh
scripts/10_summary_hints.sh
run_all.sh                  # run everything on current host
run_via_ssh.sh              # sync + run on a remote broker
```

## Quick start (on the Kafka host)

```bash
cd /path/to/kafka_administration
cp env.example env.sh
# edit env.sh: KAFKA_BOOTSTRAP, paths, JMX/Jolokia URLs
chmod +x run_all.sh scripts/*.sh
./run_all.sh
```

Reports land in `reports/<timestamp>/`.

Output uses boxed sections, aligned tables, status badges, and progress bars on a
TTY. Disable color with `NO_COLOR=1` or `KAFKA_ADMIN_COLOR=0`.

## Quick start (from your laptop via SSH)

```bash
cp env.example env.sh
# set paths/bootstrap for the *remote* host
export KAFKA_SSH_HOST=devkafka
./run_via_ssh.sh
```

## Important env vars

| Variable | Purpose |
|----------|---------|
| `KAFKA_HOME` / `KAFKA_BIN` | Kafka binaries (`kafka-topics.sh`, …) |
| `KAFKA_BOOTSTRAP` | Client bootstrap (usually `host:9094`) |
| `KAFKA_COMMAND_CONFIG` | `admin.properties` (SASL etc.) |
| `KAFKA_SERVER_PROPERTIES` | Running `server.properties` |
| `KAFKA_LOG_DIR` | Broker log dir |
| `KAFKA_JMX_METRICS_URL` | jmx_exporter scrape URL |
| `KAFKA_JOLOKIA_URL` | Jolokia base URL |
| `KAFKA_SYSTEMD_UNIT` | systemd unit name (`kafka`) |

Do **not** commit `env.sh` if it contains passwords (`sasl.jaas.config`).

## What each script answers

1. **Host** — CPU, RAM, disk, IO, Kafka RSS/CPU  
2. **Service** — systemd, listeners `9092/9093/9094`, JMX `7071`, Jolokia `8779`  
3. **Cluster CLI** — topic/partition counts, URP, unavailable partitions, groups  
4. **Admin timing** — UI-like `describe` / `--all-groups` wall times  
5. **Config** — listeners, threads, **bootstrap vs advertised EXTERNAL mismatch**  
6. **Connections** — ESTABLISHED pressure / top peer IPs on `:9094`  
7. **JMX exporter** — URP, offline, JVM, throughput, request counts  
8. **Jolokia** — p50/p95/p99 for `DescribeConfigs` etc. (+ `ResponseSendTimeMs`)  
9. **Logs** — SASL handshake errors, authorizer denials, GC pauses  
11. **Capacity** — FD / RAM / CPU headroom + estimated partition & connection ceilings  
10. **Hints** — short interpretation checklist  

## Single-script usage

```bash
source lib/common.sh   # or rely on each script sourcing it
./scripts/08_jolokia_latency.sh
./scripts/03_cluster_health.sh
```
