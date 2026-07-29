# Kafka Administration & HA Health Check

Two complementary toolkits in this directory (`kafka_realtime_check`):

1. **`check_kafka_ha.sh`** — inventory-driven multi-node HA health check  
   (`PASS` / `WARN` / `SLOW` / `FAIL`, cascade, port mesh, OS/boot, capacity).
2. **`fix_topic_min_isr.sh`** — list topics with `min.insync.replicas=N` and optionally alter them.
3. **`run_all.sh` + `scripts/`** — deep single-broker admin diagnostics  
   (Jolokia latency, DescribeConfigs, UI-like timing, log signals).

See [ARCHITECTURE.md](ARCHITECTURE.md) for topology and check flow.

## Quick start — HA check

```bash
cp config/clusters/devkafka.example.env config/clusters/devkafka.env
# edit hosts/ports/EXPECT_* — never commit secrets (*.env is gitignored)
./check_kafka_ha.sh -c config/clusters/devkafka.env -u "$USER" -y
```

Options: `-v` verbose, `--json`, `--skip-lag`, `-n` no sudo, `-o report.log`.

Exit codes: `0` PASS · `1` WARN/SLOW · `2` FAIL.

Reports: `reports/kafkaha-<slug>-<timestamp>.log`.

## Topic / cluster min.insync.replicas

Shows live broker-default + `server.properties`, optionally sets a new default
via `kafka-configs --entity-default` **and** edits config files (**no Kafka restart**),
then alters matching topics in parallel.

```bash
# Report only
./fix_topic_min_isr.sh -c config/clusters/stgkafka.env -u "$USER" -y --set 2

# Apply cluster default + topics (asks for parallel jobs unless --jobs auto|N)
./fix_topic_min_isr.sh -c config/clusters/stgkafka.env -u "$USER" --find 1 --set 2 --apply --jobs ask

# Faster non-interactive topic workers
./fix_topic_min_isr.sh -c config/clusters/stgkafka.env -u "$USER" -y --find 1 --set 2 --apply --jobs 24
```

Flags: `--skip-cluster`, `--skip-topics`, `--jobs ask|auto|N` (suggested 8–32).

## Quick start — deep broker admin

```bash
cp env.example env.sh   # paths for a single broker session
./run_all.sh            # on the broker
# or from laptop:
export KAFKA_SSH_HOST=devkafka
./run_via_ssh.sh
```

## Inventory keys (HA)

| Key | Purpose |
|-----|---------|
| `BROKER_HOSTS` / `CONTROLLER_HOSTS` | CSV of nodes |
| `VIP_HOST` / `LB_HOSTS` | optional client entry |
| `KAFKA_*_PORT(S)` | verified listeners only |
| `CHECKER_REACHABLE_PORTS` | local FAIL only if listed |
| `CLUSTER_INTERNAL_PORTS` | local miss → INFO |
| `EXPECT_MIN_INSYNC_REPLICAS` / `EXPECT_TLS` / … | config drift |
| `LAG_WARN` / `LAG_FAIL` / `URP_WARN_COUNT` | thresholds |

Credentials: SSH user prompted once (`-u` / `-y`); sudo once; Kafka SASL via `KAFKA_COMMAND_CONFIG` on broker (never commit).

## Layout

```
check_kafka_ha.sh
fix_topic_min_isr.sh
VERSION
ARCHITECTURE.md
config/clusters/*.example.env   # real *.env gitignored
lib/
scripts/
run_all.sh
run_via_ssh.sh
```
