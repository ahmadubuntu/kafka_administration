# Kafka Administration & HA Health Check

Two complementary toolkits:

1. **`check_kafka_ha.sh`** — inventory-driven multi-node HA health check  
   (`PASS` / `WARN` / `SLOW` / `FAIL`, cascade, port mesh, OS/boot, capacity).
2. **`run_all.sh` + `scripts/`** — deep single-broker admin diagnostics  
   (Jolokia latency, DescribeConfigs, UI-like timing, log signals).

See [ARCHITECTURE.md](ARCHITECTURE.md) for topology and check flow.

## Quick start — HA check

```bash
cp config/clusters/devkafka.example.env config/clusters/devkafka.env
# edit hosts/ports/EXPECT_* — never commit secrets
./check_kafka_ha.sh -c config/clusters/devkafka.env -u "$USER" -y
```

Options: `-v` verbose, `--json`, `--skip-lag`, `-n` no sudo, `-o report.log`.

Exit codes: `0` PASS · `1` WARN/SLOW · `2` FAIL.

Reports: `reports/kafkaha-<slug>-<timestamp>.log`.

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

Credentials: SSH user prompted once (`-u` / `-y`); sudo once; Kafka SASL via `KAFKA_COMMAND_CONFIG` on checker or broker (never commit).

## Layout

```
check_kafka_ha.sh
VERSION
ARCHITECTURE.md
config/clusters/*.example.env
lib/                 # HA shared + kafka_*.sh
scripts/             # deep admin suite
run_all.sh
run_via_ssh.sh
```
