# Kafka Administration & HA Health Check

Toolkits in `kafka_realtime_check`:

1. **`run_all.sh`** — orchestrator: HA + min.isr scan + broker admin suite  
2. **`check_kafka_ha.sh`** — inventory-driven multi-node HA health check  
3. **`fix_topic_min_isr.sh`** — cluster/topic `min.insync.replicas` scan/apply  
4. **`fix_topic_replication.sh`** — find topics by RF (e.g. 1,2) and raise via reassignment  
5. **`run_admin_suite.sh` + `scripts/`** — deep single-broker admin diagnostics  
   (formerly `run_all.sh`)  
6. **`run_via_ssh.sh`** — sync admin suite to a broker and run it there  

See [ARCHITECTURE.md](ARCHITECTURE.md) for topology and check flow.

## Quick start — everything

```bash
./run_all.sh -c config/clusters/dmzkafka.env -u "$USER" -y
./run_all.sh -c config/clusters/dmzkafka.env -u "$USER" -y --only ha
./run_all.sh -c config/clusters/dmzkafka.env -u "$USER" -y --only ha,min_isr
./run_all.sh --only admin -- --only host,service
./run_all.sh -c CONFIG.env -u "$USER" -y --only ha -- --only os
./run_all.sh --list-tasks
```

Args after `--` are forwarded to the selected child entrypoint(s).

## Quick start — HA check

```bash
cp config/clusters/devkafka.example.env config/clusters/devkafka.env
# edit hosts/ports/EXPECT_* — never commit secrets (*.env is gitignored)
./check_kafka_ha.sh -c config/clusters/devkafka.env -u "$USER" -y
```

Options: `-v` verbose, `--json`, `--skip-lag`, `-n` no sudo, `-o report.log`,
`--only TASKS`, `--skip TASKS`, `--ask-tasks`, `--list-tasks`.

```bash
./check_kafka_ha.sh -c config/clusters/dmzkafka.env -u "$USER" -y --only os
./check_kafka_ha.sh -c config/clusters/dmzkafka.env -u "$USER" --only "OS health & security"
./check_kafka_ha.sh -c config/clusters/dmzkafka.env -u "$USER" -y --only os,ports,cascade
./check_kafka_ha.sh -c config/clusters/dmzkafka.env -u "$USER" --ask-tasks
./check_kafka_ha.sh --list-tasks
```

Exit codes: `0` PASS · `1` WARN/SLOW · `2` FAIL.

Reports: `reports/kafkaha-<slug>-<timestamp>.log`.

## Topic / cluster min.insync.replicas

```bash
./fix_topic_min_isr.sh -c config/clusters/stgkafka.env -u "$USER" -y --set 2
./fix_topic_min_isr.sh -c config/clusters/stgkafka.env -u "$USER" --find 1 --set 2 --apply --jobs ask
./fix_topic_min_isr.sh -c config/clusters/stgkafka.env -u "$USER" -y --only cluster
./fix_topic_min_isr.sh -c config/clusters/stgkafka.env -u "$USER" -y --only topics --find 1 --set 2
./fix_topic_min_isr.sh --list-tasks
```

`--skip-cluster` / `--skip-topics` remain as aliases for `--skip cluster` / `--skip topics`.

## Topic replication factor

Finds topics with `ReplicationFactor` in `--find` (default `1,2`) and can raise them
to `--set` (default `3`) using `kafka-reassign-partitions` (keeps existing replicas,
adds brokers). Without `--apply` only reports + writes a JSON plan under `reports/`.

```bash
# Report topics with RF 1 or 2 (and RF < 3)
./fix_topic_replication.sh -c config/clusters/stgkafka.env -u "$USER" -y --find 1,2 --set 3

# Execute reassignment to RF=3
./fix_topic_replication.sh -c config/clusters/stgkafka.env -u "$USER" -y --find 1,2 --set 3 --apply

# Only RF=1 topics; scan only
./fix_topic_replication.sh -c config/clusters/stgkafka.env -u "$USER" -y --only scan --find 1 --set 3

./fix_topic_replication.sh --list-tasks
```

Needs at least `--set` live brokers. Internal `__*` topics skipped unless `--include-internal`.

## Quick start — deep broker admin

```bash
cp env.example env.sh   # paths for a single broker session
./run_admin_suite.sh            # on the broker
./run_admin_suite.sh --only host,service
./run_admin_suite.sh --list-tasks
# or from laptop:
export KAFKA_SSH_HOST=devkafka
./run_via_ssh.sh
./run_via_ssh.sh devkafka -- --only host,jolokia
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
run_all.sh                 # orchestrator (all suites)
check_kafka_ha.sh
fix_topic_min_isr.sh
fix_topic_replication.sh
run_admin_suite.sh         # deep broker admin (was run_all.sh)
run_via_ssh.sh
lib/tasks.sh               # shared --only/--skip/--ask-tasks
VERSION
ARCHITECTURE.md
config/clusters/*.example.env
lib/
scripts/
```
