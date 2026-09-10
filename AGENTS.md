# Agent notes — kafka_administration

Inventory-driven Kafka HA checks, topic admin, and MirrorMaker 2 prod/DR diagnostics.

## Layout

| Path | Role |
|------|------|
| `kafka_realtime_check/` | HA health check, min.isr / RF fixers, admin suite |
| `kafka_topic_admin/` | Pattern topic ops, `topic_hygiene.sh`, `compare_clusters.sh` |
| `kafka_mirrormaker/` | Prod vs DR storage compare + MM2 dedicated health (run on MM host) |
| `plans/` | Append-only plan history — **never delete** |

## Non-negotiables

- Inventories: `config/clusters/*.example.env` committed; real `*.env` gitignored.
- Never commit passwords, PATs, or `KAFKA_COMMAND_CONFIG` contents.
- Dual remotes: `origin` (GitHub) and `azure` (DataPlatform) — see `.cursor/rules/azure-devops.mdc`.
- Read the **latest** file in `plans/` before changing behavior.

## How to run (MM2 toolkit)

On the MirrorMaker host, with Kafka CLI and protocol access to both clusters:

```bash
cd kafka_mirrormaker
cp config/clusters/prod.example.env config/clusters/prod.env
cp config/clusters/dr.example.env config/clusters/dr.env
# edit bootstrap + command-config paths on this host
./compare_storage.sh -c config/clusters/prod.env -c config/clusters/dr.env -y --via kafka
./check_mirrormaker.sh -c config/clusters/prod.env -c config/clusters/dr.env \
  --mm2-properties /var/opt/kafka/config/mm2.properties -y
```

`--via ssh` is a stub in v0.1 (SKIP until BROKER_HOSTS + SSH are wired).

## Current focus

Latest plan: [`plans/2026-08-24_113700_mm2-prod-dr-storage.md`](plans/2026-08-24_113700_mm2-prod-dr-storage.md)
