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

## Known landmines (MM2 toolkit)

- Kafka 3.9 `kafka-log-dirs.sh` does **not** accept `--json`. Default stdout is JSON plus status lines. Parse with `mm2_parse.parse_logdirs` (JSON dict or array partitions, then text).
- Dedicated MM2 `*.consumer.group.id` may not exist on source; also list/describe on dest. HWM lag is the fallback.
- Do not treat `__consumer_offsets` HWM as mirror lag (`skip_hwm_compare`).

## Current focus

Latest plan: [`plans/2026-09-10_015649_mm2-compare-storage-is-python.md`](plans/2026-09-10_015649_mm2-compare-storage-is-python.md)
