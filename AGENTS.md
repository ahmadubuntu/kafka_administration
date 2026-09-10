# Agent notes — kafka_administration

Inventory-driven Kafka HA checks, topic admin, and MirrorMaker 2 prod/DR diagnostics.

## Layout

| Path | Role |
|------|------|
| `kafka_realtime_check/` | HA health check, min.isr / RF fixers, admin suite |
| `kafka_topic_admin/` | Pattern topic ops, `topic_hygiene.sh`, `compare_clusters.sh` |
| `kafka_mirrormaker/` | Prod vs DR storage compare + MM2 dedicated health (run on MM host) |
| `plans/` | Append-only plan history on disk — **never delete**; GitHub omit, Azure-only commit |

## Non-negotiables

- Inventories: `config/clusters/*.example.env` committed; real `*.env` gitignored.
- Never commit passwords, PATs, or `KAFKA_COMMAND_CONFIG` contents.
- Dual remotes: `origin` (GitHub) and `azure` (DataPlatform) — see `.cursor/rules/azure-devops.mdc`.
- `plans/` is Azure-only: never push it to GitHub (`origin`). See `.cursor/rules/plans-azure-only.mdc`.
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
./sync_topic_configs.sh -c config/clusters/prod.env -c config/clusters/dr.env -y
./sync_acls.sh -c config/clusters/prod.env -c config/clusters/dr.env -y
# mutate dest only after reviewing dry-run:
# ./sync_topic_configs.sh ... -y --apply
# ./sync_acls.sh ... -y --apply
```

`--via ssh` is a stub in v0.1 (SKIP until BROKER_HOSTS + SSH are wired).

## Known landmines (MM2 toolkit)

- Kafka 3.9 `kafka-log-dirs.sh` does **not** accept `--json`. Default stdout is JSON plus status lines. Parse with `mm2_parse.parse_logdirs` (JSON dict or array partitions, then text).
- Dedicated MM2 `*.consumer.group.id` may not exist on source; also list/describe on dest. HWM lag is the fallback.
- Do not treat `__consumer_offsets` HWM as mirror lag (`skip_hwm_compare`).
- Identity MM2 dest offsets are a different space than source; HWM-sum gap is not leftover history.
- Do not set MM2 `producer.compression.type` globally. Dest `compression.type` only on topics already compressed on prod.
- MM2 cannot preserve source batch compression (consumer decompresses; one producer codec per flow). Equivalent: dest topic `compression.type` per topic.
- `sync_topic_configs.sh` / `sync_acls.sh` are source→dest, dry-run unless `--apply -y`. `--apply` never creates missing dest topics; it only alters configs on topics present on both sides. `--prune` deletes dest-only ACLs.

## Current focus

Latest plan (disk / Azure only): `plans/2026-09-10_043200_sync-configs-no-create.md`
