# Kafka MirrorMaker 2 — prod vs DR

Run **on the MM2 host** (`connect-mirror-maker.sh`). Uses **Kafka protocol + local CLI**.
SSH to brokers is `--via ssh` (stub in v0.1).

Inventories (gitignored): copy `config/clusters/*.example.env` → `*.env`.
Point `KAFKA_COMMAND_CONFIG` at admin client files **on this host**.

## `compare_storage.sh`

Explains per-broker disk (e.g. prod ~193 GiB vs DR ~280 GiB): broker count, replicated
log-dirs totals, mapped topic unique bytes, MM2 internal topics, RF, retention drift, HWM gaps.

```bash
./compare_storage.sh -c config/clusters/prod.env -c config/clusters/dr.env -y --via kafka
./compare_storage.sh -c prod.env -c dr.env --only summary,logdirs,gaps --list-tasks
```

`compare_storage.sh` is **bash** (`head -1` must be `#!/usr/bin/env bash`). Do not replace it with `lib/join_storage.py`.

Set `ROLE=source` / `ROLE=dest` in env files, or pass source first then dest.
`kafka-log-dirs` is called with `--broker-list` from `kafka-broker-api-versions` (required on Kafka 3.x).
Kafka 3.9 tools have **no `--json` flag**; the script uses `--describe` and parses the default JSON (or text on older CLIs).
Default includes `_` topics (`--exclude-internal` to skip).

## `check_mirrormaker.sh`

Systemd unit, masked `mm2.properties`, JVM heap, internal topics, consumer-group /
HWM lag, optional Jolokia, Connect REST (INFO if dedicated MM2 has no REST).

```bash
./check_mirrormaker.sh -c config/clusters/prod.env -c config/clusters/dr.env \
  --mm2-properties /var/opt/kafka/config/mm2.properties -y
```

Do not put production passwords in git. Reports under `reports/`.

To shrink DR only for topics that are already compressed on prod, set dest
`compression.type` on those topics (broker recompresses new segments). Do **not**
set `producer.compression.type` on MM2 — that would compress every mirrored topic.
`compare_storage.sh` writes `reports/storage-compress-dest-*.txt` (dest-only alters).
Inferred codec default is `lz4` (`COMPRESS_INFERRED_CODEC=zstd` to change).

## `sync_topic_configs.sh` / `sync_acls.sh`

Compare source (prod) to dest (DR) and optionally make dest match. **Dry-run
unless `--apply -y`.** Does not create topics or change RF.

```bash
./sync_topic_configs.sh -c config/clusters/prod.env -c config/clusters/dr.env -y
./sync_topic_configs.sh -c prod.env -c dr.env -y --apply
./sync_acls.sh -c prod.env -c dr.env -y
./sync_acls.sh -c prod.env -c dr.env -y --apply
./sync_acls.sh -c prod.env -c dr.env -y --apply --prune   # also drop dest-only ACLs
```

`--prune` is dest-only ACL delete. Topic config sync skips replica-throttle and
`remote.*` keys unless `--sync-skipped`.
