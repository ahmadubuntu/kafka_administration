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

Set `ROLE=source` / `ROLE=dest` in env files, or pass source first then dest.

## `check_mirrormaker.sh`

Systemd unit, masked `mm2.properties`, JVM heap, internal topics, consumer-group /
HWM lag, optional Jolokia, Connect REST (INFO if dedicated MM2 has no REST).

```bash
./check_mirrormaker.sh -c config/clusters/prod.env -c config/clusters/dr.env \
  --mm2-properties /var/opt/kafka/config/mm2.properties -y
```

Do not put production passwords in git. Reports under `reports/`.
