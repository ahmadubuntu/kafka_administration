# Kafka topic batch admin

Scripts for **pattern-based** topic maintenance (outside the HA realtime check toolkit).

Reuses shared libs from [`../kafka_realtime_check/lib/`](../kafka_realtime_check/lib/)
(`entity_filter`, SSH, parallel). Prefer the same cluster inventories:

`../kafka_realtime_check/config/clusters/<env>.env`

## `manage_topics.sh`

| Action | Flags |
|--------|--------|
| List | `--list` (default) + `--pattern` |
| Set configs | `--set-config key=value` (repeatable) + `--apply` |
| Delete | `--delete` + `--apply` |

Safety: mutations need `--apply`; `--pattern` required for delete/set; broad patterns
like `.*` refused unless `--force-broad`; `--jobs` default **8**; `--max-topics` cap.
SSH inventory sweep is **off** by default (use `--check-ssh`); sudo prompt off (use `--sudo`).

```bash
# Dry-run list
./manage_topics.sh -c ../kafka_realtime_check/config/clusters/devkafka.env -u "$USER" \
  --pattern '^cursor-test-' --list

# Set retention on matching topics
./manage_topics.sh -c ../kafka_realtime_check/config/clusters/devkafka.env -u "$USER" -y \
  --pattern '^cursor-test-' \
  --set-config retention.ms=86400000 \
  --apply --jobs 8

# Delete matching topics
./manage_topics.sh -c ../kafka_realtime_check/config/clusters/devkafka.env -u "$USER" -y \
  --pattern '^cursor-test-' --delete --apply
```

**Never** point `--pattern` at production topic namespaces without a dry-run first.

## `topic_hygiene.sh`

Idle / dead topic report & cleanup, plus topics with **no active consumer**.
(`idle_topics.sh` is a thin compatibility wrapper that execs this script.)

| Mode | Flags |
|------|--------|
| Report | `--report` (default) → age-bucket table + TSV under `reports/` |
| No-consumer list | `--list-no-consumers` → topics with no live consumer assignment |
| Annotate consumers | `--with-consumers` |
| Delete idle, no consumers | `--delete --idle-days 180 --require-no-consumers --apply` |
| Delete idle even with consumers | `--delete --idle-days 180 --allow-with-consumers --apply` |

Safety: `--delete` needs `--pattern` or `--force-broad`; mutations need `--apply`;
`--max-topics` cap; `_` names skipped unless `--include-internal`.

```bash
# Age buckets (fast — no group describe)
./topic_hygiene.sh -c ../kafka_realtime_check/config/clusters/devkafka.env -u "$USER"

# Topics with no live consumer assignment
./topic_hygiene.sh -c ../kafka_realtime_check/config/clusters/devkafka.env -u "$USER" \
  --list-no-consumers --jobs 8

# Same + active consumer annotation
./topic_hygiene.sh -c … -u "$USER" --idle-days 180 --with-consumers

# Dry-run delete: idle ≥180d AND no active consumer
./topic_hygiene.sh -c … -u "$USER" -y --idle-days 180 \
  --delete --require-no-consumers --force-broad

# Apply delete for one namespace
./topic_hygiene.sh -c … -u "$USER" -y --idle-days 180 \
  --pattern '^charisma\.data\.sentry\.' \
  --delete --require-no-consumers --apply --jobs 8
```

Idle age is the **newest** `.log` segment mtime across all `BROKER_HOSTS`.
“Active consumer” / `--list-no-consumers` means a group describe row with a real
`CONSUMER-ID` (not `-`). Empty groups or committed offsets alone do **not** count.

## `compare_clusters.sh`

Same probes used when comparing kafkio/CLI latency across envs. Pass **one or more**
inventories; pick probes with `--only` / `--skip`.

| Probe | What it measures |
|-------|------------------|
| `tcp` | TCP connect ms to bootstrap hosts |
| `counts` | topics / partitions / groups / broker count |
| `broker_cli` | `api-versions` / `topics --list|--describe` / `log-dirs` on broker via SSH |
| `local_cli` | same ops from this laptop (`--local-bin`, scp admin props) |
| `jolokia` | Metadata/ApiVersions/DescribeConfigs means, idle %, external conns, heap |
| `idle` | age-bucket summary (`>180d` count + GB) |

```bash
# One cluster, quick size + TCP
./compare_clusters.sh --clusters-dir ../kafka_realtime_check/config/clusters \
  --clusters devkafka -u "$USER" -y --only tcp,counts

# Dev vs DMZ vs Stage (repeat -c also works)
./compare_clusters.sh -u "$USER" -y \
  -c ../kafka_realtime_check/config/clusters/devkafka.env \
  -c ../kafka_realtime_check/config/clusters/stgkafka.env \
  -c ../kafka_realtime_check/config/clusters/dmzkafka.env \
  --only tcp,counts,broker_cli,local_cli,jolokia \
  --local-bin ~/Softs/kafka/kafka_2.13-3.9.0/bin

./compare_clusters.sh --list-tasks
```

Writes a matrix to the console and `reports/compare-*.txt`.
