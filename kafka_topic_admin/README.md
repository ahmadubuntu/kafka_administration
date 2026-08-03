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

## `idle_topics.sh`

Finds **dead / idle** topics from `log.dirs` segment mtimes (age buckets), optionally
checks whether any consumer group has an **active assignment**, then can delete.

| Mode | Flags |
|------|--------|
| Report | `--report` (default) → age-bucket table + TSV under `reports/` |
| Annotate consumers | `--with-consumers` |
| Delete idle, no consumers | `--delete --idle-days 180 --require-no-consumers --apply` |
| Delete idle even with consumers | `--delete --idle-days 180 --allow-with-consumers --apply` |

Safety: `--delete` needs `--pattern` or `--force-broad`; mutations need `--apply`;
`--max-topics` cap; `_` names skipped unless `--include-internal`.

```bash
# Age buckets (fast — no group describe)
./idle_topics.sh -c ../kafka_realtime_check/config/clusters/devkafka.env -u "$USER"

# Same + active consumer annotation
./idle_topics.sh -c … -u "$USER" --idle-days 180 --with-consumers

# Dry-run delete: idle ≥180d AND no active consumer
./idle_topics.sh -c … -u "$USER" -y --idle-days 180 \
  --delete --require-no-consumers --force-broad

# Apply delete for one namespace
./idle_topics.sh -c … -u "$USER" -y --idle-days 180 \
  --pattern '^charisma\.data\.sentry\.' \
  --delete --require-no-consumers --apply --jobs 8
```

Idle age is the **newest** `.log` segment mtime across all `BROKER_HOSTS`.
“Active consumer” means a group describe row with a real `CONSUMER-ID` (not `-`).
