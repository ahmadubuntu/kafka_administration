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
