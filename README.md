# kafka_administration

| Toolkit | Path |
|---------|------|
| HA realtime check + min.isr / RF fixers | [`kafka_realtime_check/`](kafka_realtime_check/) |
| Topic batch admin (delete / set configs by pattern) | [`kafka_topic_admin/`](kafka_topic_admin/) |

```bash
cd kafka_realtime_check
./check_kafka_ha.sh -c config/clusters/devkafka.env -u "$USER" -y

cd ../kafka_topic_admin
./manage_topics.sh -c ../kafka_realtime_check/config/clusters/devkafka.env -u "$USER" \
  --pattern '^cursor-test-' --list
```
