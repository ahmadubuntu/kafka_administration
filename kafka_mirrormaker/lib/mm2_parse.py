#!/usr/bin/env python3
"""Parse MM2 properties, log-dirs JSON, topic describe, configs, offsets. No secrets printed."""
from __future__ import annotations

import json
import os
import re
import sys
from collections import defaultdict
from typing import Any


def load_props(path: str) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path or not os.path.isfile(path):
        return out
    with open(path, encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def mask_props(props: dict[str, str]) -> dict[str, str]:
    masked = {}
    for k, v in props.items():
        lk = k.lower()
        if any(s in lk for s in ("password", "jaas", "secret", "sasl.jaas")):
            masked[k] = "***"
        else:
            masked[k] = v
    return masked


def replication_policy(props: dict[str, str]) -> str:
    cls = props.get("replication.policy.class", "")
    if "IdentityReplicationPolicy" in cls:
        return "identity"
    return "default"


def clusters(props: dict[str, str]) -> list[str]:
    raw = props.get("clusters", "")
    return [c.strip() for c in raw.split(",") if c.strip()]


def enabled_flows(props: dict[str, str]) -> list[tuple[str, str]]:
    """Only `{src}->{dst}.enabled=true`, not `{src}->{dst}.checkpoints.enabled`."""
    known = set(clusters(props))
    flows = []
    for k, v in props.items():
        if v.lower() not in ("true", "yes", "1"):
            continue
        m = re.match(r"^([A-Za-z0-9._-]+)->([A-Za-z0-9._-]+)\.enabled$", k)
        if not m:
            continue
        src, dst = m.group(1), m.group(2)
        if known and (src not in known or dst not in known):
            continue
        if not known and ("." in dst or "." in src):
            # without clusters=, reject dotted dest (avoids .checkpoints.enabled)
            continue
        flows.append((src, dst))
    return flows


def consumer_group_ids(props: dict[str, str]) -> list[str]:
    """asia-gen.consumer.group.id and similar."""
    found = []
    for k, v in props.items():
        if k.endswith(".consumer.group.id") or k == "group.id":
            if v and v not in found:
                found.append(v)
    return found


def skip_hwm_compare(topic: str, source_alias: str, dest_alias: str) -> bool:
    if is_mm2_internal(topic, source_alias, dest_alias):
        return True
    if topic.startswith("__"):
        return True
    return False


def map_source_to_dest(source_topic: str, policy: str, source_alias: str) -> str:
    if policy == "identity":
        return source_topic
    if not source_alias:
        return source_topic
    return f"{source_alias}.{source_topic}"


def map_dest_to_source(dest_topic: str, policy: str, source_alias: str) -> str | None:
    if policy == "identity":
        return dest_topic
    prefix = f"{source_alias}."
    if source_alias and dest_topic.startswith(prefix):
        return dest_topic[len(prefix) :]
    return None


def is_mm2_internal(name: str, source_alias: str, dest_alias: str) -> str:
    """Return kind or empty."""
    aliases = [a for a in (source_alias, dest_alias) if a]
    for a in aliases:
        if name == f"{a}.heartbeats":
            return "heartbeats"
        if name == f"{a}.checkpoints.internal":
            return "checkpoints"
        if name == f"mm2-offset-syncs.{a}.internal":
            return "offset-syncs"
    if name.startswith("mm2-offset-syncs.") and name.endswith(".internal"):
        return "offset-syncs"
    if name.endswith(".heartbeats"):
        return "heartbeats"
    if name.endswith(".checkpoints.internal"):
        return "checkpoints"
    if name in ("__consumer_offsets", "__transaction_state"):
        return "kafka-internal"
    return ""


def parse_logdirs_json(text: str) -> list[dict[str, Any]]:
    """Rows: broker, logdir, topic, partition, size."""
    text = text.strip()
    if not text:
        return []
    # kafka-log-dirs may print a header line before JSON
    start = text.find("{")
    if start < 0:
        return []
    blob = text[start:]
    try:
        data = json.loads(blob)
    except json.JSONDecodeError:
        # trailing log lines after JSON
        end = blob.rfind("}")
        if end < 0:
            return []
        try:
            data = json.loads(blob[: end + 1])
        except json.JSONDecodeError:
            return []
    rows = []
    brokers = data.get("brokers") or data.get("Brokers") or []
    if isinstance(data, list):
        brokers = data
    for b in brokers:
        bid = b.get("broker", b.get("brokerId", "?"))
        for ld in b.get("logDirs") or b.get("logdirs") or []:
            logdir = ld.get("logDir") or ld.get("logdir") or ""
            err = ld.get("error")
            parts = ld.get("partitions") or {}
            items: list[tuple[Any, Any]] = []
            if isinstance(parts, dict):
                items = list(parts.items())
            elif isinstance(parts, list):
                # Kafka 3.x / KIP-849: [{partition: "topic-0", size: N}, ...]
                for meta in parts:
                    if not isinstance(meta, dict):
                        continue
                    key = meta.get("partition") or meta.get("topicPartition") or ""
                    items.append((key, meta))
            for key, meta in items:
                if not isinstance(meta, dict):
                    continue
                size = int(meta.get("size") or meta.get("Size") or 0)
                part = meta.get("partition")
                topic = meta.get("topic")
                # Array items often put the full "topic-0" in partition.
                if topic is None and part is not None and not isinstance(part, int):
                    key = str(part)
                    part = None
                if topic is None:
                    # key like "name-0"
                    if "-" in str(key):
                        topic, _, p = str(key).rpartition("-")
                        if part is None:
                            try:
                                part = int(p)
                            except ValueError:
                                part = p
                    else:
                        topic = str(key)
                        part = part if part is not None else 0
                rows.append(
                    {
                        "broker": bid,
                        "logdir": logdir,
                        "error": err,
                        "topic": topic,
                        "partition": part,
                        "size": size,
                    }
                )
    return rows


def _split_topic_part(key: str) -> tuple[str, Any]:
    if "-" not in key:
        return key, 0
    topic, _, p = key.rpartition("-")
    try:
        return topic, int(p)
    except ValueError:
        return key, 0


def parse_logdirs_text(text: str) -> list[dict[str, Any]]:
    """Parse kafka-log-dirs --describe without --json (older / some 3.x builds)."""
    rows: list[dict[str, Any]] = []
    broker: Any = "?"
    logdir = ""
    # topic-0: size=123  |  topic-0 size: 123  |  topic: foo partition: 0 size: 123
    re_broker = re.compile(r"(?i)\bbrokers?\b[:\s]+(\d+)\b")
    re_logdir = re.compile(r"(?i)log[-_ ]?dir(?:ectory)?\s*[:=]\s*(\S+)")
    re_tp_size = re.compile(
        r"(?i)(?P<tp>[A-Za-z0-9._-]+-\d+)\s*[:\s]+size\s*[:=]\s*(?P<size>\d+)"
    )
    re_named = re.compile(
        r"(?i)topic\s*[:=]\s*(?P<topic>\S+)\s+partition\s*[:=]\s*(?P<part>\d+)\s+size\s*[:=]\s*(?P<size>\d+)"
    )
    for line in text.splitlines():
        bm = re_broker.search(line)
        if bm and "size" not in line.lower():
            broker = bm.group(1)
        lm = re_logdir.search(line)
        if lm:
            logdir = lm.group(1).rstrip(",")
        named_hits = list(re_named.finditer(line))
        if named_hits:
            for m in named_hits:
                rows.append(
                    {
                        "broker": broker,
                        "logdir": logdir,
                        "error": None,
                        "topic": m.group("topic"),
                        "partition": int(m.group("part")),
                        "size": int(m.group("size")),
                    }
                )
            continue
        for m in re_tp_size.finditer(line):
            topic, part = _split_topic_part(m.group("tp"))
            rows.append(
                {
                    "broker": broker,
                    "logdir": logdir,
                    "error": None,
                    "topic": topic,
                    "partition": part,
                    "size": int(m.group("size")),
                }
            )
    return rows


def parse_logdirs(text: str) -> list[dict[str, Any]]:
    rows = parse_logdirs_json(text)
    if rows:
        return rows
    return parse_logdirs_text(text)


def parse_topic_describe(text: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for line in text.splitlines():
        if "PartitionCount:" not in line or "Topic:" not in line:
            continue
        # Topic: foo\tPartitionCount: 3\tReplicationFactor: 3
        tm = re.search(r"Topic:\s*(\S+)", line)
        pm = re.search(r"PartitionCount:\s*(\d+)", line)
        rm = re.search(r"ReplicationFactor:\s*(\d+)", line)
        if not tm:
            continue
        name = tm.group(1)
        out[name] = {
            "partitions": int(pm.group(1)) if pm else 0,
            "rf": int(rm.group(1)) if rm else 0,
        }
    return out


def parse_api_versions_brokers(text: str) -> list[str]:
    ids = []
    for line in text.splitlines():
        m = re.search(r"\(id:\s*(\d+)", line)
        if m:
            ids.append(m.group(1))
    return ids


def parse_topic_configs(text: str) -> dict[str, dict[str, str]]:
    """Parse kafka-configs --entity-type topics --describe [--all]."""
    current = None
    out: dict[str, dict[str, str]] = defaultdict(dict)
    want = (
        "retention.ms",
        "retention.bytes",
        "cleanup.policy",
        "min.insync.replicas",
        "compression.type",
        "message.timestamp.type",
    )
    header = re.compile(
        r"(?:All configs for topic|Dynamic configs for topic|Configs for topic)\s+(\S+)",
        re.I,
    )
    for line in text.splitlines():
        m = header.search(line)
        if m:
            current = m.group(1).rstrip(":").rstrip(",")
        if current is None:
            continue
        for key in want:
            km = re.search(rf"(?:^|[\s,]){re.escape(key)}=(\S+)", line)
            if km:
                val = km.group(1).rstrip(",").split(",")[0]
                out[current][key] = val
    return dict(out)


EXPLICIT_CODECS = frozenset({"gzip", "snappy", "lz4", "zstd"})


def topic_compression(cfg: dict[str, str] | None) -> str:
    raw = (cfg or {}).get("compression.type") or "producer"
    return raw.split(",")[0].strip().lower()


def recommend_dest_compression(
    extras: list[tuple[int, str, str, int, int]],
    src_configs: dict[str, dict[str, str]],
    dst_configs: dict[str, dict[str, str]],
    source_alias: str,
    dest_alias: str,
    *,
    inferred_codec: str = "lz4",
    min_ratio: float = 2.5,
    min_extra_bytes: int = 1073741824,
) -> list[dict[str, Any]]:
    """Dest-only topic compression. Never a global MM2 producer codec.

    extras rows: (extra_unique, src_topic, dst_topic, src_unique, dst_unique)
    """
    codec = inferred_codec.strip().lower()
    if codec not in EXPLICIT_CODECS:
        codec = "lz4"
    out: list[dict[str, Any]] = []
    for extra, st, dt, suniq, duniq in extras:
        if skip_hwm_compare(st, source_alias, dest_alias) or skip_hwm_compare(
            dt, source_alias, dest_alias
        ):
            continue
        sc = topic_compression(src_configs.get(st))
        dc = topic_compression(dst_configs.get(dt))
        if dc in EXPLICIT_CODECS and (sc not in EXPLICIT_CODECS or dc == sc):
            continue
        if sc in EXPLICIT_CODECS and dc != sc:
            out.append(
                {
                    "src_topic": st,
                    "dst_topic": dt,
                    "codec": sc,
                    "reason": "src_topic_config",
                    "src_codec": sc,
                    "dst_codec": dc,
                    "extra": extra,
                    "src_unique": suniq,
                    "dst_unique": duniq,
                }
            )
            continue
        if sc == "uncompressed" or dc == "uncompressed":
            continue
        if suniq <= 0:
            continue
        ratio = duniq / suniq
        if extra >= min_extra_bytes and ratio >= min_ratio:
            out.append(
                {
                    "src_topic": st,
                    "dst_topic": dt,
                    "codec": codec,
                    "reason": "size_ratio_implies_src_compressed",
                    "src_codec": sc,
                    "dst_codec": dc,
                    "extra": extra,
                    "src_unique": suniq,
                    "dst_unique": duniq,
                    "ratio": ratio,
                }
            )
    out.sort(key=lambda r: -int(r["extra"]))
    return out


def parse_offsets(text: str) -> dict[tuple[str, int], int]:
    """kafka-get-offsets.sh lines: topic:partition:offset"""
    out = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.count(":") < 2:
            continue
        topic, part, off = line.rsplit(":", 2)
        try:
            out[(topic, int(part))] = int(off)
        except ValueError:
            continue
    return out


def cmd_parse_logdirs() -> None:
    rows = parse_logdirs(sys.stdin.read())
    for r in rows:
        print(f"{r['broker']}\t{r['topic']}\t{r['partition']}\t{r['size']}")


def cmd_broker_totals() -> None:
    rows = parse_logdirs(sys.stdin.read())
    by_b: dict[Any, int] = defaultdict(int)
    for r in rows:
        by_b[r["broker"]] += r["size"]
    for b, s in sorted(by_b.items(), key=lambda x: str(x[0])):
        print(f"{b}\t{s}")


def cmd_topic_totals() -> None:
    """Max replica size per topic-partition summed (unique-ish) AND raw sum."""
    rows = parse_logdirs(sys.stdin.read())
    raw: dict[str, int] = defaultdict(int)
    per_tp: dict[tuple[str, Any], int] = defaultdict(int)
    for r in rows:
        raw[r["topic"]] += r["size"]
        key = (r["topic"], r["partition"])
        if r["size"] > per_tp[key]:
            per_tp[key] = r["size"]
    unique: dict[str, int] = defaultdict(int)
    for (topic, _), sz in per_tp.items():
        unique[topic] += sz
    for t in sorted(set(raw) | set(unique)):
        print(f"{t}\t{raw[t]}\t{unique[t]}")


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: mm2_parse.py <command>", file=sys.stderr)
        sys.exit(2)
    cmd = sys.argv[1]
    if cmd == "parse-logdirs":
        cmd_parse_logdirs()
    elif cmd == "broker-totals":
        cmd_broker_totals()
    elif cmd == "topic-totals":
        cmd_topic_totals()
    elif cmd == "dump-props-masked":
        props = mask_props(load_props(sys.argv[2] if len(sys.argv) > 2 else ""))
        for k in sorted(props):
            print(f"{k}={props[k]}")
    elif cmd == "policy":
        props = load_props(sys.argv[2])
        print(replication_policy(props))
    elif cmd == "flows":
        props = load_props(sys.argv[2])
        for a, b in enabled_flows(props):
            print(f"{a}->{b}")
    elif cmd == "broker-ids":
        print(",".join(parse_api_versions_brokers(sys.stdin.read())))
    elif cmd == "consumer-groups":
        for g in consumer_group_ids(load_props(sys.argv[2])):
            print(g)
    else:
        print(f"unknown command {cmd}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
