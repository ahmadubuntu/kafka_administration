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
    flows = []
    for k, v in props.items():
        m = re.match(r"^([A-Za-z0-9._-]+)->([A-Za-z0-9._-]+)\.enabled$", k)
        if m and v.lower() in ("true", "yes", "1"):
            flows.append((m.group(1), m.group(2)))
    return flows


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
    data = json.loads(text[start:])
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
            if isinstance(parts, dict):
                items = parts.items()
            else:
                items = []
            for key, meta in items:
                if not isinstance(meta, dict):
                    continue
                size = int(meta.get("size") or meta.get("Size") or 0)
                part = meta.get("partition")
                topic = meta.get("topic")
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
    want = ("retention.ms", "retention.bytes", "cleanup.policy", "min.insync.replicas")
    for line in text.splitlines():
        m = re.search(
            r"(?:Dynamic configs for topic|Configs for topic)\s+(\S+)", line, re.I
        )
        if m:
            current = m.group(1).rstrip(":")
            continue
        if current is None:
            continue
        km = re.search(r"^\s*([A-Za-z0-9._-]+)=(\S+)", line)
        if km and km.group(1) in want:
            val = km.group(2).split(",")[0]
            out[current][km.group(1)] = val
    return dict(out)


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
    rows = parse_logdirs_json(sys.stdin.read())
    for r in rows:
        print(f"{r['broker']}\t{r['topic']}\t{r['partition']}\t{r['size']}")


def cmd_broker_totals() -> None:
    rows = parse_logdirs_json(sys.stdin.read())
    by_b: dict[Any, int] = defaultdict(int)
    for r in rows:
        by_b[r["broker"]] += r["size"]
    for b, s in sorted(by_b.items(), key=lambda x: str(x[0])):
        print(f"{b}\t{s}")


def cmd_topic_totals() -> None:
    """Max replica size per topic-partition summed (unique-ish) AND raw sum."""
    rows = parse_logdirs_json(sys.stdin.read())
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
    else:
        print(f"unknown command {cmd}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
