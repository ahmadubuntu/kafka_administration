#!/usr/bin/env python3
"""Read Kafka request latency histograms from Jolokia."""

from __future__ import annotations

import json
import os
import sys
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from format import (  # noqa: E402
    box,
    bullet,
    fmt_bytes,
    fmt_ms,
    fmt_num,
    kv,
    note,
    section,
    status,
    subsection,
    table,
)

JOLOKIA = os.environ.get("KAFKA_JOLOKIA_URL", "http://127.0.0.1:8779/jolokia").rstrip("/")
REQUESTS = [
    "Metadata",
    "DescribeConfigs",
    "DescribeGroups",
    "ListGroups",
    "OffsetFetch",
    "FindCoordinator",
    "ApiVersions",
    "Produce",
    "Fetch",
    "FetchConsumer",
]
# Compact columns for readability (drop Count / 999th in main tables)
KEYS = ["Mean", "50thPercentile", "95thPercentile", "99thPercentile", "Max"]
KEY_LABELS = ["mean", "p50", "p95", "p99", "max"]
METRICS_FOCUS = [
    "TotalTimeMs",
    "LocalTimeMs",
    "ResponseSendTimeMs",
    "RequestQueueTimeMs",
]


def get(path: str) -> Dict[str, Any]:
    url = JOLOKIA + path
    with urllib.request.urlopen(url, timeout=15) as resp:
        return json.load(resp)


def read_bean(bean: str) -> Optional[Dict[str, Any]]:
    data = get("/read/" + urllib.parse.quote(bean))
    value = data.get("value")
    if isinstance(value, dict):
        return value
    if value is not None:
        return {"Value": value}
    return None


def gauge_value(bean: str) -> Any:
    value = read_bean(bean)
    if not value:
        return None
    if "Value" in value:
        return value.get("Value")
    return value


def latency_rows(metric: str) -> List[List[str]]:
    rows: List[List[str]] = []
    for req in REQUESTS:
        bean = "kafka.network:type=RequestMetrics,name=%s,request=%s" % (metric, req)
        value = read_bean(bean)
        if not value:
            rows.append([req, "—", "—", "—", "—", "—"])
            continue
        rows.append([req] + [fmt_ms(value.get(k)) for k in KEYS])
    return rows


def main() -> int:
    section("08  Jolokia request latency")
    kv("Jolokia URL", JOLOKIA)
    try:
        ver = get("/version")
        kv("agent", (ver.get("value") or {}).get("agent") or "—")
    except Exception as exc:  # noqa: BLE001
        status("ERROR", "cannot reach Jolokia: %s" % exc)
        return 1

    subsection("Cluster gauges")
    gauges = [
        ("UnderReplicatedPartitions", "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions"),
        ("OfflinePartitions", "kafka.controller:type=KafkaController,name=OfflinePartitionsCount"),
        ("Topics", "kafka.controller:type=KafkaController,name=GlobalTopicCount"),
        ("Partitions", "kafka.controller:type=KafkaController,name=GlobalPartitionCount"),
        ("ActiveController", "kafka.controller:type=KafkaController,name=ActiveControllerCount"),
        ("RequestQueueSize", "kafka.network:type=RequestChannel,name=RequestQueueSize"),
        ("NetworkProcessorIdle", "kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent"),
    ]
    rows = []
    for label, bean in gauges:
        rows.append([label, fmt_num(gauge_value(bean), 3)])
    table(["gauge", "value"], rows, aligns=["l", "r"])

    os_bean = read_bean("java.lang:type=OperatingSystem") or {}
    subsection("Process / OS")
    table(
        ["metric", "value"],
        [
            ["ProcessCpuLoad", ("%.1f%%" % (float(os_bean["ProcessCpuLoad"]) * 100)) if os_bean.get("ProcessCpuLoad") is not None else "—"],
            ["CpuLoad", ("%.1f%%" % (float(os_bean["CpuLoad"]) * 100)) if os_bean.get("CpuLoad") is not None else "—"],
            ["Open FD", "%s / %s" % (
                fmt_num(os_bean.get("OpenFileDescriptorCount"), 0),
                fmt_num(os_bean.get("MaxFileDescriptorCount"), 0),
            )],
            ["Free RAM", fmt_bytes(os_bean.get("FreePhysicalMemorySize"))],
        ],
        aligns=["l", "l"],
    )

    subsection("Latency histograms (ms)")
    note("Focus metrics for UI slowness: DescribeConfigs TotalTimeMs vs LocalTimeMs vs ResponseSendTimeMs")
    for metric in METRICS_FOCUS:
        print()
        print("  %s" % metric)
        table(["request"] + KEY_LABELS, latency_rows(metric))

    # Highlight DescribeConfigs breakdown
    def pick(metric: str, req: str = "DescribeConfigs") -> Dict[str, Any]:
        return read_bean(
            "kafka.network:type=RequestMetrics,name=%s,request=%s" % (metric, req)
        ) or {}

    dc_total = pick("TotalTimeMs")
    dc_local = pick("LocalTimeMs")
    dc_send = pick("ResponseSendTimeMs")
    if dc_total:
        box(
            "DescribeConfigs spotlight (UI-heavy API)",
            [
                "LocalTimeMs     p95=%s  p99=%s   ← broker processing" % (
                    fmt_ms(dc_local.get("95thPercentile")),
                    fmt_ms(dc_local.get("99thPercentile")),
                ),
                "ResponseSendMs  p95=%s  p99=%s   ← sending response to client" % (
                    fmt_ms(dc_send.get("95thPercentile")),
                    fmt_ms(dc_send.get("99thPercentile")),
                ),
                "TotalTimeMs     p95=%s  p99=%s" % (
                    fmt_ms(dc_total.get("95thPercentile")),
                    fmt_ms(dc_total.get("99thPercentile")),
                ),
            ],
        )
        try:
            send_p95 = float(dc_send.get("95thPercentile") or 0)
            local_p95 = float(dc_local.get("95thPercentile") or 0)
            if send_p95 > 500 and send_p95 > local_p95 * 5:
                status(
                    "WATCH",
                    "ResponseSend dominates — broker OK; client/UI slow to drain large responses.",
                )
            elif local_p95 > 100:
                status("WATCH", "Local processing of DescribeConfigs is elevated.")
            else:
                status("OK", "DescribeConfigs local path looks fine.")
        except (TypeError, ValueError):
            pass

    subsection("Broker throughput (1m / 5m)")
    rate_rows = []
    for name in [
        "MessagesInPerSec",
        "BytesInPerSec",
        "BytesOutPerSec",
        "TotalFetchRequestsPerSec",
        "TotalProduceRequestsPerSec",
    ]:
        bean = "kafka.server:type=BrokerTopicMetrics,name=%s" % name
        value = read_bean(bean) or {}
        one = value.get("OneMinuteRate")
        five = value.get("FiveMinuteRate")
        if "Bytes" in name:
            rate_rows.append([name, fmt_bytes(one) + "/s" if one is not None else "—", fmt_bytes(five) + "/s" if five is not None else "—"])
        else:
            rate_rows.append([name, fmt_num(one, 1), fmt_num(five, 1)])
    table(["metric", "1m", "5m"], rate_rows)

    subsection("Admin API request rate (1m)")
    admin_rows = []
    for req in ["DescribeConfigs", "Metadata", "ListGroups", "DescribeGroups", "OffsetFetch"]:
        pattern = "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=%s,*" % req
        found = get("/search/" + urllib.parse.quote(pattern)).get("value") or []
        if not found:
            pattern = "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=%s" % req
            found = get("/search/" + urllib.parse.quote(pattern)).get("value") or []
        total = 0.0
        for bean in found:
            value = read_bean(bean) or {}
            total += float(value.get("OneMinuteRate") or 0.0)
        admin_rows.append([req, fmt_num(total, 3)])
    table(["request", "req/s (1m)"], admin_rows, aligns=["l", "r"])

    subsection("Connections by listener / processor")
    pattern = "kafka.server:type=socket-server-metrics,*"
    beans = get("/search/" + urllib.parse.quote(pattern)).get("value") or []
    conn_rows = []
    for bean in sorted(beans):
        if "clientSoftwareName" in bean:
            continue
        value = read_bean(bean)
        if not isinstance(value, dict) or "connection-count" not in value:
            continue
        # shorten bean label
        label = bean.replace("kafka.server:", "").replace(",type=socket-server-metrics", "")
        conn_rows.append(
            [
                label,
                fmt_num(value.get("connection-count"), 0),
                fmt_num(value.get("io-wait-ratio"), 3),
                fmt_num(value.get("connection-creation-rate"), 3),
            ]
        )
    table(["listener", "conns", "io-wait", "create/s"], conn_rows)

    print()
    bullet(
        "If DescribeConfigs ResponseSendTimeMs p95/p99 is seconds while LocalTimeMs is tens of ms, "
        "the broker is fine but the UI/client is slow draining responses."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
