#!/usr/bin/env python3
"""Read Kafka request latency histograms from Jolokia.

Focuses on admin/UI-heavy APIs (DescribeConfigs, Metadata, groups) and
breaks TotalTimeMs into queue / local / remote / response-send components.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional

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
METRICS = [
    "TotalTimeMs",
    "RequestQueueTimeMs",
    "LocalTimeMs",
    "RemoteTimeMs",
    "ResponseQueueTimeMs",
    "ResponseSendTimeMs",
    "ThrottleTimeMs",
]
KEYS = [
    "Count",
    "Mean",
    "Max",
    "50thPercentile",
    "95thPercentile",
    "99thPercentile",
    "999thPercentile",
]


def get(path: str) -> Dict[str, Any]:
    url = JOLOKIA + path
    with urllib.request.urlopen(url, timeout=15) as resp:
        return json.load(resp)


def fmt(x: Any, width: int = 12) -> str:
    if x is None:
        return "-".rjust(width)
    if isinstance(x, float):
        return ("%.3f" % x).rjust(width)
    return str(x).rjust(width)


def read_bean(bean: str) -> Optional[Dict[str, Any]]:
    data = get("/read/" + urllib.parse.quote(bean))
    value = data.get("value")
    if isinstance(value, dict):
        return value
    # scalar gauges like {"Value": 0}
    if value is not None:
        return {"Value": value}
    return None


def print_table(metric: str) -> None:
    print()
    print("###", metric)
    print("request".ljust(18), " ".join(k.rjust(12) for k in KEYS))
    for req in REQUESTS:
        bean = "kafka.network:type=RequestMetrics,name=%s,request=%s" % (metric, req)
        value = read_bean(bean)
        if not value:
            print(req.ljust(18), "missing")
            continue
        print(req.ljust(18), " ".join(fmt(value.get(k)) for k in KEYS))


def main() -> int:
    print("Jolokia URL:", JOLOKIA)
    try:
        ver = get("/version")
        print("agent:", (ver.get("value") or {}).get("agent"))
    except Exception as exc:  # noqa: BLE001
        print("ERROR: cannot reach Jolokia:", exc, file=sys.stderr)
        return 1

    # Health gauges
    for bean in [
        "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions",
        "kafka.controller:type=KafkaController,name=OfflinePartitionsCount",
        "kafka.controller:type=KafkaController,name=GlobalTopicCount",
        "kafka.controller:type=KafkaController,name=GlobalPartitionCount",
        "kafka.controller:type=KafkaController,name=ActiveControllerCount",
        "kafka.network:type=RequestChannel,name=RequestQueueSize",
        "kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent",
    ]:
        value = read_bean(bean)
        print(bean, "=>", value)

    idle = read_bean(
        "kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent"
    )
    print("RequestHandlerAvgIdlePercent =>", idle)

    os_bean = read_bean("java.lang:type=OperatingSystem") or {}
    print(
        "OS ProcessCpuLoad=",
        os_bean.get("ProcessCpuLoad"),
        "CpuLoad=",
        os_bean.get("CpuLoad"),
        "OpenFD=",
        os_bean.get("OpenFileDescriptorCount"),
        "FreeMem=",
        os_bean.get("FreePhysicalMemorySize"),
    )

    print()
    print("## Latency histograms (ms)")
    for metric in METRICS:
        print_table(metric)

    print()
    print("## BrokerTopicMetrics rates")
    for name in [
        "MessagesInPerSec",
        "BytesInPerSec",
        "BytesOutPerSec",
        "TotalFetchRequestsPerSec",
        "TotalProduceRequestsPerSec",
    ]:
        bean = "kafka.server:type=BrokerTopicMetrics,name=%s" % name
        value = read_bean(bean) or {}
        print(
            name,
            "1m=",
            value.get("OneMinuteRate"),
            "5m=",
            value.get("FiveMinuteRate"),
            "count=",
            value.get("Count"),
        )

    print()
    print("## RequestsPerSec (admin APIs)")
    for req in ["DescribeConfigs", "Metadata", "ListGroups", "DescribeGroups", "OffsetFetch"]:
        pattern = (
            "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=%s,*" % req
        )
        found = get("/search/" + urllib.parse.quote(pattern)).get("value") or []
        if not found:
            pattern = (
                "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=%s" % req
            )
            found = get("/search/" + urllib.parse.quote(pattern)).get("value") or []
        total = 0.0
        for bean in found:
            value = read_bean(bean) or {}
            rate = float(value.get("OneMinuteRate") or 0.0)
            total += rate
            print(" ", bean, "1m=", value.get("OneMinuteRate"))
        print(req, "TOTAL_1m=", total)

    print()
    print("## EXTERNAL / PLAINTEXT connection-count by networkProcessor")
    pattern = "kafka.server:type=socket-server-metrics,*"
    beans = get("/search/" + urllib.parse.quote(pattern)).get("value") or []
    for bean in sorted(beans):
        if "clientSoftwareName" in bean:
            continue
        value = read_bean(bean)
        if not isinstance(value, dict):
            continue
        if "connection-count" not in value:
            continue
        print(
            bean,
            "connection-count=",
            value.get("connection-count"),
            "io-wait-ratio=",
            value.get("io-wait-ratio"),
            "connection-creation-rate=",
            value.get("connection-creation-rate"),
        )

    print()
    print(
        "HINT: If DescribeConfigs ResponseSendTimeMs p95/p99 is seconds-long while "
        "LocalTimeMs is tens of ms, the broker is fine but clients/UI are slow to "
        "drain large config responses (typical Kafka UI symptom)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
