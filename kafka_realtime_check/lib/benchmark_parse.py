#!/usr/bin/env python3
"""Parse kafka-producer-perf-test.sh and kafka-consumer-perf-test.sh outputs."""

from __future__ import annotations

import json
import re
import sys
from typing import Any, Dict, List, Optional

PRODUCER_SUMMARY_RE = re.compile(
    r"(?P<records>\d+)\s+records\s+sent,\s*"
    r"(?P<rps>[0-9.]+)\s+records/sec\s+\((?P<mbps>[0-9.]+)\s+MB/sec\),\s*"
    r"(?P<avg_ms>[0-9.]+)\s+ms\s+avg\s+latency,\s*"
    r"(?P<max_ms>[0-9.]+)\s+ms\s+max\s+latency,\s*"
    r"(?P<p50_ms>[0-9.]+)\s+ms\s+50th,\s*"
    r"(?P<p95_ms>[0-9.]+)\s+ms\s+95th,\s*"
    r"(?P<p99_ms>[0-9.]+)\s+ms\s+99th,\s*"
    r"(?P<p999_ms>[0-9.]+)\s+ms\s+99\.9th\.?",
    re.IGNORECASE,
)


def parse_producer_perf(text: str) -> Dict[str, Any]:
    """Extract the final summary line from kafka-producer-perf-test.sh output."""
    matches = list(PRODUCER_SUMMARY_RE.finditer(text or ""))
    if not matches:
        return {"ok": False, "error": "no producer summary line found", "raw": (text or "").strip()}
    m = matches[-1]
    return {
        "ok": True,
        "records_sent": int(m.group("records")),
        "records_per_sec": float(m.group("rps")),
        "mb_per_sec": float(m.group("mbps")),
        "avg_latency_ms": float(m.group("avg_ms")),
        "max_latency_ms": float(m.group("max_ms")),
        "p50_latency_ms": float(m.group("p50_ms")),
        "p95_latency_ms": float(m.group("p95_ms")),
        "p99_latency_ms": float(m.group("p99_ms")),
        "p999_latency_ms": float(m.group("p999_ms")),
    }


def parse_consumer_perf(text: str) -> Dict[str, Any]:
    """Parse header + CSV row emitted by kafka-consumer-perf-test.sh."""
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    header: Optional[List[str]] = None
    data_row: Optional[List[str]] = None
    for ln in lines:
        if "data.consumed.in.MB" in ln and "," in ln:
            header = [c.strip() for c in ln.split(",")]
            continue
        if header is not None and "," in ln and not ln.startswith("WARNING") and not ln.startswith("SLF4J"):
            parts = [c.strip() for c in ln.split(",")]
            if len(parts) == len(header):
                data_row = parts

    if not header or not data_row:
        return {"ok": False, "error": "no consumer CSV row found", "raw": (text or "").strip()}

    mapped = dict(zip(header, data_row))

    def _flt(key: str, default: float = 0.0) -> float:
        try:
            return float(mapped.get(key, default))
        except (TypeError, ValueError):
            return default

    def _int(key: str, default: int = 0) -> int:
        try:
            return int(float(mapped.get(key, default)))
        except (TypeError, ValueError):
            return default

    return {
        "ok": True,
        "start_time": mapped.get("start.time", ""),
        "end_time": mapped.get("end.time", ""),
        "data_consumed_mb": _flt("data.consumed.in.MB"),
        "mb_per_sec": _flt("MB.sec"),
        "records_consumed": _int("data.consumed.in.nMsg"),
        "records_per_sec": _flt("nMsg.sec"),
        "rebalance_time_ms": _int("rebalance.time.ms"),
        "fetch_time_ms": _int("fetch.time.ms"),
        "fetch_mb_per_sec": _flt("fetch.MB.sec"),
        "fetch_records_per_sec": _flt("fetch.nMsg.sec"),
    }


def summarize_timings_ms(samples_ms: List[float]) -> Dict[str, Any]:
    """Compute min/avg/max from a list of millisecond timings."""
    clean = [float(x) for x in samples_ms if x is not None]
    if not clean:
        return {"count": 0, "min_ms": 0.0, "avg_ms": 0.0, "max_ms": 0.0, "samples_ms": []}
    return {
        "count": len(clean),
        "cold_ms": round(clean[0], 2),
        "min_ms": round(min(clean), 2),
        "avg_ms": round(sum(clean) / len(clean), 2),
        "max_ms": round(max(clean), 2),
        "samples_ms": [round(x, 2) for x in clean],
    }


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        sys.stderr.write("Usage: benchmark_parse.py <producer|consumer|timings>\n")
        return 2
    mode = argv[1]
    raw = sys.stdin.read()
    if mode == "producer":
        out = parse_producer_perf(raw)
    elif mode == "consumer":
        out = parse_consumer_perf(raw)
    elif mode == "timings":
        nums = [float(x) for x in raw.split() if x.strip()]
        out = summarize_timings_ms(nums)
    else:
        sys.stderr.write(f"Unknown mode: {mode}\n")
        return 2
    print(json.dumps(out, sort_keys=True))
    return 0 if out.get("ok", True) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
