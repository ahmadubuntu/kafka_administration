#!/usr/bin/env python3
"""Estimate whether this Kafka node can sustain current load, and rough ceilings.

These are planning heuristics (not hard Kafka limits). Workload shape
(produce rate, fan-out, compaction, ISR, client misbehavior) matters more
than raw partition counts.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from format import (  # noqa: E402
    badge,
    box,
    bullet,
    fmt_bytes,
    fmt_num,
    note,
    progress_bar,
    section,
    subsection,
    table,
)


JOLOKIA = os.environ.get("KAFKA_JOLOKIA_URL", "http://127.0.0.1:8779/jolokia").rstrip("/")
JMX_URL = os.environ.get("KAFKA_JMX_METRICS_URL", "http://127.0.0.1:7071/metrics")
UNIT = os.environ.get("KAFKA_SYSTEMD_UNIT", "kafka")
SERVER_PROPS = os.environ.get(
    "KAFKA_SERVER_PROPERTIES", "/var/opt/kafka/config/server.properties"
)
LOG_DIR = os.environ.get("KAFKA_LOG_DIR", "/var/opt/kafka/logs")


def sh(cmd: str) -> str:
    try:
        return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.DEVNULL)
    except Exception:
        return ""


def jolokia_read(bean: str, attr: Optional[str] = None) -> Any:
    path = "/read/" + urllib.parse.quote(bean)
    if attr:
        path += "/" + urllib.parse.quote(attr)
    try:
        with urllib.request.urlopen(JOLOKIA + path, timeout=10) as resp:
            data = json.load(resp)
        return data.get("value")
    except Exception as exc:  # noqa: BLE001
        return {"_error": str(exc)}


def jmx_gauge(name_substr: str) -> Optional[float]:
    try:
        with urllib.request.urlopen(JMX_URL, timeout=15) as resp:
            text = resp.read().decode("utf-8", "replace")
    except Exception:
        return None
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        if name_substr in line:
            parts = line.rsplit(None, 1)
            if len(parts) == 2:
                try:
                    return float(parts[1])
                except ValueError:
                    pass
    return None


def parse_props(path: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    except OSError:
        pass
    return out


def kafka_pid() -> Optional[int]:
    out = sh("systemctl show -p MainPID --value %s" % UNIT).strip()
    if out.isdigit() and out != "0":
        return int(out)
    out = sh("pgrep -n -f 'kafka.Kafka'").strip()
    if out.isdigit():
        return int(out)
    return None


def read_limits(pid: int) -> Tuple[Optional[int], Optional[int]]:
    soft = hard = None
    try:
        with open("/proc/%d/limits" % pid, encoding="utf-8") as fh:
            for line in fh:
                if "open files" in line.lower():
                    parts = line.split()
                    nums = [p for p in parts if p.isdigit()]
                    if len(nums) >= 2:
                        soft, hard = int(nums[0]), int(nums[1])
    except OSError:
        pass
    return soft, hard


def meminfo() -> Dict[str, int]:
    info: Dict[str, int] = {}
    try:
        with open("/proc/meminfo", encoding="utf-8") as fh:
            for line in fh:
                m = re.match(r"(\w+):\s+(\d+)", line)
                if m:
                    info[m.group(1)] = int(m.group(2)) * 1024
    except OSError:
        pass
    return info


def level(util: float) -> str:
    if util < 0.50:
        return "OK"
    if util < 0.75:
        return "WATCH"
    if util < 0.90:
        return "TIGHT"
    return "CRITICAL"


def worst(*levels: str) -> str:
    order = {"OK": 0, "WATCH": 1, "TIGHT": 2, "CRITICAL": 3, "UNKNOWN": -1}
    return max(levels, key=lambda x: order.get(x, -1))


def util_vs(value: float, comfort: float, caution: float, stretch: float) -> Tuple[float, str]:
    if stretch <= 0:
        return 0.0, "UNKNOWN"
    if value <= comfort:
        return value / max(comfort, 1), "OK"
    if value <= caution:
        return value / max(caution, 1), "WATCH"
    if value <= stretch:
        return value / max(stretch, 1), "TIGHT"
    return value / max(stretch, 1), "CRITICAL"


def main() -> int:
    section("11  Capacity / headroom estimate")
    note("Heuristic planning only — validate under your real workload.")

    props = parse_props(SERVER_PROPS)
    net_threads = int(props.get("num.network.threads", "3") or 3)
    io_threads = int(props.get("num.io.threads", "8") or 8)

    pid = kafka_pid()
    soft_fd, hard_fd = (None, None)
    rss = None
    pcpu = None
    if pid:
        soft_fd, hard_fd = read_limits(pid)
        st = sh("ps -p %d -o rss=,pcpu= --no-headers" % pid).split()
        if len(st) >= 2:
            rss = int(st[0]) * 1024
            try:
                pcpu = float(st[1])
            except ValueError:
                pcpu = None

    os_bean = jolokia_read("java.lang:type=OperatingSystem")
    if not isinstance(os_bean, dict):
        os_bean = {}
    open_fd = os_bean.get("OpenFileDescriptorCount")
    max_fd = os_bean.get("MaxFileDescriptorCount") or hard_fd or soft_fd
    proc_cpu = os_bean.get("ProcessCpuLoad")
    sys_cpu = os_bean.get("CpuLoad")
    cores = int(os_bean.get("AvailableProcessors") or sh("nproc").strip() or 1)

    topics = jmx_gauge("globaltopiccount_value")
    partitions = jmx_gauge("globalpartitioncount_value")
    urp = jmx_gauge("underreplicatedpartitions_value")
    offline = jmx_gauge("offlinepartitionscount_value")

    conn_9094 = sh("ss -tn state established '( sport = :9094 )' 2>/dev/null | tail -n +2 | wc -l").strip()
    conn_9092 = sh("ss -tn state established '( sport = :9092 )' 2>/dev/null | tail -n +2 | wc -l").strip()
    try:
        connections = int(conn_9094 or 0) + int(conn_9092 or 0)
    except ValueError:
        connections = 0

    mem = meminfo()
    mem_total = mem.get("MemTotal", 0)
    mem_avail = mem.get("MemAvailable", 0)
    cached = mem.get("Cached", 0) + mem.get("Buffers", 0)

    heap = jolokia_read("java.lang:type=Memory")
    heap_used = heap_max = None
    if isinstance(heap, dict) and isinstance(heap.get("HeapMemoryUsage"), dict):
        heap_used = heap["HeapMemoryUsage"].get("used")
        heap_max = heap["HeapMemoryUsage"].get("max")

    try:
        log_files_n = int(sh("find %s -type f 2>/dev/null | wc -l" % LOG_DIR).strip() or 0)
    except ValueError:
        log_files_n = 0

    disk = sh("df -B1 --output=size,used,avail,pcent %s 2>/dev/null | tail -1" % LOG_DIR).split()
    disk_size = disk_used = disk_avail = None
    disk_pct = None
    if len(disk) >= 4:
        try:
            disk_size, disk_used, disk_avail = int(disk[0]), int(disk[1]), int(disk[2])
            disk_pct = float(disk[3].rstrip("%"))
        except ValueError:
            pass

    subsection("Observed")
    table(
        ["metric", "value"],
        [
            ["pid", pid if pid is not None else "—"],
            ["CPU cores", cores],
            ["network / io threads", "%d / %d" % (net_threads, io_threads)],
            ["topics / partitions", "%s / %s" % (fmt_num(topics, 0), fmt_num(partitions, 0))],
            ["URP / offline", "%s / %s" % (fmt_num(urp, 0), fmt_num(offline, 0))],
            [
                "connections (:9094/:9092)",
                "%d  (%s / %s)" % (connections, conn_9094 or "0", conn_9092 or "0"),
            ],
            [
                "open FD / max",
                "%s / %s  (ulimit %s/%s)"
                % (fmt_num(open_fd, 0), fmt_num(max_fd, 0), soft_fd, hard_fd),
            ],
            ["log files on disk", "%s  (not all held open)" % fmt_num(log_files_n, 0)],
            ["kafka RSS", fmt_bytes(rss) if rss is not None else "—"],
            [
                "kafka %%CPU (ps)",
                ("%.1f  (~%.0f%% of machine)" % (pcpu, pcpu / cores)) if pcpu is not None else "—",
            ],
            [
                "ProcessCpuLoad",
                ("%.1f%%" % (float(proc_cpu) * 100)) if proc_cpu is not None else "—",
            ],
            ["system CpuLoad", ("%.1f%%" % (float(sys_cpu) * 100)) if sys_cpu is not None else "—"],
            [
                "RAM total / avail",
                "%s / %s  (cache+buff %s)"
                % (fmt_bytes(mem_total), fmt_bytes(mem_avail), fmt_bytes(cached))
                if mem_total
                else "—",
            ],
            [
                "Heap used / max",
                "%s / %s  (%.0f%%)"
                % (fmt_bytes(heap_used), fmt_bytes(heap_max), 100.0 * heap_used / heap_max)
                if heap_used is not None and heap_max
                else "—",
            ],
            [
                "log.dirs disk",
                "%.0f%% used  (%s / %s, avail %s)"
                % (
                    disk_pct,
                    fmt_bytes(disk_used),
                    fmt_bytes(disk_size),
                    fmt_bytes(disk_avail),
                )
                if disk_pct is not None and disk_size
                else "—",
            ],
        ],
        aligns=["l", "l"],
    )

    max_fd_i = int(max_fd or soft_fd or 0)
    open_fd_i = int(open_fd or 0)
    parts_i = int(partitions or 0)

    fd_safe = int(max_fd_i * 0.70) if max_fd_i else 0
    fd_part_ceiling = max(0, (fd_safe - 2048 - connections) // 2) if fd_safe else 0
    fd_conn_ceiling = max(0, fd_safe - 2048 - 2 * parts_i) if fd_safe else 0

    part_comfort = cores * 500
    part_caution = min(cores * 1000, 4000)
    part_stretch = min(cores * 1500, 8000)
    if heap_max:
        heap_part = int(max(0, (heap_max / 1024**2) - 1536) / 0.75)
        part_caution = min(part_caution, heap_part)
        part_stretch = min(part_stretch, int(heap_part * 1.5))
    if fd_part_ceiling:
        part_stretch = min(part_stretch, fd_part_ceiling)
        part_caution = min(part_caution, fd_part_ceiling)

    conn_comfort = max(2000, cores * 400)
    conn_caution = max(5000, cores * 1000)
    conn_stretch = max(10000, cores * 2000)
    if fd_conn_ceiling:
        conn_stretch = min(conn_stretch, fd_conn_ceiling)
        conn_caution = min(conn_caution, fd_conn_ceiling)
    if net_threads < max(3, cores // 2):
        conn_caution = min(conn_caution, max(conn_comfort, net_threads * 1000))
        conn_stretch = min(conn_stretch, max(conn_caution, net_threads * 2000))
    if conn_caution < conn_comfort:
        conn_caution = conn_comfort
    if conn_stretch < conn_caution:
        conn_stretch = conn_caution

    subsection("Estimated ceilings (single broker)")
    table(
        ["resource", "comfort", "caution", "stretch"],
        [
            ["partitions", fmt_num(part_comfort, 0), fmt_num(part_caution, 0), fmt_num(part_stretch, 0)],
            ["connections", fmt_num(conn_comfort, 0), fmt_num(conn_caution, 0), fmt_num(conn_stretch, 0)],
            ["open FD warn@", "—", fmt_num(fd_safe, 0) + " (70%)", fmt_num(max_fd_i, 0)],
        ],
    )
    note("Bounded by cores, heap, FD budget, num.network.threads, classic ~4k partitions/broker.")

    _, part_l = util_vs(parts_i, part_comfort, part_caution, part_stretch)
    _, conn_l = util_vs(connections, conn_comfort, conn_caution, conn_stretch)

    fd_util = (open_fd_i / max_fd_i) if max_fd_i else 0.0
    fd_l = level(fd_util)

    cpu_frac = None
    if proc_cpu is not None:
        cpu_frac = float(proc_cpu)
    elif pcpu is not None and cores:
        cpu_frac = min(1.0, (pcpu / 100.0) / cores)
    cpu_l = level(cpu_frac) if cpu_frac is not None else "UNKNOWN"

    mem_util = 0.0
    mem_l = "UNKNOWN"
    if mem_total and mem_avail is not None:
        mem_util = 1.0 - (mem_avail / mem_total)
        mem_l = level(mem_util)

    heap_util = 0.0
    heap_l = "UNKNOWN"
    if heap_used and heap_max:
        heap_util = heap_used / heap_max
        heap_l = level(heap_util)

    disk_l = "UNKNOWN"
    if disk_pct is not None:
        disk_l = level(disk_pct / 100.0)

    overall = worst(part_l, conn_l, fd_l, cpu_l, mem_l, heap_l, disk_l)

    # progress vs stretch/caution for visual
    part_ratio = parts_i / part_caution if part_caution else 0
    conn_ratio = connections / conn_caution if conn_caution else 0

    subsection("Headroom scorecard")
    table(
        ["axis", "status", "usage", "detail"],
        [
            [
                "partitions",
                badge(part_l),
                progress_bar(min(part_ratio, 1.0)),
                "%d / caution %d" % (parts_i, part_caution),
            ],
            [
                "connections",
                badge(conn_l),
                progress_bar(min(conn_ratio, 1.0)),
                "%d / caution %d" % (connections, conn_caution),
            ],
            [
                "open files",
                badge(fd_l),
                progress_bar(fd_util),
                "%.1f%% of %s" % (fd_util * 100.0, fmt_num(max_fd_i, 0)),
            ],
            [
                "CPU",
                badge(cpu_l),
                progress_bar(cpu_frac or 0.0),
                ("ProcessCpuLoad %.0f%%" % (cpu_frac * 100)) if cpu_frac is not None else "n/a",
            ],
            [
                "RAM pressure",
                badge(mem_l),
                progress_bar(mem_util),
                "≈%.0f%% of RAM not Available" % (mem_util * 100),
            ],
            [
                "Heap",
                badge(heap_l),
                progress_bar(heap_util),
                "%.0f%%" % (heap_util * 100),
            ],
            [
                "Disk logs",
                badge(disk_l),
                progress_bar((disk_pct or 0) / 100.0),
                ("%.0f%%" % disk_pct) if disk_pct is not None else "n/a",
            ],
        ],
        aligns=["l", "l", "l", "l"],
    )

    if overall in ("OK", "WATCH"):
        verdict = "Node can carry the current footprint (partitions / connections / FD / disk)."
    elif overall == "TIGHT":
        verdict = "Near caution bands — OK short-term; avoid large growth without scale-out/tuning."
    else:
        verdict = "Beyond stretch heuristics or critical on one axis — plan scale-out or reduce load."

    box(
        "OVERALL  %s" % overall,
        [
            verdict,
            "Headroom to caution →  +%d partitions · +%d connections · +%d FDs"
            % (
                max(0, part_caution - parts_i),
                max(0, conn_caution - connections),
                max(0, fd_safe - open_fd_i) if max_fd_i else 0,
            ),
        ],
    )

    subsection("Practical notes")
    notes = []
    if parts_i and part_caution and parts_i > part_comfort:
        notes.append("Partition count is above comfort; prefer fewer partitions for new topics.")
    if connections > conn_comfort:
        notes.append("Connection count elevated; check idle clients / pooling.")
    if net_threads < cores and connections > 1000:
        notes.append(
            "Consider raising num.network.threads (now %d) toward ~%d."
            % (net_threads, max(cores, 8))
        )
    if fd_util < 0.2 and max_fd_i >= 100000:
        notes.append("FD limit is generous — open files are NOT the bottleneck today.")
    if cpu_frac is not None and cpu_frac >= 0.4:
        notes.append("Broker CPU is meaningful; UI/admin storms (DescribeConfigs) will hurt more.")
    if mem_avail and mem_avail < 2 * 1024**3:
        notes.append("MemAvailable < 2GiB: page cache pressure risk.")
    elif mem_avail:
        notes.append("MemAvailable looks healthy for page cache.")
    if disk_pct is not None and disk_pct >= 70:
        notes.append("log.dirs disk getting full — nearer limit than FD.")
    if not notes:
        notes.append("No extra warnings for this profile.")
    for n in notes:
        bullet(n)

    return 0


if __name__ == "__main__":
    sys.exit(main())
