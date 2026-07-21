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
                    # Max open files  soft hard units
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
                    info[m.group(1)] = int(m.group(2)) * 1024  # bytes
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


def main() -> int:
    print("============================================================")
    print(" 11 Capacity / headroom estimate (heuristic)")
    print("============================================================")
    print("NOTE: Estimates for planning only — validate under your workload.")
    print()

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
    proc_cpu = os_bean.get("ProcessCpuLoad")  # 0..1
    sys_cpu = os_bean.get("CpuLoad")
    cores = int(os_bean.get("AvailableProcessors") or sh("nproc").strip() or 1)

    topics = jmx_gauge("globaltopiccount_value")
    partitions = jmx_gauge("globalpartitioncount_value")
    urp = jmx_gauge("underreplicatedpartitions_value")
    offline = jmx_gauge("offlinepartitionscount_value")

    # connections on client listeners
    conn_9094 = sh("ss -tn state established '( sport = :9094 )' 2>/dev/null | tail -n +2 | wc -l").strip()
    conn_9092 = sh("ss -tn state established '( sport = :9092 )' 2>/dev/null | tail -n +2 | wc -l").strip()
    try:
        connections = int(conn_9094 or 0) + int(conn_9092 or 0)
    except ValueError:
        connections = 0

    mem = meminfo()
    mem_total = mem.get("MemTotal", 0)
    mem_avail = mem.get("MemAvailable", 0)
    mem_free = mem.get("MemFree", 0)
    cached = mem.get("Cached", 0) + mem.get("Buffers", 0)

    heap = jolokia_read("java.lang:type=Memory")
    heap_used = heap_max = None
    if isinstance(heap, dict) and isinstance(heap.get("HeapMemoryUsage"), dict):
        heap_used = heap["HeapMemoryUsage"].get("used")
        heap_max = heap["HeapMemoryUsage"].get("max")

    log_files = sh("find %s -type f 2>/dev/null | wc -l" % LOG_DIR).strip()
    try:
        log_files_n = int(log_files or 0)
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

    print("--- Observed ---")
    print("pid                 :", pid)
    print("cpu_cores           :", cores)
    print("num.network.threads :", net_threads)
    print("num.io.threads      :", io_threads)
    print("topics / partitions :", topics, "/", partitions)
    print("URP / offline       :", urp, "/", offline)
    print("connections :9092+9094 :", connections, "(9094=%s 9092=%s)" % (conn_9094, conn_9092))
    print("open_fd / max_fd    :", open_fd, "/", max_fd, "(ulimit soft/hard=%s/%s)" % (soft_fd, hard_fd))
    print("log segment files   :", log_files_n, "(on disk; not all held open)")
    if rss is not None:
        print("kafka RSS           : %.1f GiB" % (rss / 1024**3))
    if pcpu is not None:
        print("kafka %%CPU (ps)     : %.1f (of one core; across %d cores ~%.0f%% machine)" % (
            pcpu, cores, (pcpu / cores) if cores else 0
        ))
    if proc_cpu is not None:
        print("ProcessCpuLoad      : %.1f%%" % (float(proc_cpu) * 100))
    if sys_cpu is not None:
        print("CpuLoad (system)    : %.1f%%" % (float(sys_cpu) * 100))
    if mem_total:
        print(
            "RAM total/avail     : %.1f / %.1f GiB  (cache+buff ~%.1f GiB)"
            % (mem_total / 1024**3, mem_avail / 1024**3, cached / 1024**3)
        )
    if heap_used is not None and heap_max:
        print(
            "Heap used/max       : %.1f / %.1f GiB (%.0f%%)"
            % (heap_used / 1024**3, heap_max / 1024**3, 100.0 * heap_used / heap_max)
        )
    if disk_pct is not None and disk_size:
        print(
            "log.dirs disk       : %.0f%% used (%.1f / %.1f GiB, avail %.1f GiB)"
            % (disk_pct, disk_used / 1024**3, disk_size / 1024**3, disk_avail / 1024**3)
        )

    # ---- ceilings (heuristics) ----
    # FD: keep 50% headroom; budget for conns + ~2 FD/partition (index+log hot) + base 2048
    max_fd_i = int(max_fd or soft_fd or 0)
    open_fd_i = int(open_fd or 0)
    parts_i = int(partitions or 0)
    topics_i = int(topics or 0)

    fd_safe = int(max_fd_i * 0.70) if max_fd_i else 0
    # conservative open-FD model at ceiling: base + connections + 2*partitions
    # invert for max partitions given current connections
    fd_part_ceiling = max(0, (fd_safe - 2048 - connections) // 2) if fd_safe else 0
    # connection ceiling from FD budget (assume ~1 FD/conn + 2*current_partitions + base)
    fd_conn_ceiling = max(0, fd_safe - 2048 - 2 * parts_i) if fd_safe else 0

    # Partition ceilings from common planning bands + resources
    # - comfort ~ cores*500 to cores*800
    # - caution absolute ~4000/broker (classic guidance)
    # - stretch ~8000 with modern Kafka if load is light
    part_comfort = cores * 500
    part_caution = min(cores * 1000, 4000)
    part_stretch = min(cores * 1500, 8000)
    if heap_max:
        # ~0.5–1 MiB broker metadata/buffers ballpark per partition on heap pressure side
        heap_part = int(max(0, (heap_max / 1024**2) - 1536) / 0.75)
        part_caution = min(part_caution, heap_part)
        part_stretch = min(part_stretch, int(heap_part * 1.5))
    if fd_part_ceiling:
        part_stretch = min(part_stretch, fd_part_ceiling)
        part_caution = min(part_caution, fd_part_ceiling)

    # Connection ceilings
    # network threads can multiplex many idle conns; memory/FD dominate
    conn_comfort = max(2000, cores * 400)
    conn_caution = max(5000, cores * 1000)
    conn_stretch = max(10000, cores * 2000)
    if fd_conn_ceiling:
        conn_stretch = min(conn_stretch, fd_conn_ceiling)
        conn_caution = min(conn_caution, fd_conn_ceiling)
    # with few network threads, compress upper bands (keep comfort <= caution <= stretch)
    if net_threads < max(3, cores // 2):
        conn_caution = min(conn_caution, max(conn_comfort, net_threads * 1000))
        conn_stretch = min(conn_stretch, max(conn_caution, net_threads * 2000))
    if conn_caution < conn_comfort:
        conn_caution = conn_comfort
    if conn_stretch < conn_caution:
        conn_stretch = conn_caution

    print()
    print("--- Estimated ceilings (single broker, planning bands) ---")
    print("Partitions / broker :")
    print("  comfort ~%d   caution ~%d   stretch ~%d" % (part_comfort, part_caution, part_stretch))
    print("Connections / broker:")
    print("  comfort ~%d   caution ~%d   stretch ~%d" % (conn_comfort, conn_caution, conn_stretch))
    print("Open files          :")
    print("  warn >70%% of max  (70%% of %d = %d)" % (max_fd_i, fd_safe))
    print()
    print("How ceilings were bounded:")
    print("  - partitions: cores, heap size, FD budget, classic ~4k caution/broker")
    print("  - connections: cores, num.network.threads, FD budget")
    print("  - FD model: open ≈ base(2k) + connections + ~2 * partitions (rough)")

    # ---- utilization vs comfort/caution ----
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

    part_u, part_l = util_vs(parts_i, part_comfort, part_caution, part_stretch)
    conn_u, conn_l = util_vs(connections, conn_comfort, conn_caution, conn_stretch)

    fd_util = (open_fd_i / max_fd_i) if max_fd_i else 0.0
    fd_l = level(fd_util)

    cpu_frac = None
    if proc_cpu is not None:
        cpu_frac = float(proc_cpu)
    elif pcpu is not None and cores:
        cpu_frac = min(1.0, (pcpu / 100.0) / cores)
    cpu_l = level(cpu_frac) if cpu_frac is not None else "UNKNOWN"

    mem_l = "UNKNOWN"
    mem_util = 0.0
    if mem_total and mem_avail is not None:
        # pressure if available is small fraction of total
        mem_util = 1.0 - (mem_avail / mem_total)
        mem_l = level(mem_util)

    heap_l = "UNKNOWN"
    heap_util = 0.0
    if heap_used and heap_max:
        heap_util = heap_used / heap_max
        heap_l = level(heap_util)

    disk_l = "UNKNOWN"
    if disk_pct is not None:
        disk_l = level(disk_pct / 100.0)

    overall = worst(part_l, conn_l, fd_l, cpu_l, mem_l, heap_l, disk_l)

    print()
    print("--- Headroom scorecard ---")
    print("partitions : %-8s  (%d vs comfort %d / caution %d / stretch %d)" % (
        part_l, parts_i, part_comfort, part_caution, part_stretch))
    print("connections: %-8s  (%d vs comfort %d / caution %d / stretch %d)" % (
        conn_l, connections, conn_comfort, conn_caution, conn_stretch))
    print("open files : %-8s  (%.1f%% of max %s)" % (fd_l, fd_util * 100.0, max_fd_i))
    print("CPU        : %-8s  (%s)" % (
        cpu_l,
        ("ProcessCpuLoad %.0f%%" % (cpu_frac * 100)) if cpu_frac is not None else "n/a",
    ))
    print("RAM avail  : %-8s  (≈%.0f%% of RAM not Available)" % (mem_l, mem_util * 100))
    print("Heap       : %-8s  (%.0f%%)" % (heap_l, heap_util * 100))
    print("Disk logs  : %-8s  (%s)" % (disk_l, ("%.0f%%" % disk_pct) if disk_pct is not None else "n/a"))
    print()
    print("OVERALL    :", overall)

    print()
    print("--- Verdict ---")
    if overall in ("OK", "WATCH"):
        print(
            "This node looks able to carry the *current* footprint "
            "(partitions/connections/FD/disk)."
        )
    elif overall == "TIGHT":
        print(
            "Node is near planning caution bands. Fine short-term, but avoid "
            "large growth without more brokers / threads / RAM tuning."
        )
    else:
        print(
            "Node is beyond stretch heuristics or critically utilized on one axis. "
            "Plan scale-out or reduce partitions/connections."
        )

    # specific advice for this profile
    print()
    print("--- Practical notes for this profile ---")
    if parts_i and part_caution and parts_i > part_comfort:
        print("- Partition count is above 'comfort'; prefer fewer partitions for new topics.")
    if connections > conn_comfort:
        print("- Connection count is elevated; check idle clients / connection pooling.")
    if net_threads < cores and connections > 1000:
        print(
            "- Consider raising num.network.threads (now %d) toward ~%d on this host."
            % (net_threads, max(cores, 8))
        )
    if fd_util < 0.2 and max_fd_i >= 100000:
        print("- FD limit is generous; open files are NOT the bottleneck today.")
    if cpu_frac is not None and cpu_frac >= 0.4:
        print("- Broker CPU is meaningful; UI/admin storms (DescribeConfigs) will hurt more.")
    if mem_avail and mem_avail < 2 * 1024**3:
        print("- MemAvailable < 2GiB: page cache pressure risk for Kafka.")
    elif mem_avail:
        print("- MemAvailable looks healthy for page cache (important for Kafka reads).")
    if disk_pct is not None and disk_pct >= 70:
        print("- log.dirs disk is getting full; retention/growth is the nearer limit than FD.")

    # remaining headroom numbers
    print()
    print("--- Rough remaining headroom (to caution band) ---")
    print("extra partitions until caution : ~%d" % max(0, part_caution - parts_i))
    print("extra connections until caution: ~%d" % max(0, conn_caution - connections))
    if max_fd_i:
        print("extra open FDs until 70%%     : ~%d" % max(0, fd_safe - open_fd_i))

    return 0


if __name__ == "__main__":
    sys.exit(main())
