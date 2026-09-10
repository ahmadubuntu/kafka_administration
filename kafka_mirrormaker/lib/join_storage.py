#!/usr/bin/env python3
"""Join source/dest storage files and print a human report + extra-bytes TSV."""
from __future__ import annotations

import argparse
import os
import sys


def _ensure_mm_lib() -> None:
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.environ.get("MM_LIB", ""),
        here,
        os.path.join(here, "lib"),
        os.path.join(os.path.dirname(here), "lib"),
    ]
    for cand in candidates:
        if cand and os.path.isfile(os.path.join(cand, "mm2_parse.py")):
            if cand not in sys.path:
                sys.path.insert(0, cand)
            return
    raise ModuleNotFoundError(
        "mm2_parse.py not found (need kafka_mirrormaker/lib on PYTHONPATH or MM_LIB)"
    )


_ensure_mm_lib()
from mm2_parse import (  # noqa: E402
    is_mm2_internal,
    map_dest_to_source,
    map_source_to_dest,
    parse_topic_configs,
    parse_topic_describe,
)


def load_topic_totals(path: str) -> dict[str, tuple[int, int]]:
    out = {}
    if not path or not os.path.isfile(path):
        return out
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 3:
                continue
            out[parts[0]] = (int(parts[1]), int(parts[2]))
    return out


def load_broker_totals(path: str) -> dict[str, int]:
    out = {}
    if not os.path.isfile(path):
        return out
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2:
                out[parts[0]] = int(parts[1])
    return out


def gib(n: int) -> str:
    return f"{n / 1073741824:.2f}"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--policy", default="default")
    p.add_argument("--source-alias", default="prod")
    p.add_argument("--dest-alias", default="dr")
    p.add_argument("--src-brokers")
    p.add_argument("--dst-brokers")
    p.add_argument("--src-topics")
    p.add_argument("--dst-topics")
    p.add_argument("--src-describe")
    p.add_argument("--dst-describe")
    p.add_argument("--src-configs", default="")
    p.add_argument("--dst-configs", default="")
    p.add_argument("--extra-tsv", default="")
    p.add_argument("--top", type=int, default=30)
    args = p.parse_args()

    src_b = load_broker_totals(args.src_brokers)
    dst_b = load_broker_totals(args.dst_brokers)
    src_t = load_topic_totals(args.src_topics)
    dst_t = load_topic_totals(args.dst_topics)
    src_d = parse_topic_describe(open(args.src_describe, encoding="utf-8", errors="replace").read()) if args.src_describe and os.path.isfile(args.src_describe) else {}
    dst_d = parse_topic_describe(open(args.dst_describe, encoding="utf-8", errors="replace").read()) if args.dst_describe and os.path.isfile(args.dst_describe) else {}
    src_c = parse_topic_configs(open(args.src_configs, encoding="utf-8", errors="replace").read()) if args.src_configs and os.path.isfile(args.src_configs) else {}
    dst_c = parse_topic_configs(open(args.dst_configs, encoding="utf-8", errors="replace").read()) if args.dst_configs and os.path.isfile(args.dst_configs) else {}

    def mean(d: dict[str, int]) -> float:
        if not d:
            return 0.0
        return sum(d.values()) / len(d)

    print("=== Broker log-dirs (replicated bytes on each node) ===")
    print(f"{'side':<8} {'brokers':>8} {'cluster_GiB':>12} {'mean_GiB/broker':>16} {'min_GiB':>10} {'max_GiB':>10}")
    for label, d in (("source", src_b), ("dest", dst_b)):
        if not d:
            print(f"{label:<8} {'0':>8} {'-':>12} {'-':>16} {'-':>10} {'-':>10}")
            continue
        vals = list(d.values())
        print(
            f"{label:<8} {len(d):>8} {gib(sum(vals)):>12} {gib(int(mean(d))):>16} {gib(min(vals)):>10} {gib(max(vals)):>10}"
        )
    print()
    print("Per-broker dest:")
    for b, s in sorted(dst_b.items(), key=lambda x: -x[1]):
        print(f"  broker {b}: {gib(s)} GiB")
    print("Per-broker source:")
    for b, s in sorted(src_b.items(), key=lambda x: -x[1]):
        print(f"  broker {b}: {gib(s)} GiB")
    print()
    if len(src_b) and len(dst_b) and len(src_b) != len(dst_b):
        print(
            f"NOTE: broker count differs (source={len(src_b)} dest={len(dst_b)}). "
            "Per-broker GiB can look larger on dest even if cluster unique data is similar."
        )
        print()

    policy = args.policy
    sa, da = args.source_alias, args.dest_alias

    extras = []
    dest_only = []
    source_only = []
    mapped_src = set()
    cfg_diffs = []

    for st, (sraw, suniq) in src_t.items():
        dt = map_source_to_dest(st, policy, sa)
        mapped_src.add(st)
        if dt not in dst_t:
            source_only.append(st)
            continue
        draw, duniq = dst_t[dt]
        extra = duniq - suniq
        extras.append((extra, st, dt, suniq, duniq, sraw, draw))
        sc = src_c.get(st, {})
        dc = dst_c.get(dt, {})
        for key in ("retention.ms", "retention.bytes", "cleanup.policy"):
            if sc.get(key) and dc.get(key) and sc.get(key) != dc.get(key):
                cfg_diffs.append((st, dt, key, sc.get(key), dc.get(key)))

    for dt in dst_t:
        src_name = map_dest_to_source(dt, policy, sa)
        kind = is_mm2_internal(dt, sa, da)
        if src_name is None or src_name not in src_t:
            dest_only.append((dt, kind, dst_t[dt][1]))

    extras.sort(reverse=True)
    print("=== Top dest topics by extra unique bytes vs mapped source ===")
    print(
        f"{'extra_GiB':>10} {'src_GiB':>10} {'dst_GiB':>10} {'src_rf':>6} {'dst_rf':>6} topic_pair"
    )
    extra_path = args.extra_tsv
    fh = open(extra_path, "w", encoding="utf-8") if extra_path else None
    if fh:
        fh.write("extra_bytes\tsrc_topic\tdst_topic\tsrc_unique\tdst_unique\tsrc_raw\tdst_raw\n")
    for i, row in enumerate(extras):
        extra, st, dt, suniq, duniq, sraw, draw = row
        srf = src_d.get(st, {}).get("rf", "-")
        drf = dst_d.get(dt, {}).get("rf", "-")
        if i < args.top:
            print(
                f"{gib(extra):>10} {gib(suniq):>10} {gib(duniq):>10} {str(srf):>6} {str(drf):>6} {st} -> {dt}"
            )
        if fh:
            fh.write(f"{extra}\t{st}\t{dt}\t{suniq}\t{duniq}\t{sraw}\t{draw}\n")
    if fh:
        fh.close()
        print(f"\nWrote extra-bytes TSV: {extra_path}")

    mm2_bytes = sum(sz for _, kind, sz in dest_only if kind and kind != "kafka-internal")
    kafka_int = sum(sz for _, kind, sz in dest_only if kind == "kafka-internal")
    other_only = [(n, k, sz) for n, k, sz in dest_only if not k]
    print()
    print("=== Dest-only topics (not mapped from source) ===")
    print(f"MM2 internals unique GiB: {gib(mm2_bytes)}")
    print(f"Kafka internals unique GiB (__consumer_offsets etc.): {gib(kafka_int)}")
    print(f"Other dest-only topics: {len(other_only)}")
    dest_only.sort(key=lambda x: -x[2])
    for name, kind, sz in dest_only[:40]:
        tag = kind or "unmapped"
        print(f"  {gib(sz):>8} GiB  [{tag}] {name}")
    if len(dest_only) > 40:
        print(f"  … {len(dest_only) - 40} more")

    print()
    print(f"=== Source topics with no dest mapping ({len(source_only)}) ===")
    for n in sorted(source_only)[:40]:
        print(f"  {n}")
    if len(source_only) > 40:
        print(f"  … {len(source_only) - 40} more")

    print()
    print("=== Topic config drift (retention / cleanup) ===")
    if not src_c and not dst_c:
        print("  (configs not collected or not parsed)")
    elif not cfg_diffs:
        print("  (no retention/cleanup differences on mapped pairs)")
    else:
        for st, dt, key, sv, dv in cfg_diffs[:50]:
            print(f"  {key}: {st}={sv}  {dt}={dv}")
        if len(cfg_diffs) > 50:
            print(f"  … {len(cfg_diffs) - 50} more")


if __name__ == "__main__":
    argv0 = os.path.basename(sys.argv[0]) if sys.argv else ""
    bash_args = {"-c", "--config", "-y", "--yes", "--via", "--only", "--skip"}
    if argv0.endswith(".sh") or (len(sys.argv) > 1 and sys.argv[1] in bash_args):
        sys.stderr.write(
            "This Python file is lib/join_storage.py, not the storage compare driver.\n"
            "On the MM host run the bash wrapper:\n"
            "  ./compare_storage.sh -c config/clusters/prod.env -c config/clusters/dr.env -y\n"
            "If compare_storage.sh starts with 'import' / 'from mm2_parse', it was overwritten;\n"
            "restore the bash script (head -1 must be #!/usr/bin/env bash).\n"
        )
        sys.exit(2)
    main()
