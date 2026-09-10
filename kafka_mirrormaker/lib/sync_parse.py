#!/usr/bin/env python3
"""Parse kafka-configs --describe --all and kafka-acls --list. No secrets printed."""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from typing import Any


SKIP_CONFIG_KEYS = frozenset(
    {
        "follower.replication.throttled.replicas",
        "leader.replication.throttled.replicas",
        "remote.storage.enable",
        "remote.log.copy.disable",
        "remote.log.delete.on.disable",
    }
)

HEADER_TOPIC = re.compile(
    r"(?:All configs for topic|Dynamic configs for topic|Configs for topic)\s+(\S+)",
    re.I,
)
KV_LINE = re.compile(r"^\s*([A-Za-z0-9._-]+)=(\S+)")
RES_PATTERN = re.compile(
    r"ResourcePattern\(\s*resourceType=([A-Za-z_]+),\s*name=([^,]*?),\s*patternType=([A-Za-z]+)\s*\)",
    re.I,
)
RES_OLD = re.compile(r"`([A-Za-z_]+):([A-Za-z]+):([^`]+)`")
ACL_TUPLE = re.compile(
    r"principal=([^,]+),\s*host=([^,]+),\s*operation=([^,]+),\s*permissionType=([^)\s]+)",
    re.I,
)
ACL_PROSE = re.compile(
    r"((?:User|Group):\S+)\s+has\s+(Allow|Deny)\s+permission\s+for\s+operations?:\s*(.+?)\s+from\s+hosts?:\s*(\S+)",
    re.I,
)


def parse_topic_configs_all(text: str) -> dict[str, dict[str, str]]:
    current = None
    out: dict[str, dict[str, str]] = defaultdict(dict)
    for line in text.splitlines():
        hm = HEADER_TOPIC.search(line)
        if hm:
            current = hm.group(1).rstrip(":").rstrip(",")
        if current is None:
            continue
        if re.search(r"sensitive\s*=\s*true", line, re.I) and not re.search(
            r"sensitive\s*=\s*false", line, re.I
        ):
            continue
        km = KV_LINE.search(line)
        if not km:
            continue
        key, val = km.group(1), km.group(2).rstrip(",")
        if key.lower() == "sensitive":
            continue
        out[current][key] = val
    return dict(out)


def is_internal_name(name: str) -> bool:
    return name.startswith("_")


def config_diffs(
    src: dict[str, dict[str, str]],
    dst: dict[str, dict[str, str]],
    *,
    include_internal: bool = False,
    sync_skipped: bool = False,
    pattern: str | None = None,
    exclude: str | None = None,
) -> list[dict[str, str]]:
    pat = re.compile(pattern) if pattern else None
    ex = re.compile(exclude) if exclude else None
    rows: list[dict[str, str]] = []
    for topic in sorted(src):
        if not include_internal and is_internal_name(topic):
            continue
        if pat and not pat.search(topic):
            continue
        if ex and ex.search(topic):
            continue
        if topic not in dst:
            rows.append(
                {
                    "topic": topic,
                    "key": "*",
                    "src": "(present)",
                    "dst": "MISSING_TOPIC",
                    "action": "skip",
                }
            )
            continue
        keys = set(src[topic]) | set(dst[topic])
        for key in sorted(keys):
            if not sync_skipped and (
                key in SKIP_CONFIG_KEYS or key.startswith("remote.log.")
            ):
                continue
            sv = src[topic].get(key, "")
            dv = dst[topic].get(key, "")
            if sv == dv:
                continue
            if not sv:
                continue
            rows.append(
                {
                    "topic": topic,
                    "key": key,
                    "src": sv,
                    "dst": dv or "(unset)",
                    "action": "set",
                }
            )
    return rows


def alters_by_topic(diffs: list[dict[str, str]]) -> dict[str, list[tuple[str, str]]]:
    by: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for r in diffs:
        if r.get("action") != "set":
            continue
        by[r["topic"]].append((r["key"], r["src"]))
    return dict(by)


def format_add_config(pairs: list[tuple[str, str]]) -> list[str]:
    """Split on commas in values so kafka-configs --add-config stays valid."""
    chunks: list[str] = []
    current: list[str] = []
    for key, val in pairs:
        item = f"{key}={val}"
        if "," in val:
            if current:
                chunks.append(",".join(current))
                current = []
            chunks.append(item)
        else:
            current.append(item)
    if current:
        chunks.append(",".join(current))
    return chunks


def parse_acls(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    rtype = name = pattern = ""
    for line in text.splitlines():
        rm = RES_PATTERN.search(line)
        if rm:
            rtype, name, pattern = rm.group(1).upper(), rm.group(2).strip(), rm.group(3).upper()
            continue
        om = RES_OLD.search(line)
        if om and "ResourcePattern" not in line:
            rtype, pattern, name = om.group(1).upper(), om.group(2).upper(), om.group(3).strip()
            continue
        if not rtype:
            continue
        for tm in ACL_TUPLE.finditer(line):
            rows.append(
                {
                    "rtype": rtype,
                    "name": name,
                    "pattern": pattern,
                    "principal": tm.group(1).strip(),
                    "host": tm.group(2).strip(),
                    "operation": tm.group(3).strip().upper(),
                    "perm": tm.group(4).strip().upper(),
                }
            )
        pm = ACL_PROSE.search(line)
        if pm:
            ops = [o.strip().upper() for o in re.split(r"[,/]| and ", pm.group(3)) if o.strip()]
            for op in ops:
                rows.append(
                    {
                        "rtype": rtype,
                        "name": name,
                        "pattern": pattern,
                        "principal": pm.group(1).strip(),
                        "host": pm.group(4).strip().rstrip("."),
                        "operation": op,
                        "perm": pm.group(2).upper(),
                    }
                )
    # unique
    seen: set[tuple[str, ...]] = set()
    uniq: list[dict[str, str]] = []
    for r in rows:
        key = tuple(r[k] for k in ("rtype", "name", "pattern", "principal", "host", "operation", "perm"))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)
    return uniq


def _acl_key(r: dict[str, str]) -> tuple[str, ...]:
    return tuple(r[k] for k in ("rtype", "name", "pattern", "principal", "host", "operation", "perm"))


def acl_diffs(
    src: list[dict[str, str]],
    dst: list[dict[str, str]],
    *,
    include_internal: bool = False,
) -> dict[str, list[dict[str, str]]]:
    if not include_internal:
        src = [r for r in src if not is_internal_name(r["name"])]
        dst = [r for r in dst if not is_internal_name(r["name"])]
    sk = {_acl_key(r) for r in src}
    dk = {_acl_key(r) for r in dst}
    sm = {_acl_key(r): r for r in src}
    dm = {_acl_key(r): r for r in dst}
    add = [sm[k] for k in sorted(sk - dk)]
    extra = [dm[k] for k in sorted(dk - sk)]
    return {"add": add, "extra": extra}


# kafka-acls.sh resource flags. USER is ResourceType.USER (--user-principal).
ACL_RESOURCE_FLAGS: dict[str, tuple[str, bool]] = {
    "TOPIC": ("--topic", True),
    "GROUP": ("--group", True),
    "CLUSTER": ("--cluster", False),
    "TRANSACTIONAL_ID": ("--transactional-id", True),
    "DELEGATION_TOKEN": ("--delegation-token", True),
    "USER": ("--user-principal", True),
}


def acl_cli_args(entry: dict[str, str], *, remove: bool = False) -> list[str]:
    args: list[str] = ["--remove"] if remove else ["--add"]
    perm = entry["perm"].upper()
    if perm == "DENY":
        args += ["--deny-principal", entry["principal"], "--deny-host", entry["host"]]
    else:
        args += ["--allow-principal", entry["principal"], "--allow-host", entry["host"]]
    args += ["--operation", entry["operation"]]
    ptype = entry["pattern"].upper()
    if ptype == "PREFIXED":
        args += ["--resource-pattern-type", "prefixed"]
    elif ptype == "MATCH":
        args += ["--resource-pattern-type", "match"]
    else:
        args += ["--resource-pattern-type", "literal"]
    rtype = entry["rtype"].upper()
    spec = ACL_RESOURCE_FLAGS.get(rtype)
    if spec is None:
        raise ValueError(f"unsupported ACL resource type {rtype}")
    flag, needs_name = spec
    if needs_name:
        args += [flag, entry["name"]]
    else:
        args.append(flag)
    return args


def acl_argv_lists(
    rows: list[dict[str, str]], *, remove: bool = False
) -> tuple[list[list[str]], list[dict[str, str]]]:
    argv: list[list[str]] = []
    skipped: list[dict[str, str]] = []
    for row in rows:
        try:
            argv.append(acl_cli_args(row, remove=remove))
        except ValueError as exc:
            skipped.append({**row, "reason": str(exc)})
    return argv, skipped


def _print_json(obj: Any) -> None:
    json.dump(obj, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")


def main() -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("topic-diff")
    d.add_argument("--src-configs", required=True)
    d.add_argument("--dst-configs", required=True)
    d.add_argument("--include-internal", action="store_true")
    d.add_argument("--sync-skipped", action="store_true")
    d.add_argument("--pattern", default="")
    d.add_argument("--exclude", default="")
    a = sub.add_parser("acl-diff")
    a.add_argument("--src-acls", required=True)
    a.add_argument("--dst-acls", required=True)
    a.add_argument("--include-internal", action="store_true")
    argv = p.parse_args()
    if argv.cmd == "topic-diff":
        src = parse_topic_configs_all(open(argv.src_configs, encoding="utf-8", errors="replace").read())
        dst = parse_topic_configs_all(open(argv.dst_configs, encoding="utf-8", errors="replace").read())
        diffs = config_diffs(
            src,
            dst,
            include_internal=argv.include_internal,
            sync_skipped=argv.sync_skipped,
            pattern=argv.pattern or None,
            exclude=argv.exclude or None,
        )
        _print_json({"diffs": diffs, "alters": alters_by_topic(diffs)})
    elif argv.cmd == "acl-diff":
        src = parse_acls(open(argv.src_acls, encoding="utf-8", errors="replace").read())
        dst = parse_acls(open(argv.dst_acls, encoding="utf-8", errors="replace").read())
        dff = acl_diffs(src, dst, include_internal=argv.include_internal)
        add_argv, skip_add = acl_argv_lists(dff["add"])
        remove_argv, skip_rm = acl_argv_lists(dff["extra"], remove=True)
        out = {
            "add": dff["add"],
            "extra": dff["extra"],
            "add_argv": add_argv,
            "remove_argv": remove_argv,
            "skipped": skip_add + skip_rm,
        }
        _print_json(out)


if __name__ == "__main__":
    main()
