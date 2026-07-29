#!/usr/bin/env python3
"""Terminal formatting helpers shared by Python report scripts."""

from __future__ import annotations

import os
import sys
from typing import Any, Iterable, List, Optional, Sequence


def _use_color() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("KAFKA_ADMIN_COLOR", "").lower() in ("0", "false", "no"):
        return False
    return sys.stdout.isatty()


USE_COLOR = _use_color()

# ANSI
_RESET = "\033[0m"
_BOLD = "\033[1m"
_DIM = "\033[2m"
_RED = "\033[31m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_BLUE = "\033[34m"
_MAGENTA = "\033[35m"
_CYAN = "\033[36m"
_GRAY = "\033[90m"


def c(text: str, *codes: str) -> str:
    if not USE_COLOR or not codes:
        return text
    return "".join(codes) + text + _RESET


def bold(text: str) -> str:
    return c(text, _BOLD)


def dim(text: str) -> str:
    return c(text, _DIM)


def hr(width: int = 72, char: str = "─") -> str:
    return dim(char * width)


def section(title: str, width: int = 72) -> None:
    print()
    print(c("╭" + "─" * (width - 2) + "╮", _CYAN))
    inner = f" {title} "
    pad = width - 2 - len(inner)
    if pad < 0:
        inner = inner[: width - 2]
        pad = 0
    print(c("│", _CYAN) + bold(inner) + (" " * pad) + c("│", _CYAN))
    print(c("╰" + "─" * (width - 2) + "╯", _CYAN))


def subsection(title: str) -> None:
    print()
    print(c("▸ ", _BLUE) + bold(title))
    print(dim("  " + "·" * 56))


def kv(key: str, value: Any, key_width: int = 22) -> None:
    print(f"  {dim(key.ljust(key_width))} {value}")


def bullet(text: str) -> None:
    print(f"  {c('•', _CYAN)} {text}")


def note(text: str) -> None:
    print(f"  {c('ℹ', _BLUE)} {dim(text)}")


def badge(level: str) -> str:
    level = (level or "UNKNOWN").upper()
    styles = {
        "OK": (_GREEN, _BOLD),
        "WATCH": (_YELLOW, _BOLD),
        "TIGHT": (_YELLOW, _BOLD),
        "CRITICAL": (_RED, _BOLD),
        "WARN": (_YELLOW, _BOLD),
        "ERROR": (_RED, _BOLD),
        "UNKNOWN": (_GRAY, _BOLD),
    }
    codes = styles.get(level, (_GRAY, _BOLD))
    return c(f"[{level}]", *codes)


def status(level: str, message: str) -> None:
    print(f"  {badge(level)} {message}")


def progress_bar(ratio: float, width: int = 24) -> str:
    ratio = max(0.0, min(1.0, ratio))
    filled = int(round(ratio * width))
    bar = "█" * filled + "░" * (width - filled)
    pct = f"{ratio * 100:5.1f}%"
    if ratio < 0.50:
        color = _GREEN
    elif ratio < 0.75:
        color = _YELLOW
    else:
        color = _RED
    return f"{c(bar, color)} {pct}"


def fmt_num(x: Any, digits: int = 1) -> str:
    if x is None:
        return "—"
    try:
        v = float(x)
    except (TypeError, ValueError):
        return str(x)
    if abs(v) >= 1_000_000:
        return f"{v/1_000_000:.{digits}f}M"
    if abs(v) >= 1_000:
        return f"{v/1_000:.{digits}f}k"
    if abs(v - round(v)) < 1e-9:
        return str(int(round(v)))
    return f"{v:.{digits}f}"


def fmt_bytes(n: Optional[float]) -> str:
    if n is None:
        return "—"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    v = float(n)
    for u in units:
        if abs(v) < 1024 or u == units[-1]:
            if u == "B":
                return f"{int(v)} {u}"
            return f"{v:.1f} {u}"
        v /= 1024
    return f"{v:.1f} TiB"


def fmt_ms(x: Any) -> str:
    if x is None:
        return "—"
    try:
        v = float(x)
    except (TypeError, ValueError):
        return str(x)
    if v >= 1000:
        return f"{v/1000:.2f}s"
    if v >= 10:
        return f"{v:.1f}ms"
    if v >= 1:
        return f"{v:.2f}ms"
    return f"{v:.3f}ms"


def table(headers: Sequence[str], rows: Iterable[Sequence[Any]], aligns: Optional[Sequence[str]] = None) -> None:
    rows_list: List[List[str]] = [[str(c) for c in row] for row in rows]
    headers_list = [str(h) for h in headers]
    if not rows_list:
        print(dim("  (no rows)"))
        return
    widths = [len(h) for h in headers_list]
    for row in rows_list:
        for i, cell in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], len(cell))
    if aligns is None:
        aligns = ["l"] + ["r"] * (len(headers_list) - 1)

    def fmt_cell(i: int, text: str, header: bool = False) -> str:
        w = widths[i]
        align = aligns[i] if i < len(aligns) else "l"
        body = text.ljust(w) if align == "l" else text.rjust(w)
        return bold(body) if header else body

    print("  " + "  ".join(fmt_cell(i, h, True) for i, h in enumerate(headers_list)))
    print("  " + dim("  ".join("─" * w for w in widths)))
    for row in rows_list:
        # pad short rows
        padded = list(row) + [""] * (len(headers_list) - len(row))
        print("  " + "  ".join(fmt_cell(i, padded[i]) for i in range(len(headers_list))))


def box(title: str, lines: Sequence[str], width: int = 72) -> None:
    print()
    print(c("┌" + "─" * (width - 2) + "┐", _MAGENTA))
    t = f" {title} "
    print(c("│", _MAGENTA) + bold(t.ljust(width - 2)) + c("│", _MAGENTA))
    print(c("├" + "─" * (width - 2) + "┤", _MAGENTA))
    for line in lines:
        # visible length approx without ANSI — keep simple
        content = f" {line}"
        # truncate hard if needed
        raw = content
        if len(raw) > width - 2:
            raw = raw[: width - 5] + "..."
        print(c("│", _MAGENTA) + raw.ljust(width - 2) + c("│", _MAGENTA))
    print(c("└" + "─" * (width - 2) + "┘", _MAGENTA))
