"""`opsbi` — CLI that dispatches to registered report plugins.

Each report contributes its own argparse subparser. `opsbi <report> --help`
shows the report's parameters; `opsbi <report> [args]` runs it and prints a
grouped text table (or flat, if the report doesn't declare groupings).

Also exposes a few top-level helpers that don't belong to any one report:
    opsbi list-regions
    opsbi list-domains
    opsbi list-cells
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional

import pymysql

from openstack_bi.config import parse_regions
from openstack_bi.reports import all_reports
from openstack_bi.reports.base import Param, Report, ReportResult


def _add_param(sp: argparse.ArgumentParser, param: Param) -> None:
    flag = "--" + param.name.replace("_", "-")
    kwargs: Dict[str, Any] = {"help": param.help or param.label}
    if param.kind == "int":
        kwargs["type"] = int
    elif param.kind == "bool":
        kwargs["action"] = "store_true"
        sp.add_argument(flag, **kwargs)
        return
    elif param.kind == "multiselect":
        kwargs["action"] = "append"
        kwargs["metavar"] = param.label.upper()
    if param.default is not None and param.kind != "bool":
        kwargs["default"] = param.default
    sp.add_argument(flag, **kwargs)


def _coerce_param(param: Param, raw: Any) -> Any:
    """Normalise argparse's multiselect=None into [] so report code can
    treat the two cases identically ('no filter' vs. 'no override')."""
    if param.kind == "multiselect" and raw is None:
        return []
    return raw


def _print_text(result: ReportResult, out=sys.stdout) -> None:
    if not result.rows:
        if "error" in result.metadata:
            print(result.metadata["error"], file=out)
            return
        _print_metadata(result, out)
        print("(no rows)", file=out)
        return

    _print_metadata(result, out)
    out.write("\n")

    visible_cols = [(k, label) for k, label in result.columns if not label.startswith("_")]
    # Columns whose key starts with an underscore are hidden from CLI output.

    if not result.groupings:
        _print_flat(visible_cols, result.rows, out)
        return

    groups: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
    for row in result.rows:
        key = tuple(row.get(g) for g in result.groupings)
        groups[key].append(row)

    # Stable group order: first time we saw each key.
    seen: List[tuple] = []
    for row in result.rows:
        key = tuple(row.get(g) for g in result.groupings)
        if key not in seen:
            seen.append(key)

    total = 0
    for key in seen:
        rows = groups[key]
        heading = " / ".join(str(v) if v is not None else "(none)" for v in key)
        print(f"  {heading}  ({len(rows)} row{'s' if len(rows) != 1 else ''})", file=out)
        _print_flat(visible_cols, rows, out, indent="    ")
        out.write("\n")
        total += len(rows)

    print(f"Total: {total} row(s).", file=out)


def _print_metadata(result: ReportResult, out) -> None:
    for key, value in result.metadata.items():
        print(f"{key}: {value}", file=out)


def _print_flat(cols, rows, out, indent: str = "") -> None:
    widths = {
        key: max(
            len(label),
            min(60, max((len(str(r.get(key) or "")) for r in rows), default=0)),
        )
        for key, label in cols
    }
    header = indent + "  ".join(label.ljust(widths[key]) for key, label in cols)
    sep = indent + "  ".join("-" * widths[key] for key, _ in cols)
    print(header, file=out)
    print(sep, file=out)
    for r in rows:
        print(
            indent + "  ".join(str(r.get(key) or "").ljust(widths[key]) for key, _ in cols),
            file=out,
        )


def _handle_top_level(name: str) -> int:
    from openstack_bi import openstack

    if name == "list-regions":
        for r in parse_regions():
            print(f"{r.name:<16}  {r.host}:{r.port}  user={r.user}")
        return 0
    if name == "list-domains":
        for d in openstack.list_domains():
            print(f"{d['id']}  {d['name']:<30}  ({d['project_count']} project(s))")
        return 0
    if name == "list-cells":
        for region in parse_regions():
            for cell in openstack.list_cells(region):
                print(f"{region.name}\t{cell}")
        return 0
    raise AssertionError(f"unknown top-level command: {name}")


def _dispatch_report(report: Report, ns: argparse.Namespace) -> int:
    kwargs: Dict[str, Any] = {}
    for param in report.params:
        kwargs[param.name] = _coerce_param(param, getattr(ns, param.name, None))
    result = report.run(**kwargs)
    _print_text(result)
    return 0


def build_parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="opsbi",
        description="OpenStack operations/BI reporting suite.",
    )
    sub = root.add_subparsers(dest="command", required=True, metavar="<command>")

    for name, desc in [
        ("list-regions", "List configured regions."),
        ("list-domains", "List enabled Keystone domains."),
        ("list-cells", "List discovered Nova cell DBs per region."),
    ]:
        sub.add_parser(name, help=desc, description=desc)

    for report in all_reports():
        sp = sub.add_parser(
            report.id.replace("_", "-"),
            help=report.name,
            description=f"{report.name}. {report.description}",
        )
        sp.set_defaults(_report=report)
        for param in report.params:
            _add_param(sp, param)

    return root


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command in ("list-regions", "list-domains", "list-cells"):
            return _handle_top_level(args.command)
        report: Report = args._report
        return _dispatch_report(report, args)
    except pymysql.MySQLError as exc:
        print(f"DB error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
