#!/usr/bin/env python3
"""Command-line entry for the QEMU lifetime report.

Lists Keystone domains, lets you pick one (or pass --domain), then prints
every instance in that domain's projects along with its most recent
lifecycle action and how long ago it occurred.

By default only instances in vm_state=active are reported. Use --state
(repeatable) to pick other states, or --all-states to disable filtering.

Optionally filters with --days to surface instances with no qualifying
event in the last N days.

DB connection config lives in `core.py` (env vars).
"""

import argparse
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional

import pymysql

import core


def pick_domain_interactively(domains: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not domains:
        sys.exit("No enabled domains found in Keystone.")
    print("Domains:")
    for idx, d in enumerate(domains, 1):
        print(f"  {idx:>3}) {d['name']:<30}  ({d['project_count']} project(s))")
    while True:
        raw = input(f"\nSelect domain [1-{len(domains)}]: ").strip()
        try:
            i = int(raw)
            if 1 <= i <= len(domains):
                return domains[i - 1]
        except ValueError:
            pass
        print("Invalid selection.")


def prompt_days() -> Optional[int]:
    raw = input("Minimum days since last lifecycle event [Enter for no filter]: ").strip()
    if not raw:
        return None
    try:
        n = int(raw)
        if n < 0:
            raise ValueError
        return n
    except ValueError:
        sys.exit("Days must be a non-negative integer.")


def render_grouped(
    domain: Dict[str, Any],
    projects: List[Dict[str, Any]],
    rows: List[Dict[str, Any]],
    days_filter: Optional[int],
    state_filter: Optional[List[str]],
) -> str:
    cols = [
        ("uuid", "uuid", 36),
        ("name", "name", 24),
        ("compute_host", "host", 14),
        ("vm_state", "state", 9),
        ("last_action", "last_action", 14),
        ("last_action_time", "last_action_time", 19),
        ("age", "age", 12),
    ]

    by_project: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_project[r["project_id"]].append(r)

    out: List[str] = [f"Domain: {domain['name']}  (id: {domain['id']})"]
    if state_filter:
        out.append(f"State filter: {', '.join(state_filter)}")
    else:
        out.append("State filter: (all states)")
    if days_filter is not None:
        out.append(f"Days filter: no lifecycle event in the last {days_filter} day(s)")
    out.append("")

    total = 0
    for proj in projects:
        proj_rows = by_project.get(proj["id"], [])
        if days_filter is not None and not proj_rows:
            continue
        out.append(f"  Project: {proj['name']}  (id: {proj['id']})  — {len(proj_rows)} instance(s)")
        if not proj_rows:
            out.append("    (no instances)\n")
            continue
        widths = {key: max(len(label), min(maxw, max((len(str(r.get(key) or '')) for r in proj_rows), default=0)))
                  for key, label, maxw in cols}
        header = "    " + "  ".join(label.ljust(widths[key]) for key, label, _ in cols)
        sep = "    " + "  ".join("-" * widths[key] for key, _, _ in cols)
        out.append(header)
        out.append(sep)
        for r in proj_rows:
            line = "    " + "  ".join(str(r.get(key) or "").ljust(widths[key]) for key, _, _ in cols)
            out.append(line)
        out.append("")
        total += len(proj_rows)

    out.append(f"Total: {total} instance(s) reported.")
    return "\n".join(out)


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--domain", help="Domain name or id (skips interactive prompt)")
    p.add_argument("--days", type=int,
                   help="Only show instances with no lifecycle event in the last N days")
    p.add_argument("--state", action="append", metavar="STATE",
                   help=("Filter by vm_state. Repeatable; default is just 'active'. "
                         "Use --all-states to disable filtering."))
    p.add_argument("--all-states", action="store_true",
                   help="Show instances in any vm_state (overrides --state)")
    p.add_argument("--list-domains", action="store_true",
                   help="List domains and exit")
    p.add_argument("--list-cells", action="store_true",
                   help="List discovered cell DBs and exit")
    args = p.parse_args()

    try:
        if args.list_cells:
            for c in core.list_cell_dbs():
                print(c)
            return 0

        domains = core.list_domains()

        if args.list_domains:
            for d in domains:
                print(f"{d['id']}  {d['name']:<30}  ({d['project_count']} project(s))")
            return 0

        if args.domain:
            domain = core.find_domain(args.domain)
            if not domain:
                sys.exit(f"Domain not found: {args.domain}")
        else:
            domain = pick_domain_interactively(domains)

        days = args.days if args.days is not None else (
            prompt_days() if sys.stdin.isatty() and not args.domain else None
        )

        if args.all_states:
            vm_states: Optional[List[str]] = None
        elif args.state:
            vm_states = list(args.state)
        else:
            vm_states = list(core.DEFAULT_VM_STATES)

        projects = core.list_projects(domain["id"])
        if not projects:
            print(f"Domain {domain['name']} has no enabled projects.")
            return 0

        project_ids = [p["id"] for p in projects]
        rows: List[Dict[str, Any]] = []
        for cell in core.list_cell_dbs():
            rows.extend(core.fetch_instances(cell, project_ids, days, vm_states))
        core.annotate_ages(rows)
        print(render_grouped(domain, projects, rows, days, vm_states))
        return 0

    except pymysql.MySQLError as exc:
        print(f"DB error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
