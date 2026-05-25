"""Locate an instance by name or UUID across every datacenter.

A lookup tool, not a compliance report: an operator pastes part of an
instance name (with SQL wildcards) or a full/partial UUID, ticks the
datacenters to search, and gets back every matching Nova instance —
in any state, across all regions and cells — with project + domain
context and per-region rollups.

This is a copy of the SPLA-licensed-instances report stripped down to
the locating job. Gone are the licensing-specific filters (boot-volume
image match, MAAS exclusion, host/aggregate filters); in their place
are two plain search fields. What carries over verbatim is the bit that
makes it operationally useful: Keystone sessions get the same per-row
**Console** and **Live migrate** actions on the results table.

Filters surfaced in the form:
  • name LIKE (instance display_name OR hostname; bare words → %word%)
  • UUID (exact, or LIKE when a wildcard is present)
  • compute host LIKE (the n.host the instance runs on)
  • domain LIKE (Keystone domain name → its projects' instances)
  • project LIKE (Keystone project name → that project's instances)
  • regions (the datacenter checkboxes)

Domain / project are resolved against Keystone first (name → the set of
matching project ids) and then narrow the per-cell scan via
`n.project_id IN (...)`. When both are given they intersect.

At least one filter must be supplied — an unfiltered run would sweep
every instance in every cell, which is never what "locate" means.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote, urlencode

from openstack_bi import openstack
from openstack_bi.config import Region, parse_regions
from openstack_bi.db import query
from openstack_bi.util import format_region_errors, safe_for_each_region

from .base import Param, Report, ReportResult


def _region_choices() -> List[Tuple[str, str]]:
    return [(r.name, r.name) for r in parse_regions()]


def _like_term(raw: Optional[str]) -> Optional[str]:
    """Normalize a free-text search box into a SQL LIKE pattern.

    An empty box is no filter (None). A value that already contains a
    LIKE wildcard (`%` or `_`) is passed through untouched, so power
    users keep full control. A bare word is wrapped as `%word%` so the
    common case — "find anything containing this" — just works.
    """
    raw = (raw or "").strip()
    if not raw:
        return None
    if "%" in raw or "_" in raw:
        return raw
    return f"%{raw}%"


def _build_clauses(
    name_like: Optional[str],
    uuid_term: Optional[str],
    host_like: Optional[str] = None,
    project_ids: Optional[Sequence[str]] = None,
) -> Tuple[str, List[Any]]:
    """Compose the per-cell SQL + params for one cell scan.

    Everything rides through pymysql parameter binding; the UUID field
    matches exactly unless it carries a wildcard, in which case it falls
    back to LIKE. `host_like` is a LIKE pattern on the compute host;
    `project_ids`, when supplied, is the pre-resolved set of Keystone
    project ids from the domain / project filters.
    """
    args: List[Any] = []
    sql_parts: List[str] = [
        """
        SELECT
            n.created_at, n.id, n.hostname, n.display_name,
            n.vcpus, n.memory_mb, n.uuid, n.project_id,
            n.vm_state, n.host
        FROM instances n
        WHERE n.deleted = 0
        """
    ]
    if name_like:
        # Match either the user-facing name or the guest hostname — both
        # are columns in the table and operators think of them together.
        sql_parts.append("AND (n.display_name LIKE %s OR n.hostname LIKE %s)")
        args.append(name_like)
        args.append(name_like)
    if uuid_term:
        if "%" in uuid_term or "_" in uuid_term:
            sql_parts.append("AND n.uuid LIKE %s")
        else:
            sql_parts.append("AND n.uuid = %s")
        args.append(uuid_term)
    if host_like:
        sql_parts.append("AND n.host LIKE %s")
        args.append(host_like)
    if project_ids:
        ph = ",".join(["%s"] * len(project_ids))
        sql_parts.append(f"AND n.project_id IN ({ph})")
        args.extend(project_ids)
    sql_parts.append("ORDER BY n.created_at DESC")
    return "\n".join(sql_parts), args


def _resolve_project_directory(
    project_ids: Sequence[str],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Return `(project_name_by_id, domain_name_by_id)` for the given ids.

    Two passes: project rows first (to learn each project's domain_id),
    then a single follow-up to resolve those domains to names. Identical
    to the SPLA report's resolver — keystone is the shared directory.
    """
    from openstack_bi.config import keystone_db, keystone_region

    if not project_ids:
        return {}, {}
    pid_list = list({pid for pid in project_ids if pid})
    if not pid_list:
        return {}, {}
    proj_ph = ",".join(["%s"] * len(pid_list))
    proj_rows = query(
        keystone_region(), keystone_db(),
        f"SELECT id, name, domain_id FROM project WHERE id IN ({proj_ph})",
        pid_list,
    )
    project_name_by_id = {r["id"]: r["name"] for r in proj_rows}
    project_to_domain_id = {r["id"]: r["domain_id"] for r in proj_rows}

    domain_ids = list({d for d in project_to_domain_id.values() if d})
    domain_name_by_pid: Dict[str, str] = {}
    if domain_ids:
        dom_ph = ",".join(["%s"] * len(domain_ids))
        dom_rows = query(
            keystone_region(), keystone_db(),
            f"SELECT id, name FROM project WHERE id IN ({dom_ph}) AND is_domain = 1",
            domain_ids,
        )
        domain_name_by_id = {r["id"]: r["name"] for r in dom_rows}
        for pid, did in project_to_domain_id.items():
            if did and did in domain_name_by_id:
                domain_name_by_pid[pid] = domain_name_by_id[did]
    return project_name_by_id, domain_name_by_pid


def _resolve_domain_projects(domain_like: str) -> List[str]:
    """Project ids belonging to any domain whose name matches `domain_like`.

    Two keystone passes: domains by name (`is_domain = 1`), then the
    projects that live under those domains. Returns [] when nothing
    matches — the caller reads that as "this filter locates nothing".
    """
    from openstack_bi.config import keystone_db, keystone_region

    dom_rows = query(
        keystone_region(), keystone_db(),
        "SELECT id FROM project WHERE is_domain = 1 AND name LIKE %s",
        [domain_like],
    )
    domain_ids = [r["id"] for r in dom_rows]
    if not domain_ids:
        return []
    ph = ",".join(["%s"] * len(domain_ids))
    proj_rows = query(
        keystone_region(), keystone_db(),
        f"SELECT id FROM project WHERE is_domain = 0 AND domain_id IN ({ph})",
        domain_ids,
    )
    return [r["id"] for r in proj_rows]


def _resolve_named_projects(project_like: str) -> List[str]:
    """Project ids whose own name matches `project_like` (`is_domain = 0`)."""
    from openstack_bi.config import keystone_db, keystone_region

    rows = query(
        keystone_region(), keystone_db(),
        "SELECT id FROM project WHERE is_domain = 0 AND name LIKE %s",
        [project_like],
    )
    return [r["id"] for r in rows]


class LocateInstanceReport(Report):
    id = "locate_instance"
    name = "Locate instance"
    description = (
        "Find an instance by name (with wildcards) or UUID across the "
        "selected datacenters and cells, in any state. Resolves project + "
        "domain context and rolls up vCPU / memory per region. Keystone "
        "sessions get per-row live-migrate / console actions; click an "
        "instance UUID to drill into its action history."
    )
    category = "Lifecycle"
    scope_to_projects = False  # cross-tenant lookup + powerful actions; admin-only
    params = [
        Param(
            name="name", label="Name / hostname LIKE", kind="string",
            placeholder="e.g. web-01  or  %prod%db%",
            help="Matches the instance name or its guest hostname. A bare "
                 "word matches anywhere in either (SQL wildcards added on "
                 "both sides); include SQL wildcards yourself for full LIKE "
                 "control.",
        ),
        Param(
            name="uuid", label="Instance UUID", kind="string",
            placeholder="full UUID (or partial with %)",
            help="Exact instance UUID. Add a SQL wildcard to match a "
                 "partial UUID instead.",
        ),
        Param(
            name="host", label="Compute host LIKE", kind="string",
            placeholder="e.g. cmp-07  or  %dtw%",
            help="Compute host the instance runs on. A bare word matches "
                 "anywhere in the host name (SQL wildcards added on both "
                 "sides); include SQL wildcards yourself for full LIKE "
                 "control.",
        ),
        Param(
            name="domain", label="Domain LIKE", kind="string",
            placeholder="e.g. acme  or  %corp%",
            help="Keystone domain name. Matches every instance in the "
                 "domain's projects. A bare word matches anywhere in the "
                 "name (SQL wildcards added on both sides).",
        ),
        Param(
            name="project", label="Project LIKE", kind="string",
            placeholder="e.g. web-prod  or  %staging%",
            help="Keystone project name. Matches every instance in that "
                 "project. A bare word matches anywhere in the name (SQL "
                 "wildcards added on both sides).",
        ),
        Param(
            name="regions", label="Datacenters", kind="multiselect",
            choices=_region_choices,
            help="Which datacenters to search. Empty = all configured.",
        ),
    ]

    def run(
        self,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
        host: Optional[str] = None,
        domain: Optional[str] = None,
        project: Optional[str] = None,
        regions: Optional[List[str]] = None,
        **_: Any,
    ) -> ReportResult:
        name_like = _like_term(name)
        uuid_term = (uuid or "").strip() or None
        host_like = _like_term(host)
        domain_like = _like_term(domain)
        project_like = _like_term(project)

        columns = [
            ("created_at", "Created (UTC)"),
            ("region", "Region"),
            ("domain_name", "Domain"),
            ("project_name", "Project"),
            ("display_name", "Instance"),
            ("hostname", "Hostname"),
            ("host", "Compute host"),
            ("vm_state", "State"),
            ("vcpus", "vCPU"),
            ("memory_mb", "Memory (MB)"),
            ("uuid", "Instance UUID"),
        ]

        # Refuse to run with no filter at all: an unfiltered scan would
        # return every instance in every cell, which defeats the purpose
        # (and is slow). Nudge instead of erroring so the form stays put.
        if not (name_like or uuid_term or host_like or domain_like or project_like):
            return ReportResult(
                columns=columns, rows=[],
                metadata={
                    "search": "Enter an instance name, UUID, compute host, "
                              "domain, or project to search.",
                },
                filename_stem="locate-instance",
            )

        selected_region_names = regions or None
        if selected_region_names is None:
            selected_regions = parse_regions()
        else:
            by_name = {r.name: r for r in parse_regions()}
            selected_regions = [
                by_name[n] for n in selected_region_names if n in by_name
            ]

        # Resolve the domain / project name filters against Keystone into a
        # concrete set of project ids. `project_ids is None` means "no
        # domain/project narrowing"; an empty list means the filter matched
        # nothing, so the instance scan can be skipped entirely.
        project_ids: Optional[List[str]] = None
        if domain_like or project_like:
            id_sets: List[set] = []
            if domain_like:
                id_sets.append(set(_resolve_domain_projects(domain_like)))
            if project_like:
                id_sets.append(set(_resolve_named_projects(project_like)))
            project_ids = sorted(set.intersection(*id_sets)) if id_sets else []

        region_errors: List[Dict[str, str]] = []
        if project_ids is not None and not project_ids:
            # Domain/project filter matched no projects → nothing to locate.
            instance_rows: List[Dict[str, Any]] = []
        else:
            sql, args = _build_clauses(name_like, uuid_term, host_like, project_ids)

            def _collect(region: Region) -> List[Dict[str, Any]]:
                cell_rows: List[Dict[str, Any]] = []
                for cell in openstack.list_cells(region):
                    rows = query(region, cell, sql, args)
                    for r in rows:
                        r["region"] = region.name
                        r["cell_db"] = cell
                    cell_rows.extend(rows)
                return cell_rows

            results, region_errors = safe_for_each_region(selected_regions, _collect)
            instance_rows = [
                r for _region, region_rows in results for r in region_rows
            ]

        # Resolve project + domain names from keystone in one pass.
        project_name_by_id, domain_name_by_pid = _resolve_project_directory(
            [r.get("project_id") for r in instance_rows]
        )

        # Build output rows + drill-down link + per-row action targets.
        rows_out: List[Dict[str, Any]] = []
        for r in instance_rows:
            pid = r.get("project_id")
            inst_uuid = r.get("uuid") or ""
            row: Dict[str, Any] = {
                "created_at": r.get("created_at"),
                "region": r.get("region"),
                "domain_name": domain_name_by_pid.get(pid, ""),
                "project_name": project_name_by_id.get(pid, ""),
                "project_id": pid,
                "display_name": r.get("display_name"),
                "hostname": r.get("hostname"),
                "host": r.get("host"),
                "vm_state": r.get("vm_state"),
                "vcpus": int(r.get("vcpus") or 0),
                "memory_mb": int(r.get("memory_mb") or 0),
                "uuid": inst_uuid,
            }
            if inst_uuid:
                qs = urlencode({"instance_uuid": inst_uuid})
                row["uuid_link"] = f"/report/instance_history?{qs}"
                # Web-only per-row action targets (live migrate / console).
                # Not declared columns, so the CLI and Excel export ignore
                # them; report.html renders an Actions column when present.
                region_name = r.get("region") or ""
                if region_name:
                    base = (
                        f"/instance/{quote(region_name, safe='')}"
                        f"/{quote(inst_uuid, safe='')}"
                    )
                    row["_migrate_url"] = f"{base}/migrate"
                    row["_console_url"] = f"{base}/console"
            rows_out.append(row)

        # Per-region totals: count, sum vCPU, sum memory.
        region_totals: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {"count": 0, "vcpus": 0, "memory_mb": 0}
        )
        for r in rows_out:
            region_name = r.get("region") or "(unknown)"
            t = region_totals[region_name]
            t["count"] += 1
            t["vcpus"] += int(r.get("vcpus") or 0)
            t["memory_mb"] += int(r.get("memory_mb") or 0)

        metadata: Dict[str, Any] = {
            "name_search": name_like or "(none)",
            "uuid_search": uuid_term or "(none)",
            "host_search": host_like or "(none)",
            "domain_search": domain_like or "(none)",
            "project_search": project_like or "(none)",
            "regions": ", ".join(r.name for r in selected_regions) or "(none)",
            "total_instances": len(rows_out),
            "total_vcpus": sum(r["vcpus"] for r in rows_out),
            "total_memory_gb": sum(r["memory_mb"] for r in rows_out) // 1024,
            "region_errors": format_region_errors(region_errors),
        }
        # When a domain/project filter is in play, show how many Keystone
        # projects it resolved to — a quick "did my filter match anything"
        # signal that's independent of how many instances came back.
        if project_ids is not None:
            metadata["matched_projects"] = len(project_ids)
        for region_name, totals in sorted(region_totals.items()):
            metadata[f"region_{region_name}"] = (
                f"{totals['count']} VMs · "
                f"{totals['vcpus']} vCPU · "
                f"{totals['memory_mb'] // 1024} GB"
            )

        stem_bits = ["locate-instance"]
        search_label = name_like or uuid_term or host_like or domain_like or project_like or "all"
        clean = re.sub(r"[^A-Za-z0-9]+", "-", search_label).strip("-").lower() or "all"
        stem_bits.append(clean)
        if selected_region_names is not None:
            stem_bits.append("-".join(r.name for r in selected_regions))
        else:
            stem_bits.append("all-regions")

        return ReportResult(
            columns=columns,
            rows=rows_out,
            metadata=metadata,
            filename_stem="-".join(stem_bits),
        )


REPORT = LocateInstanceReport()
