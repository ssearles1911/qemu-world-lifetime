"""Aggregated issue dashboard.

Runs a battery of checks against Nova, Cinder, and Neutron and emits one
row per finding with a severity classification, the affected project and
region, and a short remediation hint. Grouped by severity in the UI.

Current checks:
    error_instances        — Nova instances in vm_state='error'
    stuck_task_state       — Nova instances with task_state set > N hours
    stuck_volumes          — Cinder volumes in transient status > N hours
    orphaned_volumes       — Cinder volumes available + unattached + old
    old_unbound_fips       — Neutron FIPs unbound > N days
    stale_snapshots_flag   — Cinder snapshots older than N days (high-level
                             count; use `stale_snapshots` for the detail)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from openstack_bi import openstack
from openstack_bi.config import (
    cinder_db,
    keystone_db,
    keystone_region,
    neutron_db,
    parse_regions,
)
from openstack_bi.db import query
from openstack_bi.util import format_region_errors, humanize, safe_for_each_region

from .base import ChartSpec, Param, Report, ReportResult


SEVERITY_ORDER = {"critical": 0, "error": 1, "warn": 2, "info": 3}
_TRANSIENT_VOL_STATUSES = ("creating", "attaching", "detaching", "deleting", "error")


def _domain_choices() -> List[Tuple[str, str]]:
    return [("", "— all domains —")] + [
        (d["name"], f'{d["name"]} ({d["project_count"]} project(s))')
        for d in openstack.list_domains()
    ]


def _region_choices() -> List[Tuple[str, str]]:
    return [(r.name, r.name) for r in parse_regions()]


def _severity_choices() -> List[Tuple[str, str]]:
    return [("", "— all —"), ("critical", "critical"), ("error", "error"),
            ("warn", "warn"), ("info", "info")]


class IssuesReport(Report):
    id = "issues"
    name = "Issues dashboard"
    description = (
        "Cross-service health check. Surfaces instances in error, stuck "
        "task_states, volumes stuck in transient statuses, orphaned volumes, "
        "long-unbound floating IPs, and stale snapshots. Each row is one "
        "finding with severity, project, region, and a hint."
    )
    params = [
        Param(name="domain", label="Domain", kind="select",
              choices=_domain_choices, default="",
              help="Scope all checks to this Keystone domain. Empty = all domains."),
        Param(name="regions", label="Regions", kind="multiselect",
              choices=_region_choices,
              help="Which regions to span. Empty = all configured regions."),
        Param(name="min_severity", label="Minimum severity", kind="select",
              default="info", choices=_severity_choices,
              help="Hide findings below this severity."),
        Param(name="stuck_hours", label="Stuck (hours)", kind="int", default=6,
              placeholder="6",
              help="task_state / volume transient threshold in hours."),
        Param(name="orphan_days", label="Orphan (days)", kind="int", default=60,
              placeholder="60",
              help="Available+unattached volume age threshold."),
        Param(name="fip_days", label="Unbound FIP (days)", kind="int", default=30,
              placeholder="30",
              help="Unbound floating IP age threshold."),
        Param(name="snapshot_days", label="Snapshot (days)", kind="int", default=180,
              placeholder="180",
              help="Snapshot age threshold flagged as stale."),
    ]

    def run(
        self,
        domain: Optional[str] = None,
        regions: Optional[List[str]] = None,
        min_severity: Optional[str] = "info",
        stuck_hours: Optional[int] = 6,
        orphan_days: Optional[int] = 60,
        fip_days: Optional[int] = 30,
        snapshot_days: Optional[int] = 180,
        **_: Any,
    ) -> ReportResult:
        selected_region_names = regions or None
        all_regions = parse_regions()
        if selected_region_names is None:
            selected_regions = all_regions
        else:
            by_name = {r.name: r for r in all_regions}
            selected_regions = [by_name[n] for n in selected_region_names if n in by_name]

        project_filter: Optional[List[str]] = None
        name_by_id: Dict[str, str] = {}
        domain_obj: Optional[Dict[str, Any]] = None
        if domain:
            domain_obj = openstack.find_domain(domain)
            if domain_obj is None:
                return ReportResult(
                    columns=[], rows=[],
                    metadata={"error": f"Domain not found: {domain!r}"},
                    filename_stem=f"issues-{domain}",
                )
            projects = openstack.list_projects(domain_obj["id"])
            project_filter = [p["id"] for p in projects]
            name_by_id = {p["id"]: p["name"] for p in projects}

        stuck_h = max(1, int(stuck_hours or 6))
        orphan_d = max(1, int(orphan_days or 60))
        fip_d = max(0, int(fip_days or 30))
        snap_d = max(1, int(snapshot_days or 180))

        def _run_checks(region):
            acc: List[Dict[str, Any]] = []
            acc.extend(_check_error_instances(region, project_filter))
            acc.extend(_check_stuck_task_state(region, project_filter, stuck_h))
            acc.extend(_check_stuck_volumes(region, project_filter, stuck_h))
            acc.extend(_check_orphaned_volumes(region, project_filter, orphan_d))
            acc.extend(_check_old_unbound_fips(region, project_filter, fip_d))
            acc.extend(_check_stale_snapshots(region, project_filter, snap_d))
            return acc

        per_region_results, region_errors = safe_for_each_region(selected_regions, _run_checks)
        findings: List[Dict[str, Any]] = [f for _, fs in per_region_results for f in fs]

        # Resolve project names for any rows we couldn't pre-populate.
        unknown_pids = {f["project_id"] for f in findings if f["project_id"] and f.get("project_name") is None}
        if unknown_pids:
            pid_list = list(unknown_pids)
            ph = ",".join(["%s"] * len(pid_list))
            rows = query(
                keystone_region(), keystone_db(),
                f"SELECT id, name FROM project WHERE id IN ({ph})",
                pid_list,
            )
            name_by_id.update({r["id"]: r["name"] for r in rows})
        for f in findings:
            if f["project_id"] and f.get("project_name") is None:
                f["project_name"] = name_by_id.get(f["project_id"], "(unknown)")
            elif f["project_name"] is None:
                f["project_name"] = ""

        # Apply minimum-severity filter.
        if min_severity:
            threshold = SEVERITY_ORDER.get(min_severity, 99)
            findings = [f for f in findings if SEVERITY_ORDER.get(f["severity"], 99) <= threshold]

        findings.sort(key=lambda f: (
            SEVERITY_ORDER.get(f["severity"], 99),
            f["issue_type"],
            f["region"],
            f.get("project_name") or "",
        ))

        # Per-region stacked chart by severity.
        regions_present = [r.name for r in selected_regions]
        counts_by_region: Dict[str, Dict[str, int]] = {rn: {"critical": 0, "error": 0, "warn": 0, "info": 0} for rn in regions_present}
        for f in findings:
            counts_by_region.setdefault(f["region"], {"critical": 0, "error": 0, "warn": 0, "info": 0})[f["severity"]] += 1
        chart = ChartSpec(
            kind="stacked_bar",
            title="Issues per region by severity",
            x_label="Region",
            y_label="Findings",
            x_categories=list(counts_by_region.keys()),
            series=[
                {"label": sev, "data": [counts_by_region[rn][sev] for rn in counts_by_region]}
                for sev in ("critical", "error", "warn", "info")
            ],
        )

        metadata = {
            "domain": domain_obj["name"] if domain_obj else "(all domains)",
            "regions": ", ".join(r.name for r in selected_regions) or "(none)",
            "min_severity": min_severity or "info",
            "thresholds": (
                f"stuck≥{stuck_h}h, orphan≥{orphan_d}d, "
                f"fip≥{fip_d}d, snapshot≥{snap_d}d"
            ),
            "total_findings": len(findings),
            "by_severity": ", ".join(
                f"{sev}={sum(1 for f in findings if f['severity'] == sev)}"
                for sev in ("critical", "error", "warn", "info")
            ),
            "region_errors": format_region_errors(region_errors),
        }

        stem_bits = ["issues"]
        if domain_obj:
            stem_bits.append(domain_obj["name"])
        stem_bits.append(
            "-".join(r.name for r in selected_regions)
            if selected_region_names is not None else "all-regions"
        )

        return ReportResult(
            columns=[
                ("severity", "Severity"),
                ("issue_type", "Type"),
                ("region", "Region"),
                ("project_name", "Project"),
                ("resource_id", "Resource"),
                ("resource_name", "Name"),
                ("age", "Age"),
                ("details", "Details"),
                ("hint", "Remediation hint"),
            ],
            rows=findings,
            groupings=["severity"],
            charts=[chart] if regions_present else [],
            metadata=metadata,
            filename_stem="-".join(stem_bits),
        )


# --- Checks ------------------------------------------------------------------


def _age(now: datetime, ts) -> Tuple[Optional[float], str]:
    if ts is None:
        return None, "-"
    seconds = (now - ts).total_seconds()
    return seconds, humanize(seconds)


def _check_error_instances(region, project_filter: Optional[List[str]]) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    where = "deleted = 0 AND vm_state = 'error'"
    args: List[Any] = []
    if project_filter is not None:
        ph = ",".join(["%s"] * len(project_filter))
        where += f" AND project_id IN ({ph})"
        args.extend(project_filter)
    for cell in openstack.list_cells(region):
        try:
            rows = query(
                region, cell,
                f"SELECT uuid, display_name, project_id, task_state, updated_at, created_at "
                f"FROM instances WHERE {where}",
                args,
            )
        except Exception:  # noqa: BLE001
            continue
        for r in rows:
            seconds, age = _age(now, r.get("updated_at") or r.get("created_at"))
            findings.append({
                "severity": "error",
                "issue_type": "error_instance",
                "region": region.name,
                "project_id": r["project_id"],
                "project_name": None,
                "resource_id": r["uuid"],
                "resource_name": r.get("display_name") or "",
                "age": age,
                "age_seconds": seconds,
                "details": f"task_state={r.get('task_state') or 'None'}",
                "hint": "Investigate why the VM failed; consider nova reset-state or rebuild.",
            })
    return findings


def _check_stuck_task_state(region, project_filter, hours: int) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    args: List[Any] = [hours]
    where = (
        "deleted = 0 AND task_state IS NOT NULL "
        "AND updated_at < (UTC_TIMESTAMP() - INTERVAL %s HOUR)"
    )
    if project_filter is not None:
        ph = ",".join(["%s"] * len(project_filter))
        where += f" AND project_id IN ({ph})"
        args.extend(project_filter)
    for cell in openstack.list_cells(region):
        try:
            rows = query(
                region, cell,
                f"SELECT uuid, display_name, project_id, vm_state, task_state, updated_at "
                f"FROM instances WHERE {where}",
                args,
            )
        except Exception:  # noqa: BLE001
            continue
        for r in rows:
            seconds, age = _age(now, r.get("updated_at"))
            findings.append({
                "severity": "warn",
                "issue_type": "stuck_task_state",
                "region": region.name,
                "project_id": r["project_id"],
                "project_name": None,
                "resource_id": r["uuid"],
                "resource_name": r.get("display_name") or "",
                "age": age,
                "age_seconds": seconds,
                "details": f"vm_state={r.get('vm_state')} task_state={r.get('task_state')}",
                "hint": "Check nova-compute logs; task may need manual clear via nova reset-state.",
            })
    return findings


def _check_stuck_volumes(region, project_filter, hours: int) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    args: List[Any] = list(_TRANSIENT_VOL_STATUSES) + [hours]
    status_ph = ",".join(["%s"] * len(_TRANSIENT_VOL_STATUSES))
    where = (
        f"deleted = 0 AND status IN ({status_ph}) "
        f"AND updated_at < (UTC_TIMESTAMP() - INTERVAL %s HOUR)"
    )
    if project_filter is not None:
        ph = ",".join(["%s"] * len(project_filter))
        where += f" AND project_id IN ({ph})"
        args.extend(project_filter)
    try:
        rows = query(
            region, cinder_db(),
            f"SELECT id, display_name, project_id, status, updated_at, size "
            f"FROM volumes WHERE {where}",
            args,
        )
    except Exception:  # noqa: BLE001
        return findings
    for r in rows:
        seconds, age = _age(now, r.get("updated_at"))
        severity = "error" if r["status"] == "error" else "warn"
        findings.append({
            "severity": severity,
            "issue_type": "stuck_volume",
            "region": region.name,
            "project_id": r["project_id"],
            "project_name": None,
            "resource_id": r["id"],
            "resource_name": r.get("display_name") or "",
            "age": age,
            "age_seconds": seconds,
            "details": f"status={r['status']} size={r.get('size')}GB",
            "hint": "Volume is stuck in a transient state; investigate cinder-volume agent.",
        })
    return findings


def _check_orphaned_volumes(region, project_filter, days: int) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    args: List[Any] = [days]
    where = (
        "v.deleted = 0 AND v.status = 'available' AND v.attach_status = 'detached' "
        "AND v.created_at < (UTC_TIMESTAMP() - INTERVAL %s DAY)"
    )
    if project_filter is not None:
        ph = ",".join(["%s"] * len(project_filter))
        where += f" AND v.project_id IN ({ph})"
        args.extend(project_filter)
    try:
        rows = query(
            region, cinder_db(),
            f"""SELECT v.id, v.display_name, v.project_id, v.size, v.created_at
                FROM volumes v
                LEFT JOIN volume_attachment va
                  ON va.volume_id = v.id AND va.deleted = 0
                WHERE {where} AND va.id IS NULL""",
            args,
        )
    except Exception:  # noqa: BLE001
        return findings
    for r in rows:
        seconds, age = _age(now, r.get("created_at"))
        findings.append({
            "severity": "info",
            "issue_type": "orphaned_volume",
            "region": region.name,
            "project_id": r["project_id"],
            "project_name": None,
            "resource_id": r["id"],
            "resource_name": r.get("display_name") or "",
            "age": age,
            "age_seconds": seconds,
            "details": f"size={r.get('size')}GB, never/no-longer attached",
            "hint": "Consider deleting the volume or confirming it's intentionally retained.",
        })
    return findings


def _check_old_unbound_fips(region, project_filter, days: int) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    where = "fip.fixed_port_id IS NULL"
    args: List[Any] = []
    if project_filter is not None:
        ph = ",".join(["%s"] * len(project_filter))
        where += f" AND fip.project_id IN ({ph})"
        args.extend(project_filter)
    if days > 0:
        where += (
            " AND fip.standard_attr_id IN ("
            "  SELECT id FROM standardattributes "
            "  WHERE created_at < (UTC_TIMESTAMP() - INTERVAL %s DAY))"
        )
        args.append(days)
    try:
        rows = query(
            region, neutron_db(),
            f"""SELECT fip.id, fip.floating_ip_address, fip.project_id,
                       sa.created_at
                FROM floatingips fip
                LEFT JOIN standardattributes sa ON sa.id = fip.standard_attr_id
                WHERE {where}""",
            args,
        )
    except Exception:  # noqa: BLE001
        return findings
    for r in rows:
        seconds, age = _age(now, r.get("created_at"))
        findings.append({
            "severity": "info",
            "issue_type": "old_unbound_fip",
            "region": region.name,
            "project_id": r["project_id"],
            "project_name": None,
            "resource_id": r["id"],
            "resource_name": r.get("floating_ip_address") or "",
            "age": age,
            "age_seconds": seconds,
            "details": "FIP allocated but not associated with a port",
            "hint": "Release the FIP if it's not planned for use — see `fip_audit` for more context.",
        })
    return findings


def _check_stale_snapshots(region, project_filter, days: int) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    args: List[Any] = [days]
    where = (
        "deleted = 0 "
        "AND created_at < (UTC_TIMESTAMP() - INTERVAL %s DAY)"
    )
    if project_filter is not None:
        ph = ",".join(["%s"] * len(project_filter))
        where += f" AND project_id IN ({ph})"
        args.extend(project_filter)
    try:
        rows = query(
            region, cinder_db(),
            f"""SELECT project_id,
                       COUNT(*) AS n,
                       COALESCE(SUM(volume_size), 0) AS gb,
                       MIN(created_at) AS oldest
                FROM snapshots
                WHERE {where}
                GROUP BY project_id""",
            args,
        )
    except Exception:  # noqa: BLE001
        return findings
    for r in rows:
        seconds, age = _age(now, r.get("oldest"))
        findings.append({
            "severity": "info",
            "issue_type": "stale_snapshots",
            "region": region.name,
            "project_id": r["project_id"],
            "project_name": None,
            "resource_id": f"{r['n']} snapshots",
            "resource_name": f"{int(r.get('gb') or 0)} GB",
            "age": age,
            "age_seconds": seconds,
            "details": f"{r['n']} snapshots older than {days} days; oldest age shown",
            "hint": "Run `stale_snapshots` for the list; delete what's no longer needed.",
        })
    return findings


REPORT = IssuesReport()
