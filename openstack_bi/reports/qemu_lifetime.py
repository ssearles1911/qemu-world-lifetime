"""QEMU instance lifetime report.

For every instance in a given Keystone domain, resolve the most recent
Nova lifecycle action (start/stop/shelve/unshelve/shelveOffload/
live-migration) and the time since it occurred. Groups output by project.

Spans every configured region by default; the `regions` param narrows it.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from openstack_bi import openstack
from openstack_bi.config import Region, parse_regions
from openstack_bi.db import query
from openstack_bi.util import annotate_ages, format_region_errors, safe_for_each_region

from .base import Param, Report, ReportResult


ALL_STATES_SENTINEL = "__all__"


# Nova action names that count as a QEMU lifecycle event for this report.
# Reboot, migrate, resize, rebuild, and create are intentionally excluded.
LIFECYCLE_ACTIONS: Tuple[str, ...] = (
    "start",
    "stop",
    "shelve",
    "unshelve",
    "shelveOffload",
    "live-migration",
)


# Common Nova `instances.vm_state` values, in roughly the order users
# care about them.
COMMON_VM_STATES: Tuple[str, ...] = (
    "active",
    "stopped",
    "paused",
    "suspended",
    "shelved",
    "shelved_offloaded",
    "error",
    "building",
    "rescued",
    "resized",
    "soft-deleted",
)

# Default state filter — the operational interest is in *running* instances.
DEFAULT_VM_STATES: Tuple[str, ...] = ("active",)


def _fetch_instances(
    region: Region,
    cell_db: str,
    project_ids: Sequence[str],
    days: Optional[int],
    vm_states: Optional[Sequence[str]],
) -> List[Dict[str, Any]]:
    if not project_ids:
        return []

    proj_ph = ",".join(["%s"] * len(project_ids))
    act_ph = ",".join(["%s"] * len(LIFECYCLE_ACTIONS))

    sql = f"""
        WITH project_instances AS (
            SELECT uuid, project_id
            FROM instances
            WHERE deleted = 0
              AND project_id IN ({proj_ph})
        ),
        ranked AS (
            SELECT ia.instance_uuid, ia.action, ia.start_time, ia.user_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY ia.instance_uuid
                       ORDER BY ia.start_time DESC
                   ) AS rn
            FROM instance_actions ia
            JOIN project_instances pi ON pi.uuid = ia.instance_uuid
            WHERE ia.deleted = 0
              AND ia.action IN ({act_ph})
        )
        SELECT
            i.uuid                                  AS uuid,
            i.display_name                          AS name,
            i.host                                  AS compute_host,
            i.vm_state                              AS vm_state,
            i.power_state                           AS power_state,
            i.created_at                            AS created_at,
            i.project_id                            AS project_id,
            r.action                                AS last_action,
            r.start_time                            AS last_action_time,
            r.user_id                               AS last_action_user,
            COALESCE(r.start_time, i.created_at)    AS effective_time
        FROM instances i
        LEFT JOIN ranked r ON r.instance_uuid = i.uuid AND r.rn = 1
        WHERE i.deleted = 0
          AND i.project_id IN ({proj_ph})
    """

    args: List[Any] = list(project_ids) + list(LIFECYCLE_ACTIONS) + list(project_ids)

    if vm_states:
        state_ph = ",".join(["%s"] * len(vm_states))
        sql += f" AND i.vm_state IN ({state_ph})"
        args.extend(vm_states)

    if days is not None:
        sql += " AND COALESCE(r.start_time, i.created_at) < (UTC_TIMESTAMP() - INTERVAL %s DAY)"
        args.append(days)

    sql += " ORDER BY i.project_id, effective_time"
    rows = query(region, cell_db, sql, args)
    for row in rows:
        row["region"] = region.name
    return rows


def _state_choices() -> List[Tuple[str, str]]:
    return [(s, s) for s in COMMON_VM_STATES] + [(ALL_STATES_SENTINEL, "— all states —")]


def _domain_choices() -> List[Tuple[str, str]]:
    return [
        (d["name"], f'{d["name"]} ({d["project_count"]} project(s))')
        for d in openstack.list_domains()
    ]


def _region_choices() -> List[Tuple[str, str]]:
    return [(r.name, r.name) for r in parse_regions()]


class QemuLifetimeReport(Report):
    id = "qemu_lifetime"
    name = "QEMU lifetime"
    description = (
        "Last start / stop / shelve / unshelve / shelveOffload / live-migration "
        "event per instance, grouped by project. Filter by domain, state, "
        "min-age days, and region."
    )
    params = [
        Param(name="domain", label="Domain", kind="select", required=True,
              choices=_domain_choices,
              help="Keystone domain to scope the report."),
        Param(name="state", label="State", kind="select",
              default="active", choices=_state_choices,
              help="vm_state filter. Defaults to 'active' (running VMs)."),
        Param(name="days", label="Min days since last event", kind="int",
              placeholder="any",
              help="Only show instances whose last lifecycle event is older than this many days."),
        Param(name="regions", label="Regions", kind="multiselect",
              choices=_region_choices,
              help="Which regions to span. Empty = all configured regions."),
    ]

    def run(
        self,
        domain: str,
        state: Optional[str] = None,
        days: Optional[int] = None,
        regions: Optional[List[str]] = None,
        **_: Any,
    ) -> ReportResult:
        if not state:
            vm_states: Optional[List[str]] = list(DEFAULT_VM_STATES)
        elif state == ALL_STATES_SENTINEL:
            vm_states = None
        else:
            vm_states = [state]

        selected_region_names = regions or None
        if selected_region_names is None:
            selected_regions = parse_regions()
        else:
            by_name = {r.name: r for r in parse_regions()}
            selected_regions = [by_name[n] for n in selected_region_names if n in by_name]

        domain_obj = openstack.find_domain(domain)
        if domain_obj is None:
            return ReportResult(
                columns=[],
                rows=[],
                metadata={"error": f"Domain not found: {domain!r}"},
                filename_stem=f"qemu-lifetime-{domain or 'no-domain'}",
            )

        projects = openstack.list_projects(domain_obj["id"])
        project_ids = [p["id"] for p in projects]
        name_by_id = {p["id"]: p["name"] for p in projects}

        def _collect(region: Region) -> List[Dict[str, Any]]:
            out: List[Dict[str, Any]] = []
            for cell in openstack.list_cells(region):
                out.extend(_fetch_instances(region, cell, project_ids, days, vm_states))
            return out

        results, region_errors = safe_for_each_region(selected_regions, _collect)
        rows: List[Dict[str, Any]] = [r for _, rs in results for r in rs]

        for row in rows:
            row["project_name"] = name_by_id.get(row["project_id"])

        annotate_ages(rows)

        columns = [
            ("project_name", "Project"),
            ("region", "Region"),
            ("uuid", "Instance UUID"),
            ("name", "Instance"),
            ("compute_host", "Host"),
            ("vm_state", "State"),
            ("last_action", "Last action"),
            ("last_action_time", "Last action time (UTC)"),
            ("age", "Age"),
            ("created_at", "Created at"),
        ]

        rows.sort(key=lambda r: (
            r.get("project_name") or "",
            r.get("region") or "",
            -(r.get("age_seconds") or 0),
        ))

        selected_names = [r.name for r in selected_regions]
        metadata = {
            "domain": domain_obj["name"],
            "domain_id": domain_obj["id"],
            "regions": ", ".join(selected_names) if selected_names else "(none)",
            "state_filter": ", ".join(vm_states) if vm_states else "(all states)",
            "days_filter": (
                f"no event in last {days} day(s)" if days is not None else "(none)"
            ),
            "lifecycle_actions": ", ".join(LIFECYCLE_ACTIONS),
            "total_instances": len(rows),
            "projects_with_data": len({r.get("project_id") for r in rows}),
            "region_errors": format_region_errors(region_errors),
        }

        stem_bits = [domain_obj["name"], "qemu-lifetime"]
        stem_bits.append("-".join(vm_states) if vm_states else "all-states")
        stem_bits.append("-".join(selected_names) if selected_region_names is not None else "all-regions")
        if days is not None:
            stem_bits.append(f"{days}d")

        return ReportResult(
            columns=columns,
            rows=rows,
            groupings=["project_name"],
            metadata=metadata,
            filename_stem="-".join(stem_bits),
        )


REPORT = QemuLifetimeReport()
