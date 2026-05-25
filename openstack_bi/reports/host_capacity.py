"""Per-host compute capacity heat map.

One row per Nova compute node across every selected datacenter and cell:
its physical CPU / memory capacity, how much is allocated, how many
instances it carries, the resulting utilization percentages, an overall
oversubscription ratio, and whether its nova-compute service is enabled
or disabled. The numeric cells are colour-shaded green / orange / red by
configurable thresholds so an operator can eyeball where the cluster is
running hot, and the enabled/disabled column is shaded green/red.

Data sources (one query per region/cell against the Nova cell DB):
  • `compute_nodes` — vcpus, vcpus_used, memory_mb, memory_mb_used,
    running_vms (the "instances assigned" count Nova keeps).
  • `services`     — the matching `nova-compute` row's `disabled` flag.

"Allocated" is Nova's own accounting (`*_used`), i.e. the sum of the
flavors placed on the host plus reservations — not live guest
consumption. CPU is routinely oversubscribed, so its utilization can
(and should) exceed 100%; memory normally cannot. The shading thresholds
default accordingly and are exposed as advanced options.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from openstack_bi import openstack
from openstack_bi.config import Region, parse_regions
from openstack_bi.db import query
from openstack_bi.util import format_region_errors, safe_for_each_region

from .base import Param, Report, ReportResult


# Defaults for the colour-shading thresholds, in percent. CPU is meant to
# be oversubscribed, so its bands sit well above 100%; memory is real, so
# its bands sit below it. All overridable per run.
DEFAULT_CPU_WARN_PCT = 150
DEFAULT_CPU_CRIT_PCT = 300
DEFAULT_MEM_WARN_PCT = 80
DEFAULT_MEM_CRIT_PCT = 90


# Per-cell query: every live compute node plus its nova-compute service
# row (LEFT JOIN so a node with no matching service still lists, with an
# unknown enabled/disabled state).
_HOST_SQL = """
    SELECT
        cn.hypervisor_hostname AS hostname,
        cn.host                AS host,
        cn.vcpus               AS vcpus,
        cn.vcpus_used          AS vcpus_used,
        cn.memory_mb           AS memory_mb,
        cn.memory_mb_used      AS memory_mb_used,
        cn.running_vms         AS running_vms,
        s.disabled             AS disabled
    FROM compute_nodes cn
    LEFT JOIN services s
      ON s.host = cn.host
     AND s.binary = 'nova-compute'
     AND s.deleted = 0
    WHERE cn.deleted = 0
    ORDER BY cn.hypervisor_hostname
"""


def _region_choices() -> List[Tuple[str, str]]:
    return [(r.name, r.name) for r in parse_regions()]


def _shade(value: Optional[float], warn: float, crit: float) -> str:
    """Heat-map bucket for a 'higher is worse' value against two thresholds.

    `value is None` (no capacity to divide by) reads as gray — "n/a",
    not "healthy". Otherwise: >= crit red, >= warn orange, else green.
    """
    if value is None:
        return "gray"
    if value >= crit:
        return "red"
    if value >= warn:
        return "orange"
    return "green"


def _pct(used: int, total: int) -> Optional[int]:
    """used/total as a rounded percent, or None when total is 0/absent."""
    if not total:
        return None
    return round(used / total * 100)


class HostCapacityReport(Report):
    id = "host_capacity"
    name = "Host capacity"
    description = (
        "Per-compute-host CPU and memory capacity, allocated vs. capacity, "
        "instance count, utilization percentages, and overall "
        "oversubscription ratio across the selected datacenters. Cells are "
        "colour-shaded green / orange / red by threshold, and the "
        "nova-compute enabled/disabled state is shaded green/red."
    )
    category = "Capacity"
    scope_to_projects = False  # infrastructure inventory; admin-only
    params = [
        Param(
            name="regions", label="Datacenters", kind="multiselect",
            choices=_region_choices,
            help="Which datacenters to span. Empty = all configured.",
        ),
        Param(
            name="cpu_warn_pct", label="CPU warn percent", kind="int",
            default=DEFAULT_CPU_WARN_PCT, advanced=True,
            help="CPU allocation percent (allocated vCPU / physical vCPU) at "
                 "or above which a cell shades orange. Also drives the "
                 "oversubscription column.",
        ),
        Param(
            name="cpu_crit_pct", label="CPU critical percent", kind="int",
            default=DEFAULT_CPU_CRIT_PCT, advanced=True,
            help="CPU allocation percent at or above which a cell shades red.",
        ),
        Param(
            name="mem_warn_pct", label="Memory warn percent", kind="int",
            default=DEFAULT_MEM_WARN_PCT, advanced=True,
            help="Memory allocation percent at or above which a cell shades "
                 "orange.",
        ),
        Param(
            name="mem_crit_pct", label="Memory critical percent", kind="int",
            default=DEFAULT_MEM_CRIT_PCT, advanced=True,
            help="Memory allocation percent at or above which a cell shades "
                 "red.",
        ),
    ]

    def run(
        self,
        regions: Optional[List[str]] = None,
        cpu_warn_pct: Optional[int] = DEFAULT_CPU_WARN_PCT,
        cpu_crit_pct: Optional[int] = DEFAULT_CPU_CRIT_PCT,
        mem_warn_pct: Optional[int] = DEFAULT_MEM_WARN_PCT,
        mem_crit_pct: Optional[int] = DEFAULT_MEM_CRIT_PCT,
        **_: Any,
    ) -> ReportResult:
        # Re-apply defaults: web/CLI inject them, but a direct run() (tests,
        # scheduled jobs) may pass None.
        cpu_warn = cpu_warn_pct if cpu_warn_pct is not None else DEFAULT_CPU_WARN_PCT
        cpu_crit = cpu_crit_pct if cpu_crit_pct is not None else DEFAULT_CPU_CRIT_PCT
        mem_warn = mem_warn_pct if mem_warn_pct is not None else DEFAULT_MEM_WARN_PCT
        mem_crit = mem_crit_pct if mem_crit_pct is not None else DEFAULT_MEM_CRIT_PCT

        selected_region_names = regions or None
        if selected_region_names is None:
            selected_regions = parse_regions()
        else:
            by_name = {r.name: r for r in parse_regions()}
            selected_regions = [
                by_name[n] for n in selected_region_names if n in by_name
            ]

        def _collect(region: Region) -> List[Dict[str, Any]]:
            host_rows: List[Dict[str, Any]] = []
            for cell in openstack.list_cells(region):
                for row in query(region, cell, _HOST_SQL):
                    row["region"] = region.name
                    host_rows.append(row)
            return host_rows

        results, region_errors = safe_for_each_region(selected_regions, _collect)
        raw_rows: List[Dict[str, Any]] = [
            r for _region, region_rows in results for r in region_rows
        ]

        rows_out: List[Dict[str, Any]] = []
        for r in raw_rows:
            vcpus = int(r.get("vcpus") or 0)
            vcpus_used = int(r.get("vcpus_used") or 0)
            memory_mb = int(r.get("memory_mb") or 0)
            memory_used_mb = int(r.get("memory_mb_used") or 0)

            cpu_pct = _pct(vcpus_used, vcpus)
            mem_pct = _pct(memory_used_mb, memory_mb)

            # Overall oversubscription: the worse of the two dimensions,
            # as an "x physical capacity" ratio. None when neither
            # dimension has capacity to divide by.
            ratios = []
            if vcpus:
                ratios.append(vcpus_used / vcpus)
            if memory_mb:
                ratios.append(memory_used_mb / memory_mb)
            oversub = round(max(ratios), 2) if ratios else None

            disabled_raw = r.get("disabled")
            if disabled_raw is None:
                service = "Unknown"
                service_shade = "gray"
            elif disabled_raw:
                service = "Disabled"
                service_shade = "red"
            else:
                service = "Enabled"
                service_shade = "green"

            row: Dict[str, Any] = {
                "region": r.get("region") or "",
                "hostname": r.get("hostname") or r.get("host") or "",
                "service": service,
                "instances": int(r.get("running_vms") or 0),
                "vcpus": vcpus,
                "vcpus_used": vcpus_used,
                "cpu_pct": cpu_pct,
                "memory_gb": round(memory_mb / 1024),
                "memory_used_gb": round(memory_used_mb / 1024),
                "mem_pct": mem_pct,
                "oversub": oversub,
                # Heat-map companion fields (see report.html td_cell). They
                # are keyed `<col>_shade`, are not declared columns, and so
                # CLI/Excel ignore them. The column key is deliberately
                # `service` (not `status`) so the template renders plain
                # shaded text instead of a status badge / row tint.
                "service_shade": service_shade,
                "cpu_pct_shade": _shade(cpu_pct, cpu_warn, cpu_crit),
                "mem_pct_shade": _shade(mem_pct, mem_warn, mem_crit),
                "oversub_shade": _shade(
                    oversub * 100 if oversub is not None else None,
                    cpu_warn, cpu_crit,
                ),
            }
            rows_out.append(row)

        rows_out.sort(key=lambda r: (r["region"], r["hostname"]))

        # Per-region rollups + cluster totals, summed from the cleaned rows
        # so the numbers match exactly what's shown in the table.
        region_totals: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {"hosts": 0, "vcpus": 0, "vcpus_used": 0,
                     "memory_mb": 0, "memory_used_mb": 0, "instances": 0,
                     "disabled": 0}
        )
        enabled = disabled = 0
        for r in rows_out:
            t = region_totals[r["region"] or "(unknown)"]
            t["hosts"] += 1
            t["vcpus"] += r["vcpus"]
            t["vcpus_used"] += r["vcpus_used"]
            t["memory_mb"] += r["memory_gb"] * 1024
            t["memory_used_mb"] += r["memory_used_gb"] * 1024
            t["instances"] += r["instances"]
            if r["service"] == "Disabled":
                disabled += 1
                t["disabled"] += 1
            elif r["service"] == "Enabled":
                enabled += 1

        total_vcpus = sum(r["vcpus"] for r in rows_out)
        total_vcpus_used = sum(r["vcpus_used"] for r in rows_out)
        total_mem_gb = sum(r["memory_gb"] for r in rows_out)
        total_mem_used_gb = sum(r["memory_used_gb"] for r in rows_out)

        metadata: Dict[str, Any] = {
            "regions": ", ".join(r.name for r in selected_regions) or "(none)",
            "total_hosts": len(rows_out),
            "enabled_hosts": enabled,
            "disabled_hosts": disabled,
            "total_instances": sum(r["instances"] for r in rows_out),
            "vcpu_allocated_vs_capacity": f"{total_vcpus_used} / {total_vcpus}",
            "memory_allocated_vs_capacity_gb": f"{total_mem_used_gb} / {total_mem_gb}",
            "cpu_thresholds": f"warn {cpu_warn}% · crit {cpu_crit}%",
            "mem_thresholds": f"warn {mem_warn}% · crit {mem_crit}%",
            "region_errors": format_region_errors(region_errors),
        }
        for region_name, t in sorted(region_totals.items()):
            metadata[f"region_{region_name}"] = (
                f"{t['hosts']} hosts ({t['disabled']} disabled) · "
                f"{t['vcpus_used']}/{t['vcpus']} vCPU · "
                f"{t['memory_used_mb'] // 1024}/{t['memory_mb'] // 1024} GB · "
                f"{t['instances']} VMs"
            )

        stem_bits = ["host-capacity"]
        if selected_region_names is not None:
            stem_bits.append("-".join(r.name for r in selected_regions) or "no-regions")
        else:
            stem_bits.append("all-regions")

        return ReportResult(
            columns=[
                ("region", "Region"),
                ("hostname", "Host"),
                ("service", "Service"),
                ("instances", "Instances"),
                ("vcpus", "vCPU capacity"),
                ("vcpus_used", "vCPU allocated"),
                ("cpu_pct", "CPU alloc %"),
                ("memory_gb", "Memory capacity (GB)"),
                ("memory_used_gb", "Memory allocated (GB)"),
                ("mem_pct", "Memory alloc %"),
                ("oversub", "Oversubscription ×"),
            ],
            rows=rows_out,
            metadata=metadata,
            filename_stem="-".join(stem_bits),
        )


REPORT = HostCapacityReport()
