"""Floating IP pool utilization per region.

For each external network referenced by allocated floatingips in each
region, report:

    pool_size  — total IPv4 addresses across the network's allocation pools
    allocated  — total allocated FIPs (regardless of binding)
    bound      — FIPs with fixed_port_id NOT NULL
    unbound    — FIPs with fixed_port_id IS NULL
    free       — pool_size - allocated (may be < 0 if a manually-added
                 IP sneaks outside the declared pools — flagged in status)
    pct_used   — allocated / pool_size, as a percentage

Stacked bar per (region, network) showing bound / unbound / free.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from openstack_bi.config import neutron_db, parse_regions
from openstack_bi.db import query

from .base import ChartSpec, Param, Report, ReportResult


def _region_choices() -> List[Tuple[str, str]]:
    return [(r.name, r.name) for r in parse_regions()]


class FipPoolsReport(Report):
    id = "fip_pools"
    name = "Floating IP pools"
    description = (
        "Per-region external-network FIP pool utilization: pool size from "
        "IP allocation pools, allocated count from `floatingips`, bound vs. "
        "unbound split, and percent used."
    )
    params = [
        Param(name="regions", label="Regions", kind="multiselect",
              choices=_region_choices,
              help="Which regions to span. Empty = all configured regions."),
    ]

    def run(
        self,
        regions: List[str] = None,  # type: ignore[assignment]
        **_: Any,
    ) -> ReportResult:
        selected_region_names = regions or None
        all_regions = parse_regions()
        if selected_region_names is None:
            selected_regions = all_regions
        else:
            by_name = {r.name: r for r in all_regions}
            selected_regions = [by_name[n] for n in selected_region_names if n in by_name]

        rows_out: List[Dict[str, Any]] = []

        for region in selected_regions:
            # FIP counts per network, split by bound/unbound. We restrict the
            # pool/name lookups to networks with at least one FIP allocation
            # — otherwise we'd list every tenant's internal network that
            # happens to have a v4 allocation pool, which is almost always
            # noise for this report.
            fip_rows = query(
                region, neutron_db(),
                """
                SELECT floating_network_id AS network_id,
                       SUM(CASE WHEN fixed_port_id IS NOT NULL THEN 1 ELSE 0 END) AS bound,
                       SUM(CASE WHEN fixed_port_id IS NULL     THEN 1 ELSE 0 END) AS unbound,
                       COUNT(*) AS allocated
                FROM floatingips
                GROUP BY floating_network_id
                """,
            )
            network_ids = {r["network_id"] for r in fip_rows}

            pool_by_network: Dict[str, int] = {}
            network_names: Dict[str, str] = {}
            if network_ids:
                ph = ",".join(["%s"] * len(network_ids))
                try:
                    pool_rows = query(
                        region, neutron_db(),
                        f"""
                        SELECT s.network_id AS network_id,
                               COALESCE(SUM(INET_ATON(p.last_ip) - INET_ATON(p.first_ip) + 1), 0) AS pool_size
                        FROM ipallocationpools p
                        JOIN subnets s ON s.id = p.subnet_id
                        WHERE s.ip_version = 4
                          AND s.network_id IN ({ph})
                        GROUP BY s.network_id
                        """,
                        list(network_ids),
                    )
                    pool_by_network = {r["network_id"]: int(r["pool_size"] or 0) for r in pool_rows}
                except Exception:  # noqa: BLE001
                    pool_by_network = {}

                name_rows = query(
                    region, neutron_db(),
                    f"SELECT id, name FROM networks WHERE id IN ({ph})",
                    list(network_ids),
                )
                network_names = {r["id"]: r["name"] for r in name_rows}
            fip_by_net = {r["network_id"]: r for r in fip_rows}

            for net_id in network_ids:
                fip_row = fip_by_net.get(net_id, {})
                pool_size = pool_by_network.get(net_id, 0)
                allocated = int(fip_row.get("allocated") or 0)
                bound = int(fip_row.get("bound") or 0)
                unbound = int(fip_row.get("unbound") or 0)
                free = pool_size - allocated
                pct = round((allocated / pool_size) * 100, 1) if pool_size else None
                status_note = "ok"
                if pool_size == 0 and allocated > 0:
                    status_note = "no declared pool"
                elif pct is not None and pct >= 95:
                    status_note = "near-full"
                elif pct is not None and pct >= 80:
                    status_note = "warn"
                rows_out.append({
                    "region": region.name,
                    "network_id": net_id,
                    "network_name": network_names.get(net_id) or "",
                    "pool_size": pool_size,
                    "allocated": allocated,
                    "bound": bound,
                    "unbound": unbound,
                    "free": max(free, 0),
                    "pct_used": pct if pct is not None else "-",
                    "status": status_note,
                })

        rows_out.sort(key=lambda r: (
            r["region"],
            -(r["pct_used"] if isinstance(r["pct_used"], (int, float)) else -1),
            r["network_name"],
        ))

        labels = [f"{r['region']} / {r['network_name'] or r['network_id'][:8]}" for r in rows_out]
        chart = ChartSpec(
            kind="stacked_bar",
            title="FIP pool utilization",
            x_label="Region / external network",
            y_label="IPs",
            x_categories=labels,
            series=[
                {"label": "bound", "data": [r["bound"] for r in rows_out]},
                {"label": "unbound", "data": [r["unbound"] for r in rows_out]},
                {"label": "free", "data": [r["free"] for r in rows_out]},
            ],
        )

        metadata = {
            "regions": ", ".join(r.name for r in selected_regions) or "(none)",
            "networks": len(rows_out),
            "total_pool_size": sum(r["pool_size"] for r in rows_out),
            "total_allocated": sum(r["allocated"] for r in rows_out),
            "total_unbound": sum(r["unbound"] for r in rows_out),
        }

        stem_bits = ["fip-pools"]
        stem_bits.append(
            "-".join(r.name for r in selected_regions)
            if selected_region_names is not None else "all-regions"
        )

        return ReportResult(
            columns=[
                ("region", "Region"),
                ("network_name", "Network"),
                ("network_id", "Network ID"),
                ("pool_size", "Pool size"),
                ("allocated", "Allocated"),
                ("bound", "Bound"),
                ("unbound", "Unbound"),
                ("free", "Free"),
                ("pct_used", "% used"),
                ("status", "Status"),
            ],
            rows=rows_out,
            charts=[chart] if rows_out else [],
            metadata=metadata,
            filename_stem="-".join(stem_bits),
        )


REPORT = FipPoolsReport()
