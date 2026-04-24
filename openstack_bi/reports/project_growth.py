"""Project growth: concurrent instance count over time, per project.

Time series is derived from `instances.created_at` and `instances.deleted_at`
(Nova retains soft-deleted rows until archive). No separate time-series
capture mechanism is required — the query scans the current `instances`
table once per cell.

The table shows each project's count at the start and end of the range;
the chart shows per-project time series for the top-N projects by
end-of-range count, with the rest aggregated as "other".
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from openstack_bi import openstack
from openstack_bi.config import parse_regions
from openstack_bi.db import query
from openstack_bi.util import (
    format_bucket_labels,
    make_buckets,
    reconstruct_concurrent_counts,
)

from .base import ChartSpec, Param, Report, ReportResult


def _domain_choices() -> List[Tuple[str, str]]:
    return [
        (d["name"], f'{d["name"]} ({d["project_count"]} project(s))')
        for d in openstack.list_domains()
    ]


def _region_choices() -> List[Tuple[str, str]]:
    return [(r.name, r.name) for r in parse_regions()]


_GRANULARITY_CHOICES = [("day", "day"), ("week", "week"), ("month", "month")]


def _granularity_choices() -> List[Tuple[str, str]]:
    return _GRANULARITY_CHOICES


def _fetch_lifecycle_events(
    regions, cell_cache, project_ids: List[str],
) -> Dict[str, List[Tuple[datetime, int]]]:
    """Return {project_id: [(ts, +1|-1), ...]} across every region/cell."""
    if not project_ids:
        return {}
    ph = ",".join(["%s"] * len(project_ids))
    sql = f"""
        SELECT project_id, created_at, deleted_at
        FROM instances
        WHERE project_id IN ({ph})
    """
    events: Dict[str, List[Tuple[datetime, int]]] = defaultdict(list)
    for region in regions:
        for cell in cell_cache[region.name]:
            for r in query(region, cell, sql, project_ids):
                pid = r["project_id"]
                if r.get("created_at") is not None:
                    events[pid].append((r["created_at"], +1))
                if r.get("deleted_at") is not None:
                    events[pid].append((r["deleted_at"], -1))
    return events


class ProjectGrowthReport(Report):
    id = "project_growth"
    name = "Project growth"
    description = (
        "Per-project concurrent instance count over time, reconstructed from "
        "`instances.created_at` / `deleted_at`. Line chart for the top-N "
        "projects plus aggregated 'other'; table shows start vs. end of range."
    )
    params = [
        Param(name="domain", label="Domain", kind="select", required=True,
              choices=_domain_choices,
              help="Keystone domain to scope the report."),
        Param(name="granularity", label="Granularity", kind="select",
              default="week", choices=_granularity_choices,
              help="Bucket size for the time series."),
        Param(name="months", label="Months of history", kind="int",
              default=12, placeholder="12",
              help="How many months back from today to include."),
        Param(name="top", label="Top N projects (chart)", kind="int",
              default=10, placeholder="10",
              help="Individual series for the top-N; rest aggregated as 'other'. 0 = single total."),
        Param(name="regions", label="Regions", kind="multiselect",
              choices=_region_choices,
              help="Which regions to span. Empty = all configured regions."),
    ]

    def run(
        self,
        domain: str,
        granularity: str = "week",
        months: Optional[int] = 12,
        top: Optional[int] = 10,
        regions: Optional[List[str]] = None,
        **_: Any,
    ) -> ReportResult:
        selected_region_names = regions or None
        all_regions = parse_regions()
        if selected_region_names is None:
            selected_regions = all_regions
        else:
            by_name = {r.name: r for r in all_regions}
            selected_regions = [by_name[n] for n in selected_region_names if n in by_name]

        domain_obj = openstack.find_domain(domain)
        if domain_obj is None:
            return ReportResult(
                columns=[],
                rows=[],
                metadata={"error": f"Domain not found: {domain!r}"},
                filename_stem=f"project-growth-{domain or 'no-domain'}",
            )

        projects = openstack.list_projects(domain_obj["id"])
        project_ids = [p["id"] for p in projects]
        name_by_id = {p["id"]: p["name"] for p in projects}
        if not project_ids:
            return ReportResult(
                columns=[],
                rows=[],
                metadata={"error": f"Domain {domain_obj['name']!r} has no enabled projects."},
                filename_stem=f"project-growth-{domain_obj['name']}",
            )

        months_back = max(1, int(months or 12))
        end = datetime.utcnow().replace(hour=23, minute=59, second=59, microsecond=0)
        start = end - timedelta(days=30 * months_back)

        boundaries = make_buckets(start, end, granularity)
        if not boundaries:
            return ReportResult(
                columns=[],
                rows=[],
                metadata={"error": "Empty range after bucketing; check granularity/months."},
                filename_stem=f"project-growth-{domain_obj['name']}",
            )
        labels = format_bucket_labels(boundaries, granularity)

        cell_cache = {r.name: openstack.list_cells(r) for r in selected_regions}
        events_by_project = _fetch_lifecycle_events(selected_regions, cell_cache, project_ids)

        # Per-project time series.
        series_by_project: Dict[str, List[int]] = {}
        for pid in project_ids:
            series_by_project[pid] = reconstruct_concurrent_counts(
                events_by_project.get(pid, []), boundaries,
            )

        # Rank by end-of-range count, then by max-count in range to break ties.
        def rank_key(pid: str) -> Tuple[int, int, str]:
            series = series_by_project[pid]
            return (-(series[-1] if series else 0),
                    -(max(series) if series else 0),
                    name_by_id.get(pid, ""))

        ranked = sorted(project_ids, key=rank_key)

        top_n = max(0, int(top or 10))
        top_pids = ranked[:top_n] if top_n > 0 else []
        rest_pids = [pid for pid in ranked if pid not in set(top_pids)]

        chart_series: List[Dict[str, Any]] = []
        if top_n == 0:
            total = [0] * len(boundaries)
            for pid in project_ids:
                for i, v in enumerate(series_by_project[pid]):
                    total[i] += v
            chart_series.append({"label": "total", "data": total})
        else:
            for pid in top_pids:
                chart_series.append({
                    "label": name_by_id.get(pid, pid[:8]),
                    "data": series_by_project[pid],
                })
            if rest_pids:
                other = [0] * len(boundaries)
                for pid in rest_pids:
                    for i, v in enumerate(series_by_project[pid]):
                        other[i] += v
                chart_series.append({"label": f"other ({len(rest_pids)})", "data": other})

        chart = ChartSpec(
            kind="line",
            title=f"Concurrent instances — {domain_obj['name']} ({granularity})",
            x_label=granularity,
            y_label="Instances",
            x_categories=labels,
            series=chart_series,
        )

        # Table: one row per project, start/end/change/peak.
        rows_out: List[Dict[str, Any]] = []
        for pid in ranked:
            s = series_by_project[pid]
            start_count = s[0] if s else 0
            end_count = s[-1] if s else 0
            peak = max(s) if s else 0
            rows_out.append({
                "project_id": pid,
                "project_name": name_by_id.get(pid, "(unknown)"),
                "start": start_count,
                "end": end_count,
                "change": end_count - start_count,
                "peak": peak,
            })

        metadata = {
            "domain": domain_obj["name"],
            "granularity": granularity,
            "range": f"{labels[0]} → {labels[-1]}",
            "regions": ", ".join(r.name for r in selected_regions) or "(none)",
            "projects": len(project_ids),
            "buckets": len(boundaries),
        }

        stem_bits = [
            "project-growth", domain_obj["name"], granularity, f"{months_back}mo",
        ]
        stem_bits.append(
            "-".join(r.name for r in selected_regions)
            if selected_region_names is not None else "all-regions"
        )

        return ReportResult(
            columns=[
                ("project_name", "Project"),
                ("project_id", "Project ID"),
                ("start", f"@ {labels[0]}"),
                ("end", f"@ {labels[-1]}"),
                ("change", "Change"),
                ("peak", "Peak"),
            ],
            rows=rows_out,
            charts=[chart],
            metadata=metadata,
            filename_stem="-".join(stem_bits),
        )


REPORT = ProjectGrowthReport()
