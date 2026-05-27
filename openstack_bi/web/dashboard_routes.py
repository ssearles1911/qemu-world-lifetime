"""Cloud health dashboard.

Single page at `/dashboard` that replaces the team's daily ops-health
email with an always-on, browsable view. Live current values come from
the regional MariaDB replicas via `dashboard_metrics.current_snapshot`;
trend data comes from the local SQLite `dashboard_metric_history`
table (populated by `opsbi snapshot-metrics`).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from flask import (
    Flask, Response, abort, jsonify, render_template, request,
)

from .. import dashboard_metrics as dm
from ..auth.session import login_required
from ..config import parse_regions

# Maps a metric key to its display label and an optional related-report
# id used for the "→ View report" chevron in each tile.
METRIC_DISPLAY: Dict[str, Tuple[str, Optional[str]]] = {
    "instances_total":         ("Instances",            "instance_leaderboard"),
    "instances_error":         ("Instances in ERROR",   "issues"),
    "routers_total":           ("Routers",              None),
    "routers_error":           ("Routers in ERROR",     "issues"),
    "ports_total":             ("Network ports",        None),
    "ports_build":             ("Ports in BUILD",       None),
    "ports_error":             ("Ports in ERROR",       "issues"),
    "floating_ips_total":      ("Floating IPs",         "fip_audit"),
    "vpn_connections_active":  ("VPN connections",      None),
    "vpn_connections_down":    ("VPN connections DOWN", None),
    "volumes_total":           ("Volumes",              None),
    "volumes_error":           ("Volumes in ERROR",     "issues"),
    "snapshots_total":         ("Snapshots",            "snapshot_leaderboard"),
    "snapshots_error":         ("Snapshots in ERROR",   "issues"),
    "snapshots_autobackup_today": ("Autobackups today", None),
    "keystone_domains":        ("Domains",              None),
    "keystone_projects":       ("Projects",             None),
}

# The metric order on Zone B (cloud-wide totals). Anomaly counters
# (the *_error and ports_build metrics) live in Zone A instead.
TOTALS_METRICS: List[str] = [
    "instances_total",
    "routers_total",
    "ports_total",
    "floating_ips_total",
    "vpn_connections_active",
    "volumes_total",
    "snapshots_total",
    "keystone_domains",
    "keystone_projects",
]

# Critical anomaly metrics (any > 0 lights up the danger zone).
CRITICAL_ANOMALY_METRICS: List[str] = [
    "instances_error",
    "ports_error",
    "volumes_error",
    "snapshots_error",
    "routers_error",
    "vpn_connections_down",
]

# Warning-level anomalies (BUILD is transient, not a fault).
WARNING_ANOMALY_METRICS: List[str] = ["ports_build"]

# Per-region table on Zone E — every metric the regional DBs produce,
# skipping the keystone global metrics (they don't have per-region rows).
PER_REGION_METRICS: List[str] = [
    "instances_total", "instances_error",
    "ports_total", "ports_build", "ports_error",
    "routers_total", "routers_error",
    "floating_ips_total",
    "vpn_connections_total", "vpn_connections_active", "vpn_connections_down",
    "volumes_total", "volumes_error",
    "snapshots_total", "snapshots_error",
    "snapshots_autobackup_today",
]

RANGE_DAYS: Dict[str, int] = {"7d": 7, "30d": 30, "90d": 90, "1y": 365}


def register(app: Flask) -> None:
    app.add_url_rule("/dashboard", view_func=dashboard, endpoint="dashboard")
    app.add_url_rule(
        "/dashboard.json", view_func=dashboard_json, endpoint="dashboard_json",
    )
    app.add_url_rule(
        "/dashboard/backups.csv", view_func=backups_csv,
        endpoint="dashboard_backups_csv",
    )


# --- View helpers -----------------------------------------------------------

def _history_bulk(days: int) -> Dict[Tuple[str, str], List[Tuple[str, int]]]:
    """`{(region, metric): [(date, value)…]}` for the last `days` days.

    One SQLite query for the entire dashboard's worth of trend data.
    """
    from .. import config_db
    out: Dict[Tuple[str, str], List[Tuple[str, int]]] = {}
    days = max(1, int(days))
    with config_db.cursor() as cur:
        cur.execute(
            "SELECT region, metric, snapshot_date, value "
            "FROM dashboard_metric_history "
            "WHERE snapshot_date >= date('now', ?) "
            "ORDER BY snapshot_date",
            (f"-{days} days",),
        )
        for r in cur.fetchall():
            out.setdefault((r["region"], r["metric"]), []).append(
                (r["snapshot_date"], int(r["value"]))
            )
    return out


def _last_two_history_values(
    history: Dict[Tuple[str, str], List[Tuple[str, int]]],
    region: str, metric: str,
) -> Tuple[Optional[int], Optional[int]]:
    """Return `(latest, previous)` history values for one (region, metric).

    Used to compute the `vs yesterday` delta. `None` for either when
    insufficient history exists (collector hasn't run enough days yet).
    """
    series = history.get((region, metric), [])
    if len(series) >= 2:
        return series[-1][1], series[-2][1]
    if len(series) == 1:
        return series[0][1], None
    return None, None


def _resolve_region_filter(region_filter: str, regions) -> str:
    """Map a request param to the row's `region` value in the history
    table. `'all'` -> `'_combined'`; anything else must be a known
    region name (else falls back to `'all'`)."""
    if region_filter in {r.name for r in regions}:
        return region_filter
    return dm.COMBINED


def _build_tile(
    metric: str,
    current: Dict[Tuple[str, str], int],
    history: Dict[Tuple[str, str], List[Tuple[str, int]]],
    region_key: str,
) -> Dict[str, Any]:
    label, related = METRIC_DISPLAY.get(metric, (metric, None))
    series = history.get((region_key, metric), [])
    value = current.get((region_key, metric))
    if value is None and series:
        value = series[-1][1]
    if value is None:
        value = 0
    today_hist, yest_hist = _last_two_history_values(history, region_key, metric)
    delta: Optional[int] = None
    if today_hist is not None and yest_hist is not None:
        delta = today_hist - yest_hist
    elif yest_hist is None and today_hist is not None and series:
        delta = None  # exactly one day of history — no delta yet
    return {
        "metric": metric,
        "label": label,
        "value": int(value),
        "delta": delta,
        "sparkline": [v for _, v in series],
        "related_report": related,
    }


def _build_anomalies(
    current: Dict[Tuple[str, str], int],
    history: Dict[Tuple[str, str], List[Tuple[str, int]]],
    region_key: str,
    regions,
) -> List[Dict[str, Any]]:
    """Anomaly tiles in severity order (critical first), filtered by
    region scope. For the `_combined` view each tile reports its
    per-region split."""
    anomalies: List[Dict[str, Any]] = []

    def _per_region_split(metric: str) -> List[Tuple[str, int]]:
        if region_key != dm.COMBINED:
            return []
        return [
            (r.name, current.get((r.name, metric), 0)) for r in regions
        ]

    for severity, metrics in (
        ("critical", CRITICAL_ANOMALY_METRICS),
        ("warning", WARNING_ANOMALY_METRICS),
    ):
        for m in metrics:
            value = current.get((region_key, m), 0)
            if value <= 0:
                continue
            label, related = METRIC_DISPLAY.get(m, (m, None))
            anomalies.append({
                "metric": m,
                "label": label,
                "value": int(value),
                "severity": severity,
                "splits": _per_region_split(m),
                "related_report": related,
            })

    # A missed autobackup run is its own kind of anomaly (no rows
    # created today). Surface it as critical so it cannot be missed.
    auto_today = current.get((dm.COMBINED, "snapshots_autobackup_today"), 0)
    if auto_today == 0:
        anomalies.append({
            "metric": "snapshots_autobackup_today",
            "label": "Autobackup run missed today",
            "value": 0,
            "severity": "critical",
            "splits": _per_region_split("snapshots_autobackup_today"),
            "related_report": None,
        })

    return anomalies


def _build_backup_section(
    current: Dict[Tuple[str, str], int],
    history: Dict[Tuple[str, str], List[Tuple[str, int]]],
    regions,
) -> Dict[str, Any]:
    """Today + per-region counts + 30-day adherence strip for Zone C."""
    today_combined = int(current.get((dm.COMBINED, "snapshots_autobackup_today"), 0))
    per_region = {
        r.name: int(current.get((r.name, "snapshots_autobackup_today"), 0))
        for r in regions
    }
    # Adherence strip — last 30 days of the combined autobackup count.
    series = history.get((dm.COMBINED, "snapshots_autobackup_today"), [])
    adherence: List[Dict[str, Any]] = []
    for date_str, value in series[-30:]:
        if value == 0:
            state = "missed"
        elif value < 10:
            state = "partial"
        else:
            state = "ok"
        adherence.append({"date": date_str, "value": int(value), "state": state})
    missed = [d for d in adherence if d["state"] == "missed"]
    last_missed = missed[-1]["date"] if missed else None
    ok_days = sum(1 for d in adherence if d["state"] == "ok")
    return {
        "today_combined": today_combined,
        "per_region": per_region,
        "adherence": adherence,
        "ok_days": ok_days,
        "total_days": len(adherence),
        "last_missed": last_missed,
    }


def _build_trend_charts(
    history: Dict[Tuple[str, str], List[Tuple[str, int]]],
    regions,
) -> Dict[str, Any]:
    """Spec dicts for Zone D's Chart.js renders.

    Each chart is `{kind, title, x_label, y_label, x_categories,
    series}` so we can feed them into the same Chart.js init code the
    report page uses.
    """

    def _dates_union(*keys: Tuple[str, str]) -> List[str]:
        seen: List[str] = []
        seen_set: set = set()
        for key in keys:
            for d, _ in history.get(key, []):
                if d not in seen_set:
                    seen.append(d)
                    seen_set.add(d)
        return seen

    def _series_for(metric: str, region_name: str, dates: List[str]) -> List[int]:
        m = {d: v for d, v in history.get((region_name, metric), [])}
        return [m.get(d, 0) for d in dates]

    region_names = [r.name for r in regions]

    # 1. Compute footprint — stacked area of per-region instances_total.
    dates = _dates_union(*((r, "instances_total") for r in region_names))
    compute_chart = {
        "kind": "stacked_bar",   # Chart.js stacked area = stacked bar with line type; reuse stacked_bar handler
        "title": "Instances by region",
        "x_label": "", "y_label": "instances",
        "x_categories": dates,
        "series": [
            {"label": r, "data": _series_for("instances_total", r, dates)}
            for r in region_names
        ],
    }

    # 2. Network footprint — combined ports build + error over time.
    dates = _dates_union((dm.COMBINED, "ports_build"), (dm.COMBINED, "ports_error"))
    network_chart = {
        "kind": "stacked_bar",
        "title": "Network ports — BUILD vs ERROR",
        "x_label": "", "y_label": "ports",
        "x_categories": dates,
        "series": [
            {"label": "BUILD", "data": _series_for("ports_build", dm.COMBINED, dates)},
            {"label": "ERROR", "data": _series_for("ports_error", dm.COMBINED, dates)},
        ],
    }

    # 3. Storage growth — cloud-wide volumes + snapshots, two lines.
    dates = _dates_union((dm.COMBINED, "volumes_total"), (dm.COMBINED, "snapshots_total"))
    storage_chart = {
        "kind": "line",
        "title": "Storage growth",
        "x_label": "", "y_label": "count",
        "x_categories": dates,
        "series": [
            {"label": "Volumes",   "data": _series_for("volumes_total", dm.COMBINED, dates)},
            {"label": "Snapshots", "data": _series_for("snapshots_total", dm.COMBINED, dates)},
        ],
    }

    # 4. Daily autobackup runs — cloud-wide.
    dates = _dates_union((dm.COMBINED, "snapshots_autobackup_today"))
    backup_chart = {
        "kind": "bar",
        "title": "Daily autobackup runs",
        "x_label": "", "y_label": "snapshots created",
        "x_categories": dates,
        "series": [
            {"label": "Autobackups",
             "data": _series_for("snapshots_autobackup_today", dm.COMBINED, dates)},
        ],
    }
    return {
        "compute": compute_chart, "network": network_chart,
        "storage": storage_chart, "backup": backup_chart,
    }


def _build_per_region_table(
    current: Dict[Tuple[str, str], int],
    history: Dict[Tuple[str, str], List[Tuple[str, int]]],
    regions,
) -> List[Dict[str, Any]]:
    """Rows for Zone E — every per-region metric across DTW / CVG / Total."""
    out: List[Dict[str, Any]] = []
    region_names = [r.name for r in regions]
    for m in PER_REGION_METRICS:
        label = METRIC_DISPLAY.get(m, (m, None))[0]
        per_region_values = {
            r: int(current.get((r, m), 0)) for r in region_names
        }
        total = int(current.get((dm.COMBINED, m), sum(per_region_values.values())))
        out.append({
            "metric": m, "label": label,
            "per_region": per_region_values,
            "total": total,
            "sparkline": [v for _, v in history.get((dm.COMBINED, m), [])],
        })
    return out


def _gather(region_filter: str, range_str: str) -> Dict[str, Any]:
    """Compute every piece of data the dashboard template + JSON view
    needs. Pure data — no Flask response building.

    Reads exclusively from the local SQLite history table (the
    collector's output). Live MariaDB-replica reads are intentionally
    NOT done at page-render time — they take ~20 seconds when Cinder's
    snapshot table is large, which is incompatible with the spec's
    sub-second loading goal. The collector runs from cron daily; the
    ⟳ refresh button re-runs it explicitly when the operator wants
    fresher data than the last cron tick.
    """
    days = RANGE_DAYS.get(range_str, 30)
    regions = parse_regions()
    region_key = _resolve_region_filter(region_filter, regions)

    current, snapshot_at = _current_from_history()
    history = _history_bulk(days)
    region_errors: List[Dict[str, str]] = []
    if not current:
        region_errors.append({
            "region": "(any)",
            "error": "no metric history yet — run `opsbi snapshot-metrics` "
                     "or click ⟳ to take the first snapshot",
        })

    tiles = [_build_tile(m, current, history, region_key) for m in TOTALS_METRICS]
    anomalies = _build_anomalies(current, history, region_key, regions)
    backups = _build_backup_section(current, history, regions)
    charts = _build_trend_charts(history, regions)
    per_region_rows = (
        _build_per_region_table(current, history, regions)
        if region_filter == "all" else []
    )

    return {
        "as_of": snapshot_at or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "region_filter": region_filter,
        "region_key": region_key,
        "range": range_str,
        "regions": [{"name": r.name} for r in regions],
        "tiles": tiles,
        "anomalies": anomalies,
        "backups": backups,
        "charts": charts,
        "per_region_rows": per_region_rows,
        "region_errors": region_errors,
    }


def _current_from_history() -> Tuple[Dict[Tuple[str, str], int], Optional[str]]:
    """Most-recent row per `(region, metric)` from SQLite history.

    This is the primary source for the "current" tile values. Returns
    `(snapshot_dict, snapshot_at)`; both are empty/`None` when the
    collector has never run, in which case the dashboard renders a
    banner pointing the operator at `opsbi snapshot-metrics`.
    """
    from .. import config_db
    out: Dict[Tuple[str, str], int] = {}
    snapshot_at: Optional[str] = None
    try:
        with config_db.cursor() as cur:
            cur.execute(
                "SELECT region, metric, value, snapshot_at "
                "FROM dashboard_metric_history "
                "WHERE snapshot_date = ("
                "  SELECT MAX(snapshot_date) FROM dashboard_metric_history"
                ")"
            )
            for r in cur.fetchall():
                out[(r["region"], r["metric"])] = int(r["value"])
                if snapshot_at is None or r["snapshot_at"] > snapshot_at:
                    snapshot_at = r["snapshot_at"]
    except Exception:  # noqa: BLE001
        return out, snapshot_at
    return out, snapshot_at


# --- Views -----------------------------------------------------------------

@login_required
def dashboard():
    region_filter = (request.args.get("region") or "all").strip()
    range_str = (request.args.get("range") or "30d").strip()
    data = _gather(region_filter, range_str)
    return render_template("dashboard.html", **data)


@login_required
def dashboard_json():
    """Partial-refresh JSON endpoint. Same data shape as the page.

    The ⟳ button on the dashboard uses this to refresh tile values,
    sparklines, and chart data in place. Pass `?fresh=1` to first run
    a fresh metric collection (live MariaDB read) and persist it to
    the history table — this is the explicit "I want newer than the
    last cron tick" path. Without `fresh=1` the endpoint just reads
    what's already in SQLite (sub-second).
    """
    region_filter = (request.args.get("region") or "all").strip()
    range_str = (request.args.get("range") or "30d").strip()
    fresh = (request.args.get("fresh") or "").strip() in {"1", "true", "yes"}
    if fresh:
        try:
            rows = dm.collect_snapshot()
            dm.write_snapshot(rows)
        except Exception as exc:  # noqa: BLE001
            return jsonify(ok=False, error=str(exc)), 502
    data = _gather(region_filter, range_str)
    data["ok"] = True
    return jsonify(data)


@login_required
def backups_csv():
    """Per-region autobackup CSV download for Zone C."""
    region_name = (request.args.get("region") or "").strip()
    date = (request.args.get("date") or dm._today()).strip()
    regions = {r.name: r for r in parse_regions()}
    region = regions.get(region_name)
    if region is None:
        abort(404, f"Unknown region {region_name!r}.")
    body = dm.today_autobackups_csv(region, date)
    filename = f"autobackup-{region_name}-{date}.csv"
    return Response(
        body, mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
