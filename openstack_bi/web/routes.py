"""Web routes: catalog, per-report runner, per-report Excel export."""

from __future__ import annotations

import json
import traceback
from typing import Any, Dict, List

from flask import Flask, abort, flash, redirect, render_template, request, send_file, url_for

from openstack_bi.reports import all_reports, by_id
from openstack_bi.reports.base import ReportResult

from ..auth.session import (
    current_user_project_ids,
    login_required,
    report_visible_to_current_user,
)
from . import excel, forms


def register(app: Flask) -> None:
    app.add_url_rule("/", view_func=catalog, endpoint="catalog")
    app.add_url_rule("/report/<report_id>", view_func=run_report, endpoint="run_report")
    app.add_url_rule(
        "/report/<report_id>/export.xlsx",
        view_func=export_report,
        endpoint="export_report",
    )
    app.register_error_handler(Exception, _render_error)


def _render_error(exc):
    """Render uncaught exceptions as a readable page with a traceback.

    This catches errors that slip past per-region tolerance (schema
    mismatches, unhandled DB drivers, bugs). The traceback is always
    shown — the UI is local-only by convention, so there's no risk of
    leaking it externally.
    """
    from werkzeug.exceptions import HTTPException
    if isinstance(exc, HTTPException):
        return exc
    tb = traceback.format_exc()
    return render_template(
        "error.html",
        reports=all_reports(),
        exception_type=type(exc).__name__,
        exception_message=str(exc),
        traceback=tb,
    ), 500


@login_required
def catalog():
    reports = sorted(all_reports(), key=lambda r: (r.category, r.name))
    visible = [
        {"report": r, "enabled": report_visible_to_current_user(r)}
        for r in reports
    ]
    return render_template("catalog.html", report_entries=visible)


def _resolve_report(report_id: str):
    try:
        return by_id(report_id)
    except KeyError:
        abort(404, f"Unknown report: {report_id}")


def _block_if_invisible(report):
    """Return a redirect response if the current user can't see this report,
    else None. Keeps view code tidy without abusing `abort()`.
    """
    if not report_visible_to_current_user(report):
        flash(
            f"'{report.name}' doesn't yet support project-scoped access. "
            "Ask an administrator to run it for you.",
            "error",
        )
        return redirect(url_for("catalog"))
    return None


@login_required
def run_report(report_id: str):
    report = _resolve_report(report_id)
    blocked = _block_if_invisible(report)
    if blocked is not None:
        return blocked

    # Resolve dynamic choices once per request so multiselects can render
    # checkboxes for every available value.
    param_choices = {p.name: p.resolve_choices() for p in report.params}
    collected = forms.collect(report.params, request)
    values = forms.form_values(report.params, collected)

    result: ReportResult | None = None
    error: str | None = None
    grouped: Dict[tuple, List[Dict[str, Any]]] = {}
    group_order: List[tuple] = []

    # Only run if every required param has a value.
    missing_required = [p for p in report.params if p.required and not collected.get(p.name)]
    if request.args and not missing_required:
        run_kwargs = dict(collected)
        if getattr(report, "scope_to_projects", False):
            run_kwargs["_scope_project_ids"] = current_user_project_ids()
        result = report.run(**run_kwargs)
        if "error" in result.metadata:
            error = result.metadata["error"]
        elif result.groupings:
            seen_keys = set()
            for row in result.rows:
                key = tuple(row.get(g) for g in result.groupings)
                if key not in seen_keys:
                    seen_keys.add(key)
                    group_order.append(key)
                grouped.setdefault(key, []).append(row)

    visible_columns = (
        [(k, label) for k, label in (result.columns if result else []) if not label.startswith("_")]
    )
    numeric_cols = _detect_numeric_cols(result, visible_columns) if result else set()

    return render_template(
        "report.html",
        report=report,
        reports=all_reports(),
        param_choices=param_choices,
        values=values,
        result=result,
        error=error,
        grouped=grouped,
        group_order=group_order,
        visible_columns=visible_columns,
        numeric_cols=numeric_cols,
        charts_json=json.dumps(
            [_chart_to_json(c) for c in (result.charts if result else [])]
        ),
        query_string=request.query_string.decode("ascii"),
    )


def _detect_numeric_cols(result: ReportResult, visible_columns) -> set:
    """Identify columns whose sampled values are all int/float (not bool).

    The web template right-aligns these and renders them with tabular figures
    so counts/sizes scan cleanly. Severity/status columns are skipped — they
    render as badges, not numbers.
    """
    numeric: set = set()
    sample_size = 20
    for key, _ in visible_columns:
        if key in ("severity", "status"):
            continue
        seen_any = False
        all_numeric = True
        for row in result.rows[:sample_size]:
            value = row.get(key)
            if value is None:
                continue
            seen_any = True
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                all_numeric = False
                break
        if seen_any and all_numeric:
            numeric.add(key)
    return numeric


@login_required
def export_report(report_id: str):
    report = _resolve_report(report_id)
    blocked = _block_if_invisible(report)
    if blocked is not None:
        return blocked
    collected = forms.collect(report.params, request)
    missing_required = [p for p in report.params if p.required and not collected.get(p.name)]
    if missing_required:
        abort(400, f"Missing required parameter(s): {', '.join(p.name for p in missing_required)}")
    if getattr(report, "scope_to_projects", False):
        collected["_scope_project_ids"] = current_user_project_ids()
    result = report.run(**collected)
    if "error" in result.metadata:
        abort(404, result.metadata["error"])
    bio = excel.build(result)
    return send_file(
        bio,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=f"{result.filename_stem}.xlsx",
    )


def _chart_to_json(chart) -> Dict[str, Any]:
    return {
        "kind": chart.kind,
        "title": chart.title,
        "x_label": chart.x_label,
        "y_label": chart.y_label,
        "x_categories": chart.x_categories,
        "series": chart.series,
    }
