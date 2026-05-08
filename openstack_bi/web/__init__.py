"""Flask app factory for the web UI."""

from __future__ import annotations

from itertools import groupby
from pathlib import Path
from typing import List, Tuple

from flask import Flask


def create_app() -> Flask:
    root = Path(__file__).resolve().parent.parent.parent
    app = Flask(
        __name__,
        template_folder=str(root / "templates"),
        static_folder=str(root / "static"),
    )

    @app.context_processor
    def _inject_globals():
        from openstack_bi.reports import all_reports

        reports = sorted(all_reports(), key=lambda r: (r.category, r.name))
        categorized: List[Tuple[str, list]] = [
            (cat, list(items))
            for cat, items in groupby(reports, key=lambda r: r.category)
        ]
        return {
            "all_reports_list": reports,
            "all_reports_by_category": categorized,
        }

    from . import routes  # noqa: WPS433
    routes.register(app)
    return app


# Convenience for `waitress-serve web:app` via the root `web.py` shim.
app = create_app()
