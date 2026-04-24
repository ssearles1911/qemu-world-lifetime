"""Flask app factory for the web UI."""

from __future__ import annotations

from pathlib import Path

from flask import Flask


def create_app() -> Flask:
    root = Path(__file__).resolve().parent.parent.parent
    app = Flask(
        __name__,
        template_folder=str(root / "templates"),
        static_folder=str(root / "static"),
    )
    from . import routes  # noqa: WPS433
    routes.register(app)
    return app


# Convenience for `waitress-serve web:app` via the root `web.py` shim.
app = create_app()
