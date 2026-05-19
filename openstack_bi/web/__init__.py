"""Flask app factory for the web UI.

Wires the SQLite-backed config + session + CSRF + auth middleware. The
module-level `app` was previously created at import time, which meant
importing the package opened the config DB. It's now constructed lazily
so the CLI and tests can `import openstack_bi.web` without that side
effect.
"""

from __future__ import annotations

import logging
import sys
from itertools import groupby
from pathlib import Path
from typing import List, Tuple

from flask import Flask, redirect, request, url_for
from flask_wtf.csrf import CSRFProtect

from .. import config_db
from ..auth.session import (
    current_capabilities,
    current_user,
    filter_visible_reports,
    is_admin,
    is_local_admin,
)

log = logging.getLogger(__name__)

# Endpoints that should be reachable while logged out / before setup
# completes. Static files are always allowed.
_PUBLIC_ENDPOINTS = {
    "static",
    "login",
    "do_login",
    "logout",
}

# Reachable while setup is incomplete (in addition to public endpoints).
_SETUP_ENDPOINTS = {
    "setup",
    "setup_step",
}


csrf = CSRFProtect()


def create_app() -> Flask:
    """Build the Flask app. Side-effecting on the config DB."""
    root = Path(__file__).resolve().parent.parent.parent
    app = Flask(
        __name__,
        template_folder=str(root / "templates"),
        static_folder=str(root / "static"),
    )

    perm_warnings = config_db.check_file_perms()
    for w in perm_warnings:
        if w.startswith("ERROR:"):
            log.error(w)
            raise RuntimeError(w)
        log.warning(w)

    config_db.init()  # ensure migrations applied + secret_key seeded
    secret_key = config_db.web_setting("secret_key") or ""
    if not secret_key:
        raise RuntimeError("SECRET_KEY missing from web_settings; config DB init failed")
    app.config["SECRET_KEY"] = secret_key
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["WTF_CSRF_TIME_LIMIT"] = None  # tied to session lifetime

    csrf.init_app(app)

    # Existing GET-based report forms don't carry CSRF tokens; only
    # mutating routes (login/setup/admin/POSTs) need protection.
    csrf.exempt("openstack_bi.web.routes.run_report")
    csrf.exempt("openstack_bi.web.routes.export_report")

    @app.context_processor
    def _inject_globals():
        from openstack_bi.reports import all_reports

        all_list = sorted(all_reports(), key=lambda r: (r.category, r.name))
        visible = filter_visible_reports(all_list)
        categorized: List[Tuple[str, list]] = [
            (cat, list(items))
            for cat, items in groupby(visible, key=lambda r: r.category)
        ]
        info = current_user()
        caps = current_capabilities()
        return {
            "all_reports_list": visible,
            "all_reports_by_category": categorized,
            "current_user": info,
            "is_admin": is_admin(),
            "setup_status": config_db.setup_status(),
            "config_db_path": str(config_db.db_path()),
            "current_caps": caps,
            # The Admin menu / configuration surface is local-admin only;
            # Keystone sessions are privileged report users, not app admins.
            "has_admin_access": is_local_admin(),
        }

    @app.before_request
    def _gate():
        endpoint = request.endpoint or ""
        if endpoint in _PUBLIC_ENDPOINTS or endpoint.startswith("static"):
            return None
        # Setup wizard: while incomplete and no admin exists, anyone
        # reaching the app is steered to /setup. Once an admin exists,
        # the resume path becomes admin-only (handled inside the routes).
        status = config_db.setup_status()
        if status == config_db.SetupStatus.NO_ADMIN:
            if endpoint in _SETUP_ENDPOINTS:
                return None
            return redirect(url_for("setup"))
        if current_user() is None:
            return redirect(url_for("login", next=request.path))
        return None

    from . import (
        auth_routes, setup_routes, admin_routes, instance_routes,
        tools_routes, routes,
    )

    auth_routes.register(app)
    setup_routes.register(app)
    admin_routes.register(app)
    instance_routes.register(app)
    tools_routes.register(app)
    routes.register(app)
    return app


# Lazy proxy: `from openstack_bi.web import app` works for both
# `python web.py` and `waitress-serve web:app`, but we don't actually
# build the Flask instance until something touches it. Tests and CLI
# can `import openstack_bi.web` without DB I/O.
class _LazyApp:
    _instance: "Flask | None" = None

    def _get(self) -> Flask:
        if self._instance is None:
            self._instance = create_app()
        return self._instance

    def __getattr__(self, name):
        return getattr(self._get(), name)

    def __call__(self, *args, **kwargs):
        return self._get()(*args, **kwargs)


app = _LazyApp()
