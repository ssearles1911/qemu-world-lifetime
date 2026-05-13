"""Login / logout routes."""

from __future__ import annotations

from urllib.parse import urlparse

from flask import Flask, flash, redirect, render_template, request, url_for

from .. import config_db
from ..auth import keystone as ks_auth
from ..auth import local as local_auth
from ..auth.session import login_keystone, login_local, logout as do_logout


def register(app: Flask) -> None:
    app.add_url_rule("/login", view_func=login, endpoint="login", methods=("GET", "POST"))
    app.add_url_rule("/logout", view_func=logout, endpoint="logout", methods=("GET", "POST"))


def _safe_next(target: str | None) -> str:
    """Only allow same-host redirect targets, else fall back to /catalog."""
    if not target:
        return url_for("catalog")
    parsed = urlparse(target)
    if parsed.scheme or parsed.netloc:
        return url_for("catalog")
    if not parsed.path.startswith("/"):
        return url_for("catalog")
    return target


def login():
    next_url = request.args.get("next") or request.form.get("next") or ""
    keystone_default_domain = config_db.web_setting("keystone_default_domain", "Default") or "Default"

    if request.method == "POST":
        backend = request.form.get("backend") or "keystone"
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        domain = (request.form.get("domain") or "").strip() or keystone_default_domain

        if backend == "local":
            user = local_auth.verify(username, password)
            if user:
                login_local(user)
                flash(f"Welcome, {user['username']}.", "success")
                return redirect(_safe_next(next_url))
            flash("Invalid administrator credentials.", "error")
        else:
            try:
                identity = ks_auth.authenticate(username, password, domain=domain)
            except ks_auth.KeystoneAuthError as exc:
                flash(str(exc), "error")
            else:
                login_keystone(identity)
                flash(f"Welcome, {identity.username}.", "success")
                return redirect(_safe_next(next_url))

    return render_template(
        "login.html",
        next_url=next_url,
        keystone_default_domain=keystone_default_domain,
        keystone_configured=bool(config_db.web_setting("keystone_auth_url")),
    )


def logout():
    do_logout()
    flash("Signed out.", "info")
    return redirect(url_for("login"))
