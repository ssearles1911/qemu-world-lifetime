"""Flask session helpers + decorators.

The session payload looks like:

    {
        "kind": "local" | "keystone",
        "user_id": str,             # local user id (as str) or keystone user id
        "username": str,
        "is_admin": bool,           # True for local admins; False for keystone users
        "project_ids": list[str],   # keystone-only; admins have unscoped access
        "domain_id": str | None,    # keystone-only
    }
"""

from __future__ import annotations

from functools import wraps
from typing import Callable, Iterable, Optional, Set

from flask import flash, g, redirect, request, session, url_for

from .. import config_db
from .keystone import KeystoneIdentity

SESSION_KEY = "auth"


def login_local(user_row: dict) -> None:
    session.clear()
    session[SESSION_KEY] = {
        "kind": "local",
        "user_id": str(user_row["id"]),
        "username": user_row["username"],
        "is_admin": bool(user_row["is_admin"]),
        "project_ids": [],
        "domain_id": None,
    }
    session.permanent = True


def login_keystone(identity: KeystoneIdentity) -> None:
    session.clear()
    session[SESSION_KEY] = {
        "kind": "keystone",
        "user_id": identity.user_id,
        "username": identity.username,
        "is_admin": False,
        "project_ids": sorted(identity.project_ids),
        "domain_id": identity.domain_id,
    }
    session.permanent = True


def logout() -> None:
    info = session.get(SESSION_KEY) or {}
    if info:
        config_db.record_audit(
            info.get("kind", "?"),
            str(info.get("user_id") or info.get("username") or ""),
            "logout",
            "",
        )
    session.clear()


def current_user() -> Optional[dict]:
    """Return the session payload, or None if not logged in.

    Cached on the Flask `g` for the request lifecycle.
    """
    if hasattr(g, "_opsbi_user"):
        return g._opsbi_user  # type: ignore[attr-defined]
    info = session.get(SESSION_KEY)
    g._opsbi_user = info  # type: ignore[attr-defined]
    return info


def is_admin() -> bool:
    info = current_user()
    return bool(info and info.get("is_admin"))


def current_user_project_ids() -> Optional[Set[str]]:
    """Set of project_ids the current Keystone user has access to.

    Returns:
        None  — current user is unscoped (admin or not logged in).
        set() — Keystone user with no projects (deny-all).
        set(...) — explicit allow-list.
    """
    info = current_user()
    if not info or info.get("is_admin"):
        return None
    if info.get("kind") == "keystone":
        return set(info.get("project_ids") or [])
    return None


def login_required(view: Callable) -> Callable:
    @wraps(view)
    def wrapped(*args, **kwargs):
        if current_user() is None:
            flash("Please sign in to continue.", "info")
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)

    return wrapped


def admin_required(view: Callable) -> Callable:
    @wraps(view)
    def wrapped(*args, **kwargs):
        info = current_user()
        if info is None:
            flash("Please sign in to continue.", "info")
            return redirect(url_for("login", next=request.path))
        if not info.get("is_admin"):
            flash("Administrator access is required for that page.", "error")
            return redirect(url_for("catalog"))
        return view(*args, **kwargs)

    return wrapped


def report_visible_to_current_user(report) -> bool:
    """Decide whether a report should be enabled for the current user.

    Admins see everything. Keystone users see reports that opt in via
    `Report.scope_to_projects = True`.
    """
    if is_admin():
        return True
    return bool(getattr(report, "scope_to_projects", False))


def filter_visible_reports(reports: Iterable):
    return [r for r in reports if report_visible_to_current_user(r)]
