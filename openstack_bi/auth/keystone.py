"""Keystone v3 password authentication via keystoneauth1.

Returns a `KeystoneIdentity`. Login is gated on the user holding the
configured admin role.

Role discovery deliberately does **not** use `GET /v3/role_assignments`:
that endpoint requires list privileges an unscoped token never has (an
unscoped token carries no roles, so every role-gated policy check on it
fails), so the call 403s for ordinary users. Instead we:

  1. authenticate unscoped — verifies the credentials, identifies the user;
  2. list the user's projects via the self-service `/v3/auth/projects`
     endpoint (an unscoped token *is* sufficient there); and
  3. project-scope a token into each — a scoped token carries the user's
     role names for that project directly (`AccessInfo.role_names`).

The first project on which the user holds the admin role yields the
scoped token kept for Nova actions (live migration, console).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, List, Optional, Set, Tuple

from keystoneauth1 import session as ks_session
from keystoneauth1.exceptions import (
    AuthorizationFailure,
    ConnectFailure,
    Unauthorized,
)
from keystoneauth1.identity import v3 as v3_identity

from .. import config_db

log = logging.getLogger(__name__)

# Defensive cap on the number of role names we'll attach to a session.
# Cookie payloads above ~4KB hit the Werkzeug warning, and most users
# don't have anywhere near this many roles. Truncation is logged and
# audited so an operator can tell the cap was exercised.
MAX_SESSION_ROLES = 50


class KeystoneAuthError(Exception):
    """Raised when authentication or scope discovery fails."""


@dataclass(frozen=True)
class KeystoneIdentity:
    user_id: str
    username: str
    domain_id: Optional[str]
    domain_name: Optional[str]
    project_ids: Set[str] = field(default_factory=set)
    role_names: Set[str] = field(default_factory=set)
    # Project-scoped token + service catalog, used to call Nova on the
    # user's behalf (live migration, console). None when no scoped token
    # could be obtained — Nova actions then degrade to a re-login prompt.
    scoped_access: Optional[Any] = None
    auth_url: Optional[str] = None


def _auth_url() -> str:
    url = (config_db.web_setting("keystone_auth_url") or "").strip()
    if not url:
        raise KeystoneAuthError(
            "Keystone auth URL is not configured. An admin must set it under "
            "Admin → Keystone."
        )
    return url


def _default_domain() -> str:
    return (config_db.web_setting("keystone_default_domain") or "Default").strip()


def _admin_role_name() -> str:
    """Keystone role required to sign in (web setting, default `admin`).

    Compared case-insensitively against the lowercased role names the
    app resolves, so it is normalised to lowercase here too.
    """
    raw = (config_db.web_setting("keystone_admin_role") or "").strip().lower()
    return raw or "admin"


def _v3_base_url() -> str:
    """Auth URL normalised to end with `/v3` exactly once."""
    base = _auth_url().rstrip("/")
    if not base.endswith("/v3"):
        base = f"{base}/v3"
    return base


def authenticate(username: str, password: str, domain: Optional[str] = None) -> KeystoneIdentity:
    """Authenticate against Keystone and resolve the user's roles/projects.

    `domain` accepts either a domain id or name; falls back to the
    configured default. Raises `KeystoneAuthError` on any failure,
    including when the user does not hold the admin role.
    """
    if not username or not password:
        raise KeystoneAuthError("username and password are required")

    auth_url = _auth_url()
    user_domain = (domain or _default_domain()).strip() or "Default"

    # 1. Unscoped auth — verifies the credentials and identifies the user,
    #    and gives us a token to enumerate the user's projects with.
    auth = v3_identity.Password(
        auth_url=auth_url,
        username=username,
        password=password,
        user_domain_name=user_domain,
    )
    sess = ks_session.Session(auth=auth)

    try:
        sess.get_token()
        access = auth.get_access(sess)
    except Unauthorized:
        config_db.record_audit("keystone", username, "login_failure", "Unauthorized")
        raise KeystoneAuthError("Invalid Keystone credentials.")
    except (AuthorizationFailure, ConnectFailure) as exc:
        config_db.record_audit(
            "keystone", username, "login_failure", f"{type(exc).__name__}: {exc}"
        )
        raise KeystoneAuthError(f"Could not reach Keystone: {exc}")

    user_obj = getattr(access, "user", {}) or {}
    domain_obj = user_obj.get("domain") or {}
    user_id = user_obj.get("id") or access.user_id
    if not user_id:
        raise KeystoneAuthError("Keystone returned no user id.")

    admin_role = _admin_role_name()

    # 2-3. Enumerate the user's projects and read their roles from
    #      project-scoped tokens. This also yields the scoped token used
    #      for Nova actions.
    project_ids, role_names, scoped_access = _resolve_scopes(
        sess, auth_url, username, password, user_domain, admin_role
    )

    # Login gate: only members of the admin role may sign in.
    if admin_role not in role_names:
        config_db.record_audit(
            "keystone", str(user_id), "login_denied_not_admin", username
        )
        raise KeystoneAuthError(
            f"Your account is not authorized — the {admin_role!r} role is "
            "required to sign in."
        )

    if len(role_names) > MAX_SESSION_ROLES:
        log.warning(
            "Truncating role list for %s from %d to %d.",
            user_id, len(role_names), MAX_SESSION_ROLES,
        )
        config_db.record_audit(
            "keystone", str(user_id), "session_roles_truncated",
            f"{len(role_names)} -> {MAX_SESSION_ROLES}",
        )
        role_names = set(sorted(role_names)[:MAX_SESSION_ROLES])

    config_db.record_audit("keystone", user_id, "login_success", username)

    return KeystoneIdentity(
        user_id=str(user_id),
        username=user_obj.get("name") or username,
        domain_id=domain_obj.get("id"),
        domain_name=domain_obj.get("name"),
        project_ids=project_ids,
        role_names=role_names,
        scoped_access=scoped_access,
        auth_url=auth_url,
    )


def _resolve_scopes(
    sess: ks_session.Session,
    auth_url: str,
    username: str,
    password: str,
    user_domain: str,
    admin_role: str,
) -> Tuple[Set[str], Set[str], Optional[Any]]:
    """Enumerate the user's projects and read roles from scoped tokens.

    Returns `(project_ids, role_names, scoped_access)`. `scoped_access`
    is the `AccessInfo` of a project on which the user holds `admin_role`
    (kept for Nova calls), or None.
    """
    projects = _list_projects(sess)
    project_ids: Set[str] = {p["id"] for p in projects if p.get("id")}
    role_names: Set[str] = set()
    scoped_access: Optional[Any] = None

    # Heuristic: a project named like the admin role (conventionally
    # `admin`) is the most likely place the admin role lives — try it
    # first so the common case stops after a single scoped request.
    ordered = sorted(
        projects,
        key=lambda p: (
            0 if (p.get("name") or "").strip().lower() == admin_role else 1,
            p.get("name") or "",
        ),
    )
    for proj in ordered:
        pid = proj.get("id")
        if not pid:
            continue
        acc = _scope_to_project(auth_url, username, password, user_domain, pid)
        if acc is None:
            continue
        proj_roles = {
            r.strip().lower() for r in (getattr(acc, "role_names", None) or []) if r
        }
        role_names |= proj_roles
        if admin_role in proj_roles:
            scoped_access = acc
            break  # admin confirmed + a Nova-capable token in hand

    return project_ids, role_names, scoped_access


def _list_projects(sess: ks_session.Session) -> List[dict]:
    """Projects the current token's user can scope to.

    `GET /v3/auth/projects` is a self-service endpoint — unlike
    `/v3/role_assignments`, an unscoped token is sufficient.
    """
    url = f"{_v3_base_url()}/auth/projects"
    try:
        resp = sess.get(url, endpoint_filter=None)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not list projects: %s", exc)
        return []
    if resp.status_code >= 400:
        log.warning(
            "/v3/auth/projects returned %s: %s",
            resp.status_code, resp.text[:200],
        )
        return []
    return (resp.json() or {}).get("projects", []) or []


def _scope_to_project(
    auth_url: str,
    username: str,
    password: str,
    user_domain: str,
    project_id: str,
) -> Optional[Any]:
    """Authenticate scoped to one project; return its `AccessInfo` or None.

    A project-scoped token carries the user's role names for that project
    (`AccessInfo.role_names`), so no separate role lookup is needed.
    """
    try:
        scoped_auth = v3_identity.Password(
            auth_url=auth_url,
            username=username,
            password=password,
            user_domain_name=user_domain,
            project_id=project_id,
        )
        scoped_sess = ks_session.Session(auth=scoped_auth)
        scoped_sess.get_token()
        return scoped_auth.get_access(scoped_sess)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not scope a token to project %s: %s", project_id, exc)
        return None
