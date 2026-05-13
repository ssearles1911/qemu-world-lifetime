"""Keystone v3 password authentication via keystoneauth1.

Returns a `KeystoneIdentity` with the authenticated user's id, name,
domain, the set of project_ids they have effective roles on, and the
set of role names assigned to them.

Project resolution uses `GET /v3/role_assignments?user.id=...&effective=true`
so Keystone handles group memberships, domain-inherited roles, and project
hierarchies on the server side. We do not reimplement that resolution
in SQL.

Role-name resolution uses a separate `GET /v3/roles` lookup to build an
`{id: name}` cache, then maps `role.id` from each assignment through
that cache. This sidesteps inconsistencies in `?include_names=true` on
older Keystone versions, especially for group-derived assignments where
`role.name` is sometimes missing from the response.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Tuple

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


def authenticate(username: str, password: str, domain: Optional[str] = None) -> KeystoneIdentity:
    """Authenticate against Keystone and resolve effective project_ids.

    `domain` accepts either a domain id or name; falls back to the
    configured default. Raises `KeystoneAuthError` on any failure.
    """
    if not username or not password:
        raise KeystoneAuthError("username and password are required")

    auth_url = _auth_url()
    user_domain = (domain or _default_domain()).strip() or "Default"

    auth = v3_identity.Password(
        auth_url=auth_url,
        username=username,
        password=password,
        user_domain_name=user_domain,
        # Unscoped token: enough to call /v3/role_assignments and learn
        # what the user can reach without picking a project up front.
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

    project_ids, role_names = _effective_assignments(sess, user_id)

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
    )


def _v3_base_url() -> str:
    """Auth URL normalised to end with `/v3` exactly once."""
    base = _auth_url().rstrip("/")
    if not base.endswith("/v3"):
        base = f"{base}/v3"
    return base


def _fetch_role_id_to_name(sess: ks_session.Session) -> Dict[str, str]:
    """Build the `{role_id: role_name}` lookup table from `GET /v3/roles`.

    This is the authoritative role-name source; we don't trust
    `?include_names=true` on `/v3/role_assignments` because older
    Keystone releases sometimes omit `role.name` for group-derived
    assignments.
    """
    url = f"{_v3_base_url()}/roles"
    try:
        resp = sess.get(url, endpoint_filter=None)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not fetch /v3/roles: %s", exc)
        return {}
    if resp.status_code >= 400:
        log.warning(
            "/v3/roles returned %s: %s",
            resp.status_code, resp.text[:200],
        )
        return {}
    body = resp.json() or {}
    return {r["id"]: r.get("name", "") for r in body.get("roles", []) if r.get("id")}


def _extract_role_id(ra: dict) -> Optional[str]:
    """Pull a role id out of a role_assignments entry.

    The spec shape is `role: {id: ...}`; some Keystone versions inline
    `role_id` instead, especially in older microversions. We fall back
    accordingly.
    """
    role = ra.get("role") or {}
    if isinstance(role, dict) and role.get("id"):
        return role["id"]
    return ra.get("role_id")


def _effective_assignments(
    sess: ks_session.Session, user_id: str
) -> Tuple[Set[str], Set[str]]:
    """Walk `/v3/role_assignments?user.id=...&effective=true` once and
    return `(project_ids, role_names)`.

    `effective=true` expands group memberships, domain-inherited roles,
    and project hierarchies server-side. Role names are resolved via
    the role-id cache rather than `include_names`.
    """
    project_ids: Set[str] = set()
    role_names: Set[str] = set()

    url = f"{_v3_base_url()}/role_assignments?user.id={user_id}&effective"
    try:
        resp = sess.get(url, endpoint_filter=None)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not fetch role_assignments for %s: %s", user_id, exc)
        return project_ids, role_names

    if resp.status_code >= 400:
        log.warning(
            "role_assignments returned %s for %s: %s",
            resp.status_code, user_id, resp.text[:200],
        )
        return project_ids, role_names

    body = resp.json() or {}
    assignments = body.get("role_assignments", [])

    # Only fetch the role id->name table when we'll actually use it.
    role_id_to_name = _fetch_role_id_to_name(sess) if assignments else {}

    for ra in assignments:
        scope = ra.get("scope") or {}
        proj = scope.get("project") or {}
        if proj.get("id"):
            project_ids.add(proj["id"])
        role_id = _extract_role_id(ra)
        if role_id:
            name = role_id_to_name.get(role_id, "").strip().lower()
            if name:
                role_names.add(name)

    return project_ids, role_names
