"""Keystone v3 password authentication via keystoneauth1.

Returns a `KeystoneIdentity` with the authenticated user's id, name,
domain, and the set of project_ids they have effective roles on.

Project resolution uses `GET /v3/role_assignments?user.id=...&effective=true`
so Keystone handles group memberships, domain-inherited roles, and project
hierarchies on the server side. We do not reimplement that resolution
in SQL.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional, Set

from keystoneauth1 import session as ks_session
from keystoneauth1.exceptions import (
    AuthorizationFailure,
    ConnectFailure,
    Unauthorized,
)
from keystoneauth1.identity import v3 as v3_identity

from .. import config_db

log = logging.getLogger(__name__)


class KeystoneAuthError(Exception):
    """Raised when authentication or scope discovery fails."""


@dataclass(frozen=True)
class KeystoneIdentity:
    user_id: str
    username: str
    domain_id: Optional[str]
    domain_name: Optional[str]
    project_ids: Set[str] = field(default_factory=set)


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

    project_ids = _effective_project_ids(sess, user_id)

    config_db.record_audit("keystone", user_id, "login_success", username)

    return KeystoneIdentity(
        user_id=str(user_id),
        username=user_obj.get("name") or username,
        domain_id=domain_obj.get("id"),
        domain_name=domain_obj.get("name"),
        project_ids=project_ids,
    )


def _effective_project_ids(sess: ks_session.Session, user_id: str) -> Set[str]:
    """Walk /v3/role_assignments?user.id=...&effective=true and collect
    every project the user has any role on.

    `effective=true` expands group memberships and domain-inherited roles
    automatically. We don't filter by role name — any role is enough for
    "this user can see reports about this project".
    """
    base_url = _auth_url().rstrip("/")
    if not base_url.endswith("/v3"):
        # keystoneauth1 normalizes this for endpoints, but we want a
        # raw GET, so be explicit.
        if base_url.endswith("/"):
            base_url = base_url[:-1]
        if not base_url.endswith("/v3"):
            base_url = f"{base_url}/v3"

    out: Set[str] = set()
    url = f"{base_url}/role_assignments?user.id={user_id}&effective"
    try:
        resp = sess.get(url, endpoint_filter=None)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not fetch role_assignments for %s: %s", user_id, exc)
        return out

    if resp.status_code >= 400:
        log.warning(
            "role_assignments returned %s for %s: %s",
            resp.status_code, user_id, resp.text[:200],
        )
        return out

    body = resp.json() or {}
    for ra in body.get("role_assignments", []):
        scope = ra.get("scope") or {}
        proj = scope.get("project") or {}
        if proj.get("id"):
            out.add(proj["id"])
    return out
