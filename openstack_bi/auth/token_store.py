"""In-memory cache of scoped Keystone tokens.

A Keystone login that passes the admin gate yields a project-scoped
token. We keep it server-side so per-user Nova actions (live migration,
console access) can be issued later without re-prompting for a password.
The Flask session cookie only carries an opaque cache key — the token
itself never leaves the process.

Caveats, by design:

* Process-local and in-memory: a restart clears every entry and users
  simply log in again. This matches the single-process waitress deploy.
* Keystone tokens expire (default ~1h). `get()` drops expired entries and
  returns None, so callers surface a "re-login" message rather than a
  stale-token error.
"""

from __future__ import annotations

import secrets
import threading
from datetime import datetime, timezone
from typing import Dict, Optional

from keystoneauth1 import session as ks_session
from keystoneauth1.identity import access as access_plugin

from .. import config_db

# Maps an opaque key -> a keystoneauth1 AccessInfo (the scoped token +
# its service catalog). Guarded by `_lock` since waitress serves requests
# from multiple threads in one process.
_lock = threading.Lock()
_store: Dict[str, object] = {}


def put(access: object) -> str:
    """Cache a scoped `AccessInfo` and return its opaque lookup key."""
    key = secrets.token_urlsafe(32)
    with _lock:
        _store[key] = access
    return key


def _expired(access: object) -> bool:
    expires = getattr(access, "expires", None)
    if not expires:
        return False
    try:
        return expires <= datetime.now(timezone.utc)
    except TypeError:
        # Naive datetime — compare without tzinfo.
        return expires <= datetime.now()


def get(key: Optional[str]) -> Optional[object]:
    """Return the cached `AccessInfo` for `key`, or None if absent/expired."""
    if not key:
        return None
    with _lock:
        access = _store.get(key)
    if access is None:
        return None
    if _expired(access):
        discard(key)
        return None
    return access


def discard(key: Optional[str]) -> None:
    """Drop a cached token (called on logout / on expiry)."""
    if not key:
        return
    with _lock:
        _store.pop(key, None)


def session_for(key: Optional[str]) -> Optional[ks_session.Session]:
    """Build a keystoneauth1 Session from the cached token.

    Returns None when the key is unknown or the token has expired — the
    caller should then prompt the user to sign in again.
    """
    access = get(key)
    if access is None:
        return None
    auth_url = (config_db.web_setting("keystone_auth_url") or "").strip() or None
    plugin = access_plugin.AccessInfoPlugin(auth_url=auth_url, auth_ref=access)
    return ks_session.Session(auth=plugin)
