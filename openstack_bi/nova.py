"""Minimal Nova (compute) API client.

The app is otherwise read-only against MariaDB replicas — this module is
the one place it calls an OpenStack service API, and only ever on behalf
of a logged-in Keystone user (see openstack_bi.auth.token_store). The
per-region compute endpoint is resolved from that user's token catalog,
so the app's region names must match the Keystone catalog region names.
"""

from __future__ import annotations

from typing import Any, Dict, List

from keystoneauth1 import exceptions as ksa_exc
from keystoneauth1.session import Session

# block_migration='auto' needs compute microversion >= 2.25; the
# POST /remote-consoles endpoint needs >= 2.6.
COMPUTE_MICROVERSION = "2.25"


class NovaError(Exception):
    """A Nova API call failed; the message is safe to show to the user."""


def _endpoint_filter(region: str) -> Dict[str, str]:
    return {
        "service_type": "compute",
        "interface": "public",
        "region_name": region,
    }


def _error_message(resp) -> str:
    """Pull a human-readable message out of a Nova error response."""
    try:
        body = resp.json()
    except Exception:  # noqa: BLE001
        body = None
    if isinstance(body, dict):
        for key in (
            "badRequest", "forbidden", "itemNotFound",
            "conflictingRequest", "computeFault", "error",
        ):
            section = body.get(key)
            if isinstance(section, dict) and section.get("message"):
                return f"Nova API: {section['message']}"
    return f"Nova API returned HTTP {resp.status_code}."


def _request(session: Session, region: str, method: str, path: str, **kwargs):
    """Issue one Nova request, raising NovaError on any failure."""
    headers = dict(kwargs.pop("headers", {}) or {})
    headers["OpenStack-API-Version"] = f"compute {COMPUTE_MICROVERSION}"
    try:
        resp = session.request(
            path, method,
            endpoint_filter=_endpoint_filter(region),
            headers=headers,
            raise_exc=False,
            **kwargs,
        )
    except ksa_exc.EndpointNotFound:
        raise NovaError(
            f"No compute endpoint for region {region!r} in the token's "
            "service catalog."
        )
    except ksa_exc.ClientException as exc:  # connection / SSL / etc.
        raise NovaError(f"Could not reach the compute API: {exc}")
    if resp.status_code >= 400:
        raise NovaError(_error_message(resp))
    return resp


def list_compute_hosts(session: Session, region: str) -> List[Dict[str, str]]:
    """Return nova-compute services in `region` as {host, status, state}."""
    resp = _request(session, region, "GET", "/os-services?binary=nova-compute")
    hosts: List[Dict[str, str]] = []
    for svc in (resp.json() or {}).get("services", []):
        if svc.get("binary") != "nova-compute":
            continue
        host = svc.get("host") or ""
        if not host:
            continue
        hosts.append({
            "host": host,
            "status": svc.get("status") or "",   # enabled / disabled
            "state": svc.get("state") or "",     # up / down
        })
    hosts.sort(key=lambda h: h["host"])
    return hosts


def get_server(session: Session, region: str, server_id: str) -> Dict[str, Any]:
    """Return basic detail for one server: id, name, status, host."""
    resp = _request(session, region, "GET", f"/servers/{server_id}")
    srv = (resp.json() or {}).get("server", {}) or {}
    return {
        "id": srv.get("id") or server_id,
        "name": srv.get("name") or "",
        "status": srv.get("status") or "",
        "host": srv.get("OS-EXT-SRV-ATTR:host") or "",
    }


def live_migrate(
    session: Session, region: str, server_id: str, target_host: str
) -> None:
    """Start a live migration of `server_id` to `target_host`.

    Nova returns 202 and migrates asynchronously; there is no completion
    signal here.
    """
    body = {"os-migrateLive": {"host": target_host, "block_migration": "auto"}}
    _request(session, region, "POST", f"/servers/{server_id}/action", json=body)


def remote_console(session: Session, region: str, server_id: str) -> str:
    """Create a noVNC console for `server_id` and return its URL."""
    body = {"remote_console": {"protocol": "vnc", "type": "novnc"}}
    resp = _request(
        session, region, "POST",
        f"/servers/{server_id}/remote-consoles", json=body,
    )
    console = (resp.json() or {}).get("remote_console", {}) or {}
    url = console.get("url")
    if not url:
        raise NovaError("Nova did not return a console URL.")
    return url
