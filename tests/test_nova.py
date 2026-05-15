"""Nova API client request shaping (mocked keystoneauth1 Session)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from openstack_bi import nova


def _resp(status_code, body):
    r = MagicMock()
    r.status_code = status_code
    r.json.return_value = body
    return r


def test_list_compute_hosts_filters_and_sorts():
    sess = MagicMock()
    sess.request.return_value = _resp(200, {"services": [
        {"binary": "nova-compute", "host": "cmp-2", "status": "enabled", "state": "up"},
        {"binary": "nova-compute", "host": "cmp-1", "status": "disabled", "state": "down"},
        {"binary": "nova-scheduler", "host": "ctl-1", "status": "enabled", "state": "up"},
    ]})

    hosts = nova.list_compute_hosts(sess, "dfw")
    assert [h["host"] for h in hosts] == ["cmp-1", "cmp-2"]

    url, method = sess.request.call_args[0]
    kwargs = sess.request.call_args[1]
    assert url == "/os-services?binary=nova-compute"
    assert method == "GET"
    assert kwargs["endpoint_filter"]["service_type"] == "compute"
    assert kwargs["endpoint_filter"]["region_name"] == "dfw"
    assert kwargs["headers"]["OpenStack-API-Version"] == "compute 2.25"


def test_live_migrate_posts_expected_body():
    sess = MagicMock()
    sess.request.return_value = _resp(202, None)

    nova.live_migrate(sess, "ord", "srv-1", "cmp-9")

    url, method = sess.request.call_args[0]
    kwargs = sess.request.call_args[1]
    assert url == "/servers/srv-1/action"
    assert method == "POST"
    assert kwargs["json"] == {
        "os-migrateLive": {"host": "cmp-9", "block_migration": "auto"}
    }


def test_remote_console_returns_url():
    sess = MagicMock()
    sess.request.return_value = _resp(200, {
        "remote_console": {
            "protocol": "vnc", "type": "novnc", "url": "https://vnc.example/abc",
        }
    })

    url = nova.remote_console(sess, "dfw", "srv-1")
    assert url == "https://vnc.example/abc"

    req_url, method = sess.request.call_args[0]
    kwargs = sess.request.call_args[1]
    assert req_url == "/servers/srv-1/remote-consoles"
    assert method == "POST"
    assert kwargs["json"] == {"remote_console": {"protocol": "vnc", "type": "novnc"}}


def test_nova_error_surfaces_api_message():
    sess = MagicMock()
    sess.request.return_value = _resp(403, {"forbidden": {"message": "not allowed"}})

    with pytest.raises(nova.NovaError) as excinfo:
        nova.get_server(sess, "dfw", "srv-1")
    assert "not allowed" in str(excinfo.value)


def test_nova_error_on_missing_endpoint():
    from keystoneauth1 import exceptions as ksa_exc

    sess = MagicMock()
    sess.request.side_effect = ksa_exc.EndpointNotFound()

    with pytest.raises(nova.NovaError):
        nova.list_compute_hosts(sess, "nowhere")
