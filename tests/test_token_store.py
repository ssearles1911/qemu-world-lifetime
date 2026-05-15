"""In-memory scoped-token cache."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from openstack_bi.auth import token_store


def test_put_get_round_trip():
    access = object()
    key = token_store.put(access)
    assert isinstance(key, str) and key
    assert token_store.get(key) is access


def test_get_unknown_or_empty_key():
    assert token_store.get(None) is None
    assert token_store.get("") is None
    assert token_store.get("no-such-key") is None


def test_discard_removes_entry():
    key = token_store.put(object())
    token_store.discard(key)
    assert token_store.get(key) is None
    # Discarding an unknown / empty key is a no-op.
    token_store.discard("no-such-key")
    token_store.discard(None)


def test_expired_token_is_dropped():
    class Stale:
        expires = datetime.now(timezone.utc) - timedelta(minutes=5)

    key = token_store.put(Stale())
    assert token_store.get(key) is None


def test_unexpired_token_is_kept():
    class Fresh:
        expires = datetime.now(timezone.utc) + timedelta(hours=1)

    fresh = Fresh()
    key = token_store.put(fresh)
    assert token_store.get(key) is fresh
