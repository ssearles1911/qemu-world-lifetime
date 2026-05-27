"""Internal scheduler — runs the dashboard collector every N minutes.

Replaces cron. A daemon thread is started at web-app boot (from
`web.py`); it exits when the waitress process exits. Multiple
processes (unlikely in this single-host deploy) would each schedule
independently, but `INSERT OR REPLACE` on
`dashboard_metric_history` makes the writes idempotent.

Default cadence: every 15 minutes. Override via environment variables:

    OPSBI_COLLECTOR_INTERVAL_MINUTES = 1..1440   (default 15)
    OPSBI_DISABLE_SCHEDULER          = '1'       suppress the thread
                                                  (tests, external scheduler)

The collector also runs once immediately when the thread starts, so a
fresh deploy / restart has dashboard data right away rather than
waiting one full interval. Daily-granularity trend data is preserved
because the history table keys on `(snapshot_date, region, metric)`:
each 15-minute pass overwrites that day's row with the latest reading.

Tests and the CLI never import `web.py`, so they never start the
scheduler thread. Anyone consuming `openstack_bi.web.create_app`
directly gets the same isolation.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Optional

log = logging.getLogger(__name__)

_thread: Optional[threading.Thread] = None
_stop = threading.Event()


def _env_int(name: str, default: int, lo: int, hi: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        v = int(raw)
    except ValueError:
        log.warning("scheduler: invalid %s=%r; using default %d", name, raw, default)
        return default
    if not lo <= v <= hi:
        log.warning(
            "scheduler: %s=%d out of range [%d, %d]; using default %d",
            name, v, lo, hi, default,
        )
        return default
    return v


def _disabled() -> bool:
    return os.environ.get("OPSBI_DISABLE_SCHEDULER", "").strip() in {
        "1", "true", "yes", "y", "on",
    }


def _interval_minutes() -> int:
    """Cadence in minutes (default 15). Clamped to a 1-min..24-hour band."""
    return _env_int("OPSBI_COLLECTOR_INTERVAL_MINUTES", 15, 1, 1440)


def _run_collector() -> int:
    """One collector pass — exceptions are logged, never raised.

    Returns the number of rows written. The scheduler thread must not
    crash on a single failed run; it'll try again next interval (or
    sooner if the operator hits the ⟳ button on the dashboard).
    """
    from . import dashboard_metrics as dm
    try:
        rows = dm.collect_snapshot()
        dm.write_snapshot(rows)
        log.info("dashboard collector: wrote %d row(s)", len(rows))
        return len(rows)
    except Exception as exc:  # noqa: BLE001
        log.error("dashboard collector failed: %s", exc, exc_info=True)
        return 0


def _loop(interval_minutes: int) -> None:
    log.info(
        "dashboard scheduler started — collector every %d minute(s)",
        interval_minutes,
    )
    # First run happens immediately at boot so the dashboard isn't
    # blank for one full interval after a fresh deploy / restart.
    _run_collector()
    interval_s = float(interval_minutes) * 60.0
    while not _stop.is_set():
        if _stop.wait(timeout=interval_s):
            return
        _run_collector()


def start() -> bool:
    """Start the daemon scheduler thread (idempotent).

    Returns True if a new thread was started, False if disabled or
    already running.
    """
    global _thread
    if _disabled():
        log.info("scheduler disabled via OPSBI_DISABLE_SCHEDULER")
        return False
    if _thread is not None and _thread.is_alive():
        return False
    interval = _interval_minutes()
    _stop.clear()
    _thread = threading.Thread(
        target=_loop,
        args=(interval,),
        name="opsbi-scheduler",
        daemon=True,
    )
    _thread.start()
    return True


def stop(timeout: float = 5.0) -> None:
    """Signal the scheduler thread to exit and wait briefly for it.

    Mainly used in tests. Production lets the daemon thread die with
    the waitress process.
    """
    global _thread
    _stop.set()
    if _thread is not None:
        _thread.join(timeout=timeout)
        _thread = None
