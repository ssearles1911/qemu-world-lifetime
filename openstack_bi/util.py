"""Formatting + row-annotation helpers shared across reports."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, TypeVar


def humanize(seconds: Optional[float]) -> str:
    if seconds is None:
        return "-"
    s = int(seconds)
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    if d:
        return f"{d}d {h}h"
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def annotate_ages(rows: Iterable[Dict[str, Any]]) -> None:
    """Mutates each row in-place, adding `age_seconds` and `age` from
    `effective_time`. Also rewrites `last_action=None` as a readable marker.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for r in rows:
        eff = r.get("effective_time")
        if eff is None:
            r["age_seconds"] = None
            r["age"] = "-"
        else:
            r["age_seconds"] = (now - eff).total_seconds()
            r["age"] = humanize(r["age_seconds"])
        if r.get("last_action") is None:
            r["last_action"] = "(none recorded)"


def make_buckets(
    start: datetime, end: datetime, granularity: str,
) -> List[datetime]:
    """Bucket boundaries between `start` and `end`, inclusive on both ends.

    Granularity is "day", "week", or "month". The first boundary is the
    bucket containing `start`; subsequent boundaries step forward.
    """
    if start > end:
        start, end = end, start

    if granularity == "day":
        cur = start.replace(hour=0, minute=0, second=0, microsecond=0)
        step = timedelta(days=1)
        boundaries: List[datetime] = []
        while cur <= end:
            boundaries.append(cur)
            cur = cur + step
        return boundaries

    if granularity == "week":
        # Monday as week start
        cur = start - timedelta(days=start.weekday())
        cur = cur.replace(hour=0, minute=0, second=0, microsecond=0)
        boundaries = []
        while cur <= end:
            boundaries.append(cur)
            cur = cur + timedelta(days=7)
        return boundaries

    if granularity == "month":
        cur = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        boundaries = []
        while cur <= end:
            boundaries.append(cur)
            # step one month forward
            y, m = cur.year, cur.month
            if m == 12:
                cur = cur.replace(year=y + 1, month=1)
            else:
                cur = cur.replace(month=m + 1)
        return boundaries

    raise ValueError(f"unknown granularity: {granularity!r}")


def reconstruct_concurrent_counts(
    events: Sequence[Tuple[datetime, int]],
    boundaries: Sequence[datetime],
) -> List[int]:
    """Given a stream of `(timestamp, delta)` events (delta ∈ {+1, -1}),
    return the running concurrent count *as of each boundary* (end of
    bucket). Events at exactly `boundary` are included.

    Events and boundaries need not be pre-sorted; this function sorts them.
    """
    ev = sorted(events, key=lambda e: e[0])
    running = 0
    i = 0
    out: List[int] = []
    for b in boundaries:
        while i < len(ev) and ev[i][0] <= b:
            running += ev[i][1]
            i += 1
        out.append(running)
    return out


T = TypeVar("T")


def safe_for_each_region(
    regions: Sequence[Any],
    fn: Callable[[Any], T],
) -> Tuple[List[Tuple[Any, T]], List[Dict[str, str]]]:
    """Call `fn(region)` for each region, tolerating per-region failures.

    Returns `(results, errors)` where `results` is the list of successful
    `(region, return_value)` pairs and `errors` is a list of
    `{"region": name, "error": str(exc)}` dicts for the failures. Any
    exception raised by `fn` is captured — callers don't need to wrap.

    This is the standard fan-out pattern for multi-region reports: one
    dead replica shouldn't crash a whole report run.
    """
    results: List[Tuple[Any, T]] = []
    errors: List[Dict[str, str]] = []
    for region in regions:
        try:
            results.append((region, fn(region)))
        except Exception as exc:  # noqa: BLE001 — intentional catch-all
            errors.append({"region": region.name, "error": f"{type(exc).__name__}: {exc}"})
    return results, errors


def format_region_errors(errors: Sequence[Dict[str, str]]) -> str:
    """Render a region-error list as a short human-readable string.

    Used in ReportResult.metadata so the UI / Excel export / CLI can all
    show the same summary.
    """
    if not errors:
        return "(none)"
    return "; ".join(f"{e['region']}: {e['error']}" for e in errors)


def format_bucket_labels(boundaries: Sequence[datetime], granularity: str) -> List[str]:
    """Human labels for each bucket boundary, matched to the granularity."""
    if granularity == "day":
        return [b.strftime("%Y-%m-%d") for b in boundaries]
    if granularity == "week":
        return [b.strftime("%Y-W%V") for b in boundaries]
    if granularity == "month":
        return [b.strftime("%Y-%m") for b in boundaries]
    return [b.isoformat() for b in boundaries]
