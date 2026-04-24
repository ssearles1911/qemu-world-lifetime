"""Form parameter rendering helpers for the web UI.

Given a `Param` schema and the incoming `request.args`, produce the
kwargs that go into `Report.run()` and the `form_values` that the
template macro echoes back into the form.
"""

from __future__ import annotations

from typing import Any, Dict, List

from flask import Request

from openstack_bi.reports.base import Param


def collect(params: List[Param], request: Request) -> Dict[str, Any]:
    """Return {param_name: value} from the request.

    Value types match the param kind:
        int           → Optional[int]
        bool          → bool
        multiselect   → List[str]  (empty list = no selection / use default)
        everything else → Optional[str]
    """
    out: Dict[str, Any] = {}
    for p in params:
        if p.kind == "int":
            raw = request.args.get(p.name, "")
            try:
                out[p.name] = int(raw) if raw not in (None, "") else None
            except ValueError:
                out[p.name] = None
        elif p.kind == "bool":
            out[p.name] = request.args.get(p.name) not in (None, "")
        elif p.kind == "multiselect":
            out[p.name] = request.args.getlist(p.name)
        else:
            raw = request.args.get(p.name)
            out[p.name] = raw if raw else None
    return out


def form_values(params: List[Param], collected: Dict[str, Any]) -> Dict[str, Any]:
    """Values to echo back into the form. Applies defaults when the
    request didn't supply a value.
    """
    out: Dict[str, Any] = {}
    for p in params:
        v = collected.get(p.name)
        if p.kind == "multiselect":
            out[p.name] = list(v) if v else []
        elif p.kind == "bool":
            out[p.name] = bool(v)
        elif v in (None, ""):
            out[p.name] = p.default if p.default is not None else ""
        else:
            out[p.name] = v
    return out
