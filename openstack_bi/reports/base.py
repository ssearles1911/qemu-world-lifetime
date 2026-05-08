"""Report plugin contract.

A `Report` declares its human-facing identity (`id`, `name`, `description`),
a set of `Param`s that the CLI + web renderers use to build their input
surfaces, and a `run(**kwargs)` method that produces a `ReportResult`.

The CLI, the web catalog, and the Excel exporter all consume `ReportResult`
uniformly — reports do not render themselves.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple


ParamKind = str  # one of: "string", "int", "select", "multiselect", "date", "bool"


@dataclass
class Param:
    name: str
    label: str
    kind: ParamKind
    required: bool = False
    default: Any = None
    # `choices` is either a static list of (value, label) pairs or a zero-arg
    # callable that returns one. Callables are resolved at render time so the
    # choices can reflect live DB state (e.g. the current domain list).
    choices: Optional[Callable[[], List[Tuple[str, str]]]] = None
    help: str = ""
    placeholder: str = ""
    # Tuning knobs whose defaults are usually fine. The web UI hides these
    # behind a collapsible "Advanced options" panel so the run button stays
    # in view on reports with many params.
    advanced: bool = False

    def resolve_choices(self) -> List[Tuple[str, str]]:
        if self.choices is None:
            return []
        return list(self.choices())


@dataclass
class ChartSpec:
    """Minimal chart description. Chart.js consumes it directly via its
    `data`/`options`-shaped JSON; the matplotlib exporter renders the same
    structure as a PNG for Excel embedding.
    """

    kind: str  # "bar" | "line" | "stacked_bar"
    title: str
    x_label: str
    y_label: str
    x_categories: List[str]
    # Each series: {"label": str, "data": list[float|int]}
    series: List[Dict[str, Any]]


@dataclass
class ReportResult:
    # columns are rendered in the order declared here. `key` must match a
    # key present in every row; `label` is the header text.
    columns: List[Tuple[str, str]]
    rows: List[Dict[str, Any]]
    # Column keys to group-by, in order. Empty = flat table.
    groupings: List[str] = field(default_factory=list)
    charts: List[ChartSpec] = field(default_factory=list)
    # Free-form metadata (domain, filters applied, regions queried, etc.).
    # Rendered verbatim in the Excel metadata block and the web meta line.
    metadata: Dict[str, Any] = field(default_factory=dict)
    # Suggested filename stem for export artifacts. No extension.
    filename_stem: str = "report"


class Report(ABC):
    id: str
    name: str
    description: str
    params: List[Param]
    # Used by the web catalog/navbar to group reports.
    category: str = "General"
    # Whether the report honors per-user project scoping. When True,
    # `run()` may receive `_scope_project_ids: Optional[Set[str]]` —
    # None for unscoped/admin callers, a set for Keystone users.
    # Reports that don't opt in are admin-only via the web catalog.
    scope_to_projects: bool = False

    @abstractmethod
    def run(self, **kwargs: Any) -> ReportResult:
        ...
