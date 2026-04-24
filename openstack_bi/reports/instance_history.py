"""Full action history for one instance.

Parameter: instance UUID. The report fans out across every region and cell
to find the instance and returns its complete `instance_actions` log plus
aggregated event counts per action. This is a debugging drill-down — it's
the `openstack server event list` view without leaving the BI UI.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from openstack_bi import openstack
from openstack_bi.config import parse_regions
from openstack_bi.db import query
from openstack_bi.util import humanize

from .base import Param, Report, ReportResult


class InstanceHistoryReport(Report):
    id = "instance_history"
    name = "Instance history"
    description = (
        "Complete Nova `instance_actions` log for one instance UUID, across "
        "every region and cell. Each action row shows duration, user, "
        "request-id, message, event count, and the most recent event name."
    )
    params = [
        Param(name="instance_uuid", label="Instance UUID", kind="string",
              required=True, placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
              help="The Nova instance UUID to look up."),
    ]

    def run(self, instance_uuid: str, **_: Any) -> ReportResult:
        if not instance_uuid:
            return ReportResult(
                columns=[], rows=[],
                metadata={"error": "instance_uuid is required"},
                filename_stem="instance-history-none",
            )
        uuid = instance_uuid.strip()

        instance_row: Optional[Dict[str, Any]] = None
        action_rows: List[Dict[str, Any]] = []
        found_region: Optional[str] = None
        found_cell: Optional[str] = None

        for region in parse_regions():
            cells = openstack.list_cells(region)
            for cell in cells:
                rows = query(
                    region, cell,
                    """
                    SELECT uuid, display_name, project_id, host,
                           vm_state, power_state, task_state,
                           created_at, deleted_at, deleted
                    FROM instances
                    WHERE uuid = %s
                    LIMIT 1
                    """,
                    (uuid,),
                )
                if not rows:
                    continue
                instance_row = rows[0]
                found_region = region.name
                found_cell = cell
                action_rows = query(
                    region, cell,
                    """
                    SELECT ia.id, ia.action, ia.request_id, ia.user_id,
                           ia.project_id, ia.start_time, ia.finish_time,
                           ia.message,
                           (SELECT COUNT(*) FROM instance_actions_events iae
                            WHERE iae.action_id = ia.id AND iae.deleted = 0) AS event_count,
                           (SELECT iae.event FROM instance_actions_events iae
                            WHERE iae.action_id = ia.id AND iae.deleted = 0
                            ORDER BY iae.finish_time DESC, iae.start_time DESC
                            LIMIT 1) AS last_event,
                           (SELECT iae.result FROM instance_actions_events iae
                            WHERE iae.action_id = ia.id AND iae.deleted = 0
                            ORDER BY iae.finish_time DESC, iae.start_time DESC
                            LIMIT 1) AS last_event_result
                    FROM instance_actions ia
                    WHERE ia.instance_uuid = %s AND ia.deleted = 0
                    ORDER BY ia.start_time ASC
                    """,
                    (uuid,),
                )
                break
            if instance_row is not None:
                break

        if instance_row is None:
            return ReportResult(
                columns=[], rows=[],
                metadata={"error": f"Instance {uuid!r} not found in any region/cell."},
                filename_stem=f"instance-history-{uuid}",
            )

        from openstack_bi.config import keystone_db, keystone_region
        proj_name = None
        if instance_row.get("project_id"):
            pr = query(
                keystone_region(), keystone_db(),
                "SELECT name FROM project WHERE id = %s",
                (instance_row["project_id"],),
            )
            proj_name = pr[0]["name"] if pr else None

        rows_out: List[Dict[str, Any]] = []
        for r in action_rows:
            start = r.get("start_time")
            finish = r.get("finish_time")
            duration = None
            if start and finish:
                duration = (finish - start).total_seconds()
            rows_out.append({
                "action": r.get("action"),
                "start_time": start,
                "finish_time": finish,
                "duration": humanize(duration) if duration is not None else "-",
                "duration_seconds": duration,
                "user_id": r.get("user_id"),
                "request_id": r.get("request_id"),
                "message": r.get("message"),
                "event_count": r.get("event_count"),
                "last_event": r.get("last_event"),
                "last_event_result": r.get("last_event_result"),
            })

        metadata = {
            "instance_uuid": uuid,
            "instance_name": instance_row.get("display_name"),
            "project_id": instance_row.get("project_id"),
            "project_name": proj_name or "(unresolved)",
            "region": found_region,
            "cell_db": found_cell,
            "vm_state": instance_row.get("vm_state"),
            "power_state": instance_row.get("power_state"),
            "task_state": instance_row.get("task_state") or "(none)",
            "host": instance_row.get("host"),
            "created_at": instance_row.get("created_at"),
            "deleted_at": instance_row.get("deleted_at") or "(active)",
            "actions_recorded": len(rows_out),
        }

        return ReportResult(
            columns=[
                ("action", "Action"),
                ("start_time", "Start (UTC)"),
                ("finish_time", "Finish (UTC)"),
                ("duration", "Duration"),
                ("event_count", "Events"),
                ("last_event", "Last event"),
                ("last_event_result", "Result"),
                ("user_id", "User ID"),
                ("request_id", "Request ID"),
                ("message", "Message"),
            ],
            rows=rows_out,
            metadata=metadata,
            filename_stem=f"instance-history-{uuid}",
        )


REPORT = InstanceHistoryReport()
