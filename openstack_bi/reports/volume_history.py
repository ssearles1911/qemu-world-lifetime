"""Full history for one Cinder volume.

Parameter: volume UUID. The report searches every region for the volume,
then returns its metadata (in the header) plus its attachment history
(one row per attachment, past or present). Recent user-facing messages
are summarised in the metadata.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from openstack_bi.config import (
    cinder_db,
    keystone_db,
    keystone_region,
    parse_regions,
)
from openstack_bi.db import query
from openstack_bi.util import humanize

from .base import Param, Report, ReportResult


class VolumeHistoryReport(Report):
    id = "volume_history"
    name = "Volume history"
    description = (
        "Cinder metadata + attachment timeline for one volume UUID. "
        "Each attachment row shows which instance the volume was bound to, "
        "when it attached, when (or if) it detached, and on which host."
    )
    params = [
        Param(name="volume_uuid", label="Volume UUID", kind="string",
              required=True, placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
              help="The Cinder volume UUID to look up."),
    ]

    def run(self, volume_uuid: str, **_: Any) -> ReportResult:
        if not volume_uuid:
            return ReportResult(
                columns=[], rows=[],
                metadata={"error": "volume_uuid is required"},
                filename_stem="volume-history-none",
            )
        uuid = volume_uuid.strip()

        volume_row: Optional[Dict[str, Any]] = None
        attach_rows: List[Dict[str, Any]] = []
        recent_messages: List[Dict[str, Any]] = []
        found_region: Optional[str] = None

        for region in parse_regions():
            try:
                rows = query(
                    region, cinder_db(),
                    """
                    SELECT id, project_id, user_id, status, attach_status,
                           size, display_name, created_at, updated_at,
                           deleted_at, deleted
                    FROM volumes
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (uuid,),
                )
            except Exception:  # noqa: BLE001
                continue
            if not rows:
                continue
            volume_row = rows[0]
            found_region = region.name
            attach_rows = query(
                region, cinder_db(),
                """
                SELECT id, instance_uuid, mountpoint, attach_time,
                       detach_time, attached_host, attach_mode,
                       created_at, deleted_at
                FROM volume_attachment
                WHERE volume_id = %s
                ORDER BY COALESCE(attach_time, created_at) ASC
                """,
                (uuid,),
            )
            try:
                recent_messages = query(
                    region, cinder_db(),
                    """
                    SELECT action_id, detail_id, message_level, created_at, expires_at
                    FROM messages
                    WHERE resource_uuid = %s
                    ORDER BY created_at DESC
                    LIMIT 10
                    """,
                    (uuid,),
                )
            except Exception:  # noqa: BLE001
                recent_messages = []
            break

        if volume_row is None:
            return ReportResult(
                columns=[], rows=[],
                metadata={"error": f"Volume {uuid!r} not found in any region."},
                filename_stem=f"volume-history-{uuid}",
            )

        proj_name = None
        if volume_row.get("project_id"):
            pr = query(
                keystone_region(), keystone_db(),
                "SELECT name FROM project WHERE id = %s",
                (volume_row["project_id"],),
            )
            proj_name = pr[0]["name"] if pr else None

        rows_out: List[Dict[str, Any]] = []
        for a in attach_rows:
            attach_time = a.get("attach_time")
            detach_time = a.get("detach_time")
            dur = None
            if attach_time:
                end = detach_time or (a.get("deleted_at") if a.get("deleted_at") else None)
                if end:
                    dur = (end - attach_time).total_seconds()
            rows_out.append({
                "instance_uuid": a.get("instance_uuid"),
                "mountpoint": a.get("mountpoint"),
                "attached_host": a.get("attached_host"),
                "attach_mode": a.get("attach_mode"),
                "attach_time": attach_time,
                "detach_time": detach_time or ("(current)" if not a.get("deleted_at") else a.get("deleted_at")),
                "duration": humanize(dur) if dur is not None else "-",
                "created_at": a.get("created_at"),
            })

        messages_summary = ", ".join(
            f"{m.get('action_id') or 'unknown'}/{m.get('detail_id') or '-'}"
            for m in recent_messages
        ) or "(none)"

        metadata = {
            "volume_uuid": uuid,
            "display_name": volume_row.get("display_name") or "(unnamed)",
            "project_id": volume_row.get("project_id"),
            "project_name": proj_name or "(unresolved)",
            "region": found_region,
            "status": volume_row.get("status"),
            "attach_status": volume_row.get("attach_status"),
            "size_gb": volume_row.get("size"),
            "created_at": volume_row.get("created_at"),
            "updated_at": volume_row.get("updated_at"),
            "deleted_at": volume_row.get("deleted_at") or "(active)",
            "attachments_recorded": len(rows_out),
            "recent_messages": messages_summary,
        }

        return ReportResult(
            columns=[
                ("attach_time", "Attached (UTC)"),
                ("detach_time", "Detached (UTC)"),
                ("duration", "Duration"),
                ("instance_uuid", "Instance UUID"),
                ("attached_host", "Host"),
                ("mountpoint", "Mount"),
                ("attach_mode", "Mode"),
            ],
            rows=rows_out,
            metadata=metadata,
            filename_stem=f"volume-history-{uuid}",
        )


REPORT = VolumeHistoryReport()
