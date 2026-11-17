"""Read-only, snapshot-paginated CSV export for Google Apps Script (SQLite)."""
import base64
import hashlib
import hmac
import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from sqlalchemy import func
from sqlalchemy.orm import Session

from csv_export import generate_telemetry_csv
from db import TelemetryLog, get_db

VN_TIME = timezone(timedelta(hours=7))
router = APIRouter(prefix="/drive-backup", include_in_schema=False)


def require_backup_key(x_backup_token: Annotated[str | None, Header()] = None):
    expected = os.getenv("BACKUP_READ_TOKEN", "").strip()
    if len(expected) < 32:
        raise HTTPException(503, "Backup reader is not configured")
    if not x_backup_token or not hmac.compare_digest(x_backup_token.encode(), expected.encode()):
        raise HTTPException(401, "Invalid backup credentials")


def day_bounds(day: date):
    if day >= datetime.now(VN_TIME).date():
        raise HTTPException(422, "Only completed days in Asia/Ho_Chi_Minh can be exported")
    start = datetime.combine(day, time(), VN_TIME).astimezone(timezone.utc)
    end = start + timedelta(days=1)
    return start.isoformat(), end.isoformat()


def daily_query(db: Session, day: date):
    start, end = day_bounds(day)
    # SQLite normalizes ISO timestamps with Z, offsets and naive UTC consistently.
    recorded = func.julianday(TelemetryLog.recorded_at)
    return db.query(TelemetryLog).filter(recorded >= func.julianday(start), recorded < func.julianday(end))


@router.get("/manifest", dependencies=[Depends(require_backup_key)])
def manifest(response: Response, day: date, db: Session = Depends(get_db)):
    response.headers["Cache-Control"] = "no-store"
    snapshot = db.query(func.max(TelemetryLog.id)).scalar() or 0
    groups = daily_query(db, day).filter(TelemetryLog.id <= snapshot).with_entities(
        TelemetryLog.device_id, func.count(TelemetryLog.id)
    ).group_by(TelemetryLog.device_id).order_by(TelemetryLog.device_id).all()
    return {
        "day": day.isoformat(), "timezone": "Asia/Ho_Chi_Minh", "snapshot_id": snapshot,
        "files": [{"device_id": dev, "records": count, "filename": f"Tuoi_cay_{day.isoformat()}.csv"}
                  for dev, count in groups],
    }


@router.get("/csv", dependencies=[Depends(require_backup_key)])
def csv_page(
    day: date, device_id: Annotated[str, Query(min_length=1, max_length=64)],
    snapshot_id: Annotated[int, Query(ge=0)],
    after_id: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=2000)] = 2000,
    db: Session = Depends(get_db),
):
    records = daily_query(db, day).filter(
        TelemetryLog.device_id == device_id, TelemetryLog.id > after_id,
        TelemetryLog.id <= snapshot_id,
    ).order_by(TelemetryLog.id).limit(limit + 1).all()
    has_more = len(records) > limit
    records = records[:limit]
    # generate_telemetry_csv already includes one UTF-8 BOM.
    content = generate_telemetry_csv(records, device_id).encode("utf-8")
    return Response(content, media_type="text/csv; charset=utf-8", headers={
        "Cache-Control": "no-store",
        "X-Backup-Count": str(len(records)),
        "X-Backup-Next-Id": str(records[-1].id if has_more else 0),
        "X-Backup-SHA256": hashlib.sha256(content).hexdigest(),
        "Content-Disposition": f'attachment; filename="Tuoi_cay_{day.isoformat()}.csv"',
    })


@router.get("/changes", dependencies=[Depends(require_backup_key)])
def incremental_changes(
    response: Response,
    after_id: Annotated[int, Query(ge=0)] = 0,
    snapshot_id: Annotated[int | None, Query(ge=0)] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 500,
    db: Session = Depends(get_db),
):
    """Append-only telemetry stream, including today's and late-arriving records.

    CSV is base64 wrapped in JSON so HTTP text/BOM transformations cannot change
    the bytes whose checksum the client validates. No database writes occur.
    """
    response.headers["Cache-Control"] = "no-store"
    current = db.query(func.max(TelemetryLog.id)).scalar() or 0
    snapshot = current if snapshot_id is None else snapshot_id
    if after_id > current or snapshot > current or after_id > snapshot:
        raise HTTPException(409, "Backup cursor exceeds database snapshot; check database restore before resetting cursor")
    records = db.query(TelemetryLog).filter(
        TelemetryLog.id > after_id, TelemetryLog.id <= snapshot,
    ).order_by(TelemetryLog.id).limit(limit + 1).all()
    has_more = len(records) > limit
    records = records[:limit]
    grouped = {}
    for record in records:
        try:
            timestamp = datetime.fromisoformat(record.recorded_at.replace("Z", "+00:00"))
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            day = timestamp.astimezone(VN_TIME).date().isoformat()
        except (ValueError, AttributeError, OverflowError):
            raise HTTPException(422, f"Invalid recorded_at in telemetry ID {record.id}; cursor was not advanced")
        grouped.setdefault((record.device_id, day), []).append(record)
    groups = []
    for (device_id, day), rows in grouped.items():
        content = generate_telemetry_csv(rows, device_id).encode("utf-8")
        groups.append({
            "device_id": device_id, "day": day,
            "filename": f"Tuoi_cay_{day}.csv", "ids": [row.id for row in rows],
            "records": len(rows), "csv_base64": base64.b64encode(content).decode("ascii"),
            "sha256": hashlib.sha256(content).hexdigest(),
        })
    return {
        "version": 2, "timezone": "Asia/Ho_Chi_Minh", "snapshot_id": snapshot,
        "after_id": after_id, "next_id": records[-1].id if records else after_id,
        "has_more": has_more, "records": len(records), "groups": groups,
    }
