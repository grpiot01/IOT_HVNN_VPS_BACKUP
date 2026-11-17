"""
Router REST API & WebSocket cho Backend VPS chạy song song MQTT.
Tương thích chuẩn REST + WS của tue-minh/IoT_Backend và hệ thống tưới cà chua 3 hàng.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Response, WebSocket, status
from fastapi.responses import FileResponse
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, ConfigDict, field_validator
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from auth import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    VALID_ROLES,
    Token,
    authenticate_user,
    create_access_token,
    get_current_active_user,
    get_password_hash,
    normalize_role,
    require_admin,
    require_operator_or_admin,
)
from db import (
    Alert,
    Command,
    Device,
    EventLog,
    History,
    Schedule,
    SensorData,
    TelemetryLog,
    User,
    get_db,
)
from mqtt import mqtt_handler
from websocket_manager import manager

logger = logging.getLogger(__name__)
router = APIRouter()


@router.websocket("/ws/sensor_data")
async def websocket_endpoint(websocket: WebSocket):
    """Kênh WebSocket đẩy dữ liệu telemetry, event và trạng thái online/offline realtime."""
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except Exception:
        manager.disconnect(websocket)


# =================== SCHEMAS ===================


class UserCreate(BaseModel):
    username: Optional[str] = None
    email: Optional[str] = None
    name: Optional[str] = None
    password: str


class AdminUserCreate(BaseModel):
    username: Optional[str] = None
    email: Optional[str] = None
    name: Optional[str] = None
    password: str
    role: str = "user"


class AdminUserUpdate(BaseModel):
    role: Optional[str] = None
    name: Optional[str] = None
    password: Optional[str] = None


class UserSchema(BaseModel):
    id: str
    username: str
    name: str
    role: str
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)

    @field_validator("role", mode="before")
    @classmethod
    def _norm_role(cls, v: Any) -> str:
        return normalize_role(str(v) if v is not None else None)


class DeviceCreate(BaseModel):
    id: Optional[str] = None
    name: str
    type: Optional[str] = "tomato_irrigation_3row"
    status: Optional[str] = "offline"


class DeviceNameUpdate(BaseModel):
    name: str


class DeviceSchema(BaseModel):
    id: str
    user_id: Optional[str] = None
    name: str
    type: Optional[str] = None
    status: Optional[str] = None
    last_seq: Optional[int] = 0
    last_seen_at: Optional[datetime] = None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class CommandRequest(BaseModel):
    device_id: str
    action: str
    row: Optional[int] = None
    mode: Optional[str] = None
    params: Optional[Dict[str, Any]] = None


class ConfigPublishRequest(BaseModel):
    device_id: str
    config: Dict[str, Any]

    @field_validator("config")
    @classmethod
    def validate_row_a_schedule(cls, config):
        if "scheduleHour" not in config and "scheduleMinute" not in config:
            return config
        hours, minutes = config.get("scheduleHour"), config.get("scheduleMinute")
        if not isinstance(hours, list) or not isinstance(minutes, list) or not 1 <= len(hours) <= 24 or len(hours) != len(minutes):
            raise ValueError("Hàng A cần 1–24 lịch tưới với số giờ và phút bằng nhau")
        if any(type(h) is not int or not 0 <= h <= 23 for h in hours) or any(type(m) is not int or not 0 <= m <= 59 for m in minutes):
            raise ValueError("Giờ tưới phải là số nguyên 0–23, phút 0–59")
        if len(set(zip(hours, minutes))) != len(hours):
            raise ValueError("Các giờ tưới Hàng A không được trùng nhau")
        return config


# =================== AUTH & USERS ===================


def _validate_login_id(raw_username: Optional[str], raw_email: Optional[str]) -> str:
    login_id = (raw_username or raw_email or "").strip()
    if len(login_id) < 3:
        raise HTTPException(status_code=400, detail="Tên đăng nhập phải tối thiểu 3 ký tự")
    if " " in login_id:
        raise HTTPException(status_code=400, detail="Tên đăng nhập không được chứa khoảng trắng")
    return login_id


@router.post("/register", response_model=UserSchema, status_code=status.HTTP_201_CREATED)
def register(user: UserCreate, db: Session = Depends(get_db)):
    login_id = _validate_login_id(user.username, user.email)
    name = (user.name or "").strip() or login_id
    if len(user.password) < 4:
        raise HTTPException(status_code=400, detail="Mật khẩu phải tối thiểu 4 ký tự")
    if db.query(User).filter(func.lower(User.email) == login_id.lower()).first():
        raise HTTPException(status_code=400, detail="Tên đăng nhập này đã được sử dụng")

    # Tự đăng ký mặc định nhận bậc guest (chỉ xem); Admin có thể nâng lên user/admin
    db_user = User(
        email=login_id,
        name=name,
        role="guest",
        password_hash=get_password_hash(user.password),
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)

    db.add(
        History(
            performer_id=db_user.id,
            event_type="USER_REGISTER",
            description=f"Đăng ký tài khoản {db_user.email} ({db_user.name}) [role=guest]",
            related_id=db_user.id,
        )
    )
    db.commit()
    return db_user


@router.post("/login", response_model=Token)
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = authenticate_user(db, form_data.username.strip(), form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Tên đăng nhập hoặc mật khẩu không đúng",
            headers={"WWW-Authenticate": "Bearer"},
        )
    role_norm = normalize_role(user.role)
    token = create_access_token(
        data={"sub": user.id, "email": user.email, "role": role_norm},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    )
    # Ghi nhận lịch sử đăng nhập vào bảng History
    db.add(
        History(
            performer_id=user.id,
            event_type="USER_LOGIN",
            description=f"Tài khoản {user.email} ({user.name}) đăng nhập hệ thống [vai trò: {role_norm}]",
            related_id=user.id,
        )
    )
    db.commit()
    return {"access_token": token, "token_type": "bearer", "username": user.name, "role": role_norm}


@router.get("/users/me", response_model=UserSchema)
def read_users_me(current_user: Annotated[User, Depends(get_current_active_user)]):
    return current_user


@router.get("/users", response_model=List[UserSchema])
def list_users(
    _admin: Annotated[User, Depends(require_admin)],
    db: Session = Depends(get_db),
):
    return db.query(User).order_by(User.created_at.asc()).all()


@router.post("/users", response_model=UserSchema, status_code=status.HTTP_201_CREATED)
def admin_create_user(
    req: AdminUserCreate,
    current_admin: Annotated[User, Depends(require_admin)],
    db: Session = Depends(get_db),
):
    login_id = _validate_login_id(req.username, req.email)
    name = (req.name or "").strip() or login_id
    role_val = req.role.strip().lower()
    if role_val not in VALID_ROLES:
        raise HTTPException(status_code=400, detail="Bậc tài khoản chỉ hỗ trợ: admin, user, guest")
    if len(req.password) < 4:
        raise HTTPException(status_code=400, detail="Mật khẩu phải tối thiểu 4 ký tự")
    if db.query(User).filter(func.lower(User.email) == login_id.lower()).first():
        raise HTTPException(status_code=400, detail="Tên đăng nhập này đã được sử dụng")

    new_user = User(
        email=login_id,
        name=name,
        role=role_val,
        password_hash=get_password_hash(req.password),
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    db.add(
        History(
            performer_id=current_admin.id,
            event_type="USER_CREATE",
            description=f"{current_admin.name} tạo tài khoản {new_user.email} (bậc={role_val})",
            related_id=new_user.id,
        )
    )
    db.commit()
    return new_user


@router.patch("/users/{user_id}", response_model=UserSchema)
def admin_update_user(
    user_id: str,
    req: AdminUserUpdate,
    current_admin: Annotated[User, Depends(require_admin)],
    db: Session = Depends(get_db),
):
    target = db.get(User, user_id)
    if not target:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài khoản")

    if req.role is not None:
        role_val = req.role.strip().lower()
        if role_val not in VALID_ROLES:
            raise HTTPException(status_code=400, detail="Bậc tài khoản chỉ hỗ trợ: admin, user, guest")
        if normalize_role(target.role) == "admin" and role_val != "admin":
            admin_count = sum(1 for u in db.query(User).all() if normalize_role(u.role) == "admin")
            if admin_count <= 1:
                raise HTTPException(status_code=400, detail="Không thể hạ quyền tài khoản Admin duy nhất trong hệ thống")
        target.role = role_val

    if req.name is not None:
        clean_name = req.name.strip()
        if not clean_name:
            raise HTTPException(status_code=400, detail="Tên hiển thị không được để trống")
        target.name = clean_name

    if req.password is not None and req.password != "":
        if len(req.password) < 4:
            raise HTTPException(status_code=400, detail="Mật khẩu mới phải tối thiểu 4 ký tự")
        target.password_hash = get_password_hash(req.password)

    db.add(
        History(
            performer_id=current_admin.id,
            event_type="USER_UPDATE",
            description=f"{current_admin.name} cập nhật tài khoản {target.email} (bậc={target.role})",
            related_id=target.id,
        )
    )
    db.commit()
    db.refresh(target)
    return target


@router.delete("/users/{user_id}", status_code=status.HTTP_200_OK)
def admin_delete_user(
    user_id: str,
    current_admin: Annotated[User, Depends(require_admin)],
    db: Session = Depends(get_db),
):
    target = db.get(User, user_id)
    if not target:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài khoản")
    if target.id == current_admin.id:
        raise HTTPException(status_code=400, detail="Không thể tự xóa tài khoản Admin đang đăng nhập")
    if normalize_role(target.role) == "admin":
        admin_count = sum(1 for u in db.query(User).all() if normalize_role(u.role) == "admin")
        if admin_count <= 1:
            raise HTTPException(status_code=400, detail="Không thể xóa tài khoản Admin duy nhất")

    deleted_email = target.email
    db.delete(target)
    db.add(
        History(
            performer_id=current_admin.id,
            event_type="USER_DELETE",
            description=f"{current_admin.name} đã xóa tài khoản {deleted_email}",
            related_id=user_id,
        )
    )
    db.commit()
    return {"deleted": True, "id": user_id}


# =================== DEVICES & TELEMETRY ===================


@router.get("/devices", response_model=List[DeviceSchema])
def get_devices(db: Session = Depends(get_db)):
    return db.scalars(select(Device)).all()


@router.patch("/devices/{device_id}", response_model=DeviceSchema)
def update_device_name(
    device_id: str,
    payload: DeviceNameUpdate,
    _user: Annotated[User, Depends(require_operator_or_admin)],
    db: Session = Depends(get_db),
):
    name = payload.name.strip()
    if not name or len(name) > 255:
        raise HTTPException(status_code=422, detail="Tên thiết bị phải có từ 1 đến 255 ký tự.")
    device = db.get(Device, device_id)
    if device is None:
        raise HTTPException(status_code=404, detail="Không tìm thấy thiết bị.")
    device.name = name
    db.add(
        History(
            device_id=device.id,
            performer_id=_user.id,
            event_type="DEVICE_RENAME",
            description=f"{_user.name} đổi tên thiết bị '{device_id}' thành '{name}'",
        )
    )
    db.commit()
    db.refresh(device)
    return device


@router.delete("/devices/{device_id}")
def delete_device(
    device_id: str,
    _user: Annotated[User, Depends(require_operator_or_admin)],
    db: Session = Depends(get_db),
):
    device = db.get(Device, device_id)
    if device is None:
        raise HTTPException(status_code=404, detail="Không tìm thấy thiết bị.")

    dev_name = device.name
    db.query(Alert).filter(Alert.device_id == device_id).delete(synchronize_session=False)
    db.query(EventLog).filter(EventLog.device_id == device_id).delete(synchronize_session=False)
    db.query(TelemetryLog).filter(TelemetryLog.device_id == device_id).delete(synchronize_session=False)
    db.query(SensorData).filter(SensorData.device_id == device_id).delete(synchronize_session=False)
    db.query(Command).filter(Command.device_id == device_id).delete(synchronize_session=False)
    db.query(Schedule).filter(Schedule.device_id == device_id).delete(synchronize_session=False)

    db.add(
        History(
            device_id=None,
            performer_id=_user.id,
            event_type="DEVICE_DELETE",
            description=f"{_user.name} đã xóa thiết bị '{dev_name}' (ID: {device_id})",
        )
    )
    db.delete(device)
    db.commit()
    return {"message": f"Đã xóa thiết bị {device_id} thành công", "deleted_id": device_id}


@router.post("/devices", response_model=DeviceSchema, status_code=status.HTTP_201_CREATED)
def create_device(
    device: DeviceCreate,
    _admin: Annotated[User, Depends(require_admin)],
    db: Session = Depends(get_db),
):
    dev = Device(
        id=device.id or device.name.lower().replace(" ", "_"),
        name=device.name,
        type=device.type,
        status=device.status,
    )
    db.add(dev)
    db.commit()
    db.refresh(dev)
    return dev


@router.get("/devices/{device_id}/latest")
def get_device_latest(device_id: str, db: Session = Depends(get_db)):
    dev = db.get(Device, device_id)
    row = (
        db.query(TelemetryLog)
        .filter(TelemetryLog.device_id == device_id)
        .order_by(desc(TelemetryLog.id))
        .first()
    )
    return {
        "device_id": device_id,
        "device_status": dev.status if dev else "unknown",
        "mqtt_connected": mqtt_handler.connected,
        "telemetry": {
            "id": row.id,
            "device_id": row.device_id,
            "seq": row.seq,
            "recorded_at": row.recorded_at,
            "received_at": row.received_at.isoformat() if row.received_at else None,
            "soil_json": row.soil_json,
            "temperature_c": row.temperature_c,
            "rh_pct": row.rh_pct,
            "vpd_kpa": row.vpd_kpa,
            "rows_json": row.rows_json,
            "tank_ok": row.tank_ok,
            "pump_on": row.pump_on,
            "control_mode": row.control_mode or "manual",
            "rssi_dbm": row.rssi_dbm,
            "rtc_time": row.rtc_time,
            "cf_synced": row.cf_synced,
        }
        if row
        else None,
    }


@router.get("/devices/{device_id}/history")
def get_device_history(
    device_id: str,
    limit: int = 120,
    hours: Optional[float] = None,
    db: Session = Depends(get_db),
):
    q = db.query(TelemetryLog).filter(TelemetryLog.device_id == device_id)
    target_limit = min(max(limit, 10), 500)

    if hours and hours > 0:
        cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=hours)
        q = q.filter(TelemetryLog.received_at >= cutoff_dt)
        total_in_window = q.count()
        if total_in_window <= target_limit:
            rows = q.order_by(TelemetryLog.id.asc()).all()
        else:
            step = max(1, total_in_window // target_limit)
            all_ids = [r[0] for r in q.with_entities(TelemetryLog.id).order_by(TelemetryLog.id.asc()).all()]
            sampled_ids = set(all_ids[::step])
            if all_ids:
                sampled_ids.add(all_ids[-1])
            rows = (
                db.query(TelemetryLog)
                .filter(TelemetryLog.id.in_(sampled_ids))
                .order_by(TelemetryLog.id.asc())
                .all()
            )
    else:
        rows = (
            q.order_by(desc(TelemetryLog.id))
            .limit(target_limit)
            .all()
        )
        rows = list(reversed(rows))

    samples = [
        {
            "id": r.id,
            "device_id": r.device_id,
            "seq": r.seq,
            "recorded_at": r.recorded_at,
            "received_at": r.received_at.isoformat() if r.received_at else None,
            "soil_json": r.soil_json,
            "temperature_c": r.temperature_c,
            "rh_pct": r.rh_pct,
            "vpd_kpa": r.vpd_kpa,
            "rows_json": r.rows_json,
            "tank_ok": r.tank_ok,
            "pump_on": r.pump_on,
            "control_mode": r.control_mode or "manual",
            "rssi_dbm": r.rssi_dbm,
            "rtc_time": r.rtc_time,
        }
        for r in rows
    ]
    return {"device_id": device_id, "samples": samples}


@router.get("/devices/{device_id}/events")
def get_device_events(device_id: str, limit: int = 50, db: Session = Depends(get_db)):
    evs = (
        db.query(EventLog)
        .filter(EventLog.device_id == device_id)
        .order_by(desc(EventLog.id))
        .limit(min(limit, 200))
        .all()
    )

    cmd_records = (
        db.query(Command, User.name, User.email)
        .outerjoin(User, Command.performer_id == User.id)
        .filter(Command.device_id == device_id, Command.performer_id.isnot(None))
        .order_by(desc(Command.created_at))
        .limit(300)
        .all()
    )
    parsed_cmds = []
    for c, u_name, u_email in cmd_records:
        r_id = None
        if c.params_json:
            try:
                pj = json.loads(c.params_json)
                r_id = pj.get("row")
            except Exception:
                pass
        parsed_cmds.append(
            {
                "action": (c.action or "").strip().lower(),
                "row": int(r_id) if isinstance(r_id, (int, str)) and str(r_id).isdigit() else None,
                "created_at": c.created_at,
                "performer_name": u_name or u_email,
            }
        )

    action_map = {
        "manual_start": "start",
        "manual_stop": "stop",
        "pump_on": "pump_on",
        "pump_off": "pump_off",
        "estop": "estop",
        "reset_water": "reset_water",
    }

    def _to_naive_utc(val):
        if val is None:
            return None
        if isinstance(val, str):
            try:
                dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                if dt.tzinfo is not None:
                    return dt.astimezone(timezone.utc).replace(tzinfo=None)
                return dt
            except Exception:
                return None
        if isinstance(val, datetime):
            if val.tzinfo is not None:
                return val.astimezone(timezone.utc).replace(tzinfo=None)
            return val
        return None

    items = []
    for e in evs:
        reason = None
        performer_name = None
        if e.detail_json:
            try:
                parsed = json.loads(e.detail_json)
                if isinstance(parsed, dict):
                    reason = parsed.get("reason")
                    performer_name = parsed.get("performer_name")
                else:
                    reason = str(parsed)
            except Exception:
                reason = e.detail_json

        if not performer_name and e.type in action_map:
            target_act = action_map[e.type]
            e_time = _to_naive_utc(e.received_at) or _to_naive_utc(e.recorded_at)
            best_cmd = None
            best_diff = 999999999
            for pc in parsed_cmds:
                if pc["action"] == target_act:
                    if e.row and pc["row"] and e.row != pc["row"]:
                        continue
                    c_time = _to_naive_utc(pc["created_at"])
                    if e_time and c_time:
                        diff = abs((e_time - c_time).total_seconds())
                        if diff < best_diff and diff <= 7200:
                            best_diff = diff
                            best_cmd = pc
                    elif not best_cmd:
                        best_cmd = pc
            if best_cmd:
                performer_name = best_cmd["performer_name"]

        items.append(
            {
                "id": e.id,
                "recorded_at": e.recorded_at,
                "row": e.row,
                "type": e.type,
                "liters": e.liters,
                "reason": reason,
                "performer_name": performer_name,
            }
        )
    return {"device_id": device_id, "events": items}


@router.get("/sensor_data")
def get_sensor_data(device_id: Optional[str] = None, db: Session = Depends(get_db)):
    q = db.query(SensorData)
    if device_id:
        q = q.filter(SensorData.device_id == device_id)
    return q.all()


# =================== MQTT COMMANDS & CONFIG ===================


@router.get("/commands")
def get_commands(device_id: Optional[str] = None, limit: int = 50, db: Session = Depends(get_db)):
    q = db.query(Command)
    if device_id:
        q = q.filter(Command.device_id == device_id)
    cmds = q.order_by(desc(Command.created_at)).limit(min(limit, 200)).all()
    # Lấy thông tin tên tài khoản từ performer_id
    user_ids = {c.performer_id for c in cmds if c.performer_id}
    users_map = {}
    if user_ids:
        found_users = db.query(User).filter(User.id.in_(user_ids)).all()
        users_map = {u.id: (u.name or u.email) for u in found_users}

    out = []
    for c in cmds:
        row_val = None
        if c.params_json:
            try:
                parsed = json.loads(c.params_json)
                if isinstance(parsed, dict):
                    row_val = parsed.get("row")
            except Exception:
                pass
        performer_name = users_map.get(c.performer_id) if c.performer_id else None
        out.append(
            {
                "id": c.id,
                "device_id": c.device_id,
                "performer_id": c.performer_id,
                "performer_name": performer_name,
                "action": c.action,
                "row": row_val,
                "params_json": c.params_json,
                "status": c.status,
                "created_at": c.created_at.isoformat() if c.created_at else None,
                "executed_at": c.executed_at.isoformat() if c.executed_at else None,
            }
        )
    return out


@router.post("/commands", status_code=status.HTTP_201_CREATED)
def create_command(
    req: CommandRequest,
    current_user: Annotated[User, Depends(require_operator_or_admin)],
    db: Session = Depends(get_db),
):
    action_norm = req.action.strip().lower()
    latest = (
        db.query(TelemetryLog)
        .filter(TelemetryLog.device_id == req.device_id)
        .order_by(desc(TelemetryLog.id))
        .first()
    )
    if action_norm in ("start", "pump_on") and latest and latest.tank_ok == 0:
        raise HTTPException(status_code=400, detail="Cảnh báo an toàn: Bồn nước đang cạn — không thể mở van hoặc bật bơm!")
    if action_norm == "pump_on" and latest:
        try:
            rows = json.loads(latest.rows_json or "[]")
        except Exception:
            rows = []
        if not any(isinstance(r, dict) and r.get("valve") for r in rows):
            raise HTTPException(status_code=400, detail="Cả 3 van đang đóng — hãy mở ít nhất 1 van trước khi bật bơm!")

    if action_norm == "reset_water":
        current_raw_map = {}
        if latest and latest.rows_json:
            try:
                curr_rows = json.loads(latest.rows_json)
                for r in curr_rows:
                    if isinstance(r, dict):
                        rid = r.get("id")
                        raw_val = r.get("raw_daily_l")
                        if raw_val is None:
                            old_off = mqtt_handler.get_water_offset(req.device_id, rid)
                            raw_val = float(r.get("daily_l") or 0.0) + old_off
                        current_raw_map[rid] = float(raw_val)
            except Exception:
                pass

        mqtt_handler.set_water_offset(req.device_id, req.row, current_raw_map)

        if latest and latest.rows_json:
            try:
                curr_rows = json.loads(latest.rows_json)
                for r in curr_rows:
                    if isinstance(r, dict):
                        if req.row is None or req.row == 0 or r.get("id") == req.row:
                            r["daily_l"] = 0.0
                latest.rows_json = json.dumps(curr_rows)

                if mqtt_handler.loop:
                    from bridge import data_queue
                    mqtt_handler.loop.call_soon_threadsafe(
                        data_queue.put_nowait,
                        {
                            "kind": "telemetry",
                            "device_id": req.device_id,
                            "data": {
                                "rows": curr_rows,
                                "tank_ok": latest.tank_ok,
                                "pump_on": latest.pump_on,
                                "control_mode": latest.control_mode,
                                "recorded_at": latest.recorded_at,
                            },
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        },
                    )
            except Exception as ex:
                logger.warning("Lỗi cập nhật latest_telemetry sau reset_water: %s", ex)

        target_name = f"Hàng {['A', 'B', 'C'][req.row - 1]}" if (req.row and 1 <= req.row <= 3) else "Tất cả 3 hàng (A, B, C)"
        db.add(
            EventLog(
                device_id=req.device_id,
                row=req.row,
                type="reset_water",
                liters=0.0,
                recorded_at=datetime.now(timezone.utc).isoformat(),
                received_at=datetime.now(timezone.utc),
                detail_json=json.dumps({
                    "reason": f"Đặt lại lượng nước ({target_name}) về 0.00 L",
                    "performer_name": current_user.name,
                }, ensure_ascii=False),
            )
        )

    payload: Dict[str, Any] = {"action": req.action}
    if req.row is not None:
        payload["row"] = req.row
    if req.mode is not None:
        payload["mode"] = req.mode
    if action_norm == "sync_rtc":
        now_dt = datetime.now(timezone.utc)
        payload["epoch"] = int(now_dt.timestamp())
        payload["iso"] = now_dt.astimezone().isoformat(timespec="seconds")
    if req.params:
        payload.update(req.params)

    sent_mqtt = mqtt_handler.publish_command(req.device_id, payload)
    db_cmd = Command(
        device_id=req.device_id,
        performer_id=current_user.id,
        action=req.action,
        params_json=json.dumps(payload, ensure_ascii=False),
        status="published_mqtt" if sent_mqtt else "queued_offline",
        executed_at=datetime.now(timezone.utc) if sent_mqtt else None,
    )
    db.add(db_cmd)
    db.add(
        History(
            device_id=req.device_id,
            performer_id=current_user.id,
            event_type="MQTT_COMMAND",
            description=f"{current_user.name} gửi lệnh MQTT '{req.action}' (row={req.row}) -> mqtt={sent_mqtt}",
            related_id=db_cmd.id,
        )
    )
    db.commit()
    db.refresh(db_cmd)
    return {
        "id": db_cmd.id,
        "device_id": db_cmd.device_id,
        "action": db_cmd.action,
        "row": req.row,
        "mqtt_published": sent_mqtt,
        "status": db_cmd.status,
    }


@router.get("/config")
def get_config(
    device_id: str,
    current_user: Annotated[User, Depends(require_operator_or_admin)],
    db: Session = Depends(get_db),
):
    dev = db.get(Device, device_id)
    parsed = None
    if dev and dev.config_json:
        try:
            parsed = json.loads(dev.config_json)
        except Exception:
            parsed = None
    return {"device_id": device_id, "config": parsed}


@router.post("/config")
def push_config(
    req: ConfigPublishRequest,
    current_user: Annotated[User, Depends(require_admin)],
    db: Session = Depends(get_db),
):
    dev = db.get(Device, req.device_id)
    if not dev:
        dev = Device(
            id=req.device_id,
            name=f"Thiết bị {req.device_id}",
            type="tomato_irrigation_3row",
            status="offline",
        )
        db.add(dev)
    dev.config_json = json.dumps(req.config, ensure_ascii=False)
    sent = mqtt_handler.publish_config(req.device_id, req.config)
    db.add(
        History(
            device_id=req.device_id,
            performer_id=current_user.id,
            event_type="MQTT_CONFIG",
            description=f"{current_user.name} đẩy cấu hình mới qua MQTT (sent={sent})",
        )
    )
    db.commit()
    return {"device_id": req.device_id, "mqtt_published": sent}


# =================== ALERTS, HISTORY, SCHEDULES ===================


@router.get("/alerts")
def get_alerts(device_id: Optional[str] = None, limit: int = 50, db: Session = Depends(get_db)):
    q = db.query(Alert)
    if device_id:
        q = q.filter(Alert.device_id == device_id)
    return q.order_by(desc(Alert.timestamp)).limit(min(limit, 200)).all()


@router.get("/history")
def get_history(limit: int = 100, db: Session = Depends(get_db)):
    rows = db.query(History).order_by(desc(History.timestamp)).limit(min(limit, 200)).all()
    user_ids = {h.performer_id for h in rows if h.performer_id}
    users_map = {}
    if user_ids:
        found_users = db.query(User).filter(User.id.in_(user_ids)).all()
        users_map = {u.id: (u.name or u.email) for u in found_users}

    return [
        {
            "id": h.id,
            "device_id": h.device_id,
            "performer_id": h.performer_id,
            "performer_name": users_map.get(h.performer_id) if h.performer_id else None,
            "event_type": h.event_type,
            "description": h.description,
            "timestamp": h.timestamp.isoformat() if h.timestamp else None,
        }
        for h in rows
    ]


@router.get("/schedules")
def get_schedules(db: Session = Depends(get_db)):
    return db.scalars(select(Schedule)).all()


# ==================== XUẤT CSV THEO KHOẢNG THỜI GIAN & SAO LƯU ====================

@router.get("/devices/{device_id}/export_check")
def export_check(
    device_id: str,
    start_time: str,
    end_time: str,
    db: Session = Depends(get_db),
):
    """Xác thực trong khoảng thời gian lựa chọn có dữ liệu hay không."""
    from csv_export import check_range_records
    return check_range_records(db, device_id, start_time, end_time)


@router.get("/devices/{device_id}/export_csv")
def export_csv(
    device_id: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Tải xuống file CSV dữ liệu theo mốc thời gian lựa chọn."""
    from csv_export import generate_telemetry_csv, parse_flexible_datetime, to_iso_search_string
    q = db.query(TelemetryLog).filter(TelemetryLog.device_id == device_id)

    clean_start = "all"
    clean_end = "now"

    if start_time:
        dt_s = parse_flexible_datetime(start_time)
        if dt_s:
            iso_s = to_iso_search_string(dt_s)
            q = q.filter(TelemetryLog.recorded_at >= iso_s)
            clean_start = dt_s.strftime("%Y%m%d_%H%M")
    if end_time:
        dt_e = parse_flexible_datetime(end_time)
        if dt_e:
            iso_e = to_iso_search_string(dt_e)
            q = q.filter(TelemetryLog.recorded_at <= iso_e)
            clean_end = dt_e.strftime("%Y%m%d_%H%M")

    records = q.order_by(TelemetryLog.id.asc()).all()
    if not records:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Không có bản ghi dữ liệu nào cho thiết bị {device_id} trong khoảng thời gian đã chọn.",
        )

    dev = db.query(Device).filter(Device.id == device_id).first()
    dev_name = dev.name if dev else device_id
    csv_text = generate_telemetry_csv(records, dev_name)

    filename = f"tuoi_cay_{device_id}_{clean_start}_den_{clean_end}.csv"
    return Response(
        content=csv_text.encode("utf-8"),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-cache",
        },
    )


@router.get("/backups")
def get_backups_list(
    response: Response,
    current_user: Annotated[User, Depends(require_operator_or_admin)],
):
    """Liệt kê danh sách các file CSV đã lưu trữ tự động."""
    from csv_export import list_saved_backups
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["Cloudflare-CDN-Cache-Control"] = "no-store"
    return list_saved_backups()


@router.get("/backups/{filename}")
def download_backup_file(
    filename: str,
    current_user: Annotated[User, Depends(require_operator_or_admin)],
):
    """Tải xuống file CSV lưu trữ tự động từ thư mục exports_csv."""
    import os
    from csv_export import EXPORTS_DIR
    safe_name = os.path.basename(filename)
    file_path = os.path.join(EXPORTS_DIR, safe_name)
    if not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="Không tìm thấy file sao lưu yêu cầu.")
    return FileResponse(
        file_path,
        media_type="text/csv",
        filename=safe_name,
        headers={"Cache-Control": "no-store, max-age=0", "Cloudflare-CDN-Cache-Control": "no-store"},
    )


@router.post("/backups/generate_today")
def trigger_backup_today(
    response: Response,
    current_user: Annotated[User, Depends(require_operator_or_admin)],
    db: Session = Depends(get_db),
):
    """Kích hoạt sao lưu toàn bộ dữ liệu ngày hôm nay ra CSV và đồng bộ Google Drive ngay lập tức."""
    from csv_export import perform_daily_backup
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["Cloudflare-CDN-Cache-Control"] = "no-store"
    return perform_daily_backup(db)
