"""Web bridge for the single ESP32 programmer in ESP_OTA_STM32 (origin/main).

Its shared topics have no device/job IDs. Configure exactly one programmer and
station per broker namespace; only a fresh, non-retained probe enables flashing.
"""
import hashlib
import hmac
import json
import os
import re
import struct
import threading
import time
from pathlib import Path
from typing import Annotated
from urllib.parse import urlencode, urlsplit

from fastapi import APIRouter, Depends, HTTPException, Response, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

from auth import get_jwt_secret, require_admin

router = APIRouter(prefix="/firmware", tags=["STM32 firmware"])
MAX_IMAGE = 256 * 1024
FLASH_TOPIC = "esp32/to_stm32/flash"
CHECK_TOPIC = "esp32/to_stm32/check"
STATUS_TOPIC = "esp32/status"


def storage_dir():
    return Path(os.getenv("OTA_STORAGE_DIR", str(Path(__file__).parent / "firmware_uploads")))


def station_id():
    return os.getenv("OTA_DEVICE_ID", "").strip()


class ProgrammerState:
    def __init__(self):
        self.lock = threading.RLock()
        self.last_seen = 0.0
        self.probe_at = 0.0
        self.check_requested = 0.0
        self.link = "unknown"
        self.detail = "Chưa nhận phản hồi ESP32"
        self.operation = None
        self.logs = []

    def snapshot(self):
        with self.lock:
            marker = storage_dir() / "active.json"
            if self.operation is None and marker.exists():
                self.operation = {"state": "unknown", "detail": "Máy chủ đã khởi động lại; cần kiểm tra STM32 trước khi gửi lại.", "started_at": 0}
            if self.operation and self.operation["state"] in ("queued", "running") and time.time() - self.operation["started_at"] > 600:
                self.operation.update(state="unknown", detail="Quá thời gian chờ phản hồi. Chưa xác nhận kết quả nạp; hãy kiểm tra STM32.")
            return {
                "device_id": station_id(),
                "configured": bool(station_id()),
                "target": "STM32F401xB/C · tối đa 256 KiB · 0x08000000",
                "last_seen": self.last_seen or None,
                "stm32_link": self.link,
                "detail": self.detail,
                "probe_ready": self.link == "connected" and time.time() - self.probe_at < 60,
                "operation": dict(self.operation) if self.operation else None,
                "logs": list(self.logs),
            }

    def receive(self, payload, retained=False):
        # Retained messages may describe a previous flash, never the current job.
        if retained:
            return
        with self.lock:
            now = time.time()
            text = payload[:512]
            # The download URL carries a temporary capability. Do not expose it
            # in UI logs, even when echoed by the unmodified ESP32 firmware.
            text = re.sub(r"https?://\S+", "[tệp firmware]", text)
            self.last_seen = now
            self.logs.append({"time": now, "message": text})
            self.logs = self.logs[-80:]
            if text.startswith("online "):
                self.probe_at = 0
                self.link = "unknown"
            if text.startswith("stm32: DISCONNECTED") or "stm32=DISCONNECTED" in text:
                self.link = "disconnected"
                self.probe_at = 0
                self.detail = text
            elif text.startswith("stm32: CONNECTED"):
                self.detail = text
                self.link = "connected"
                if self.check_requested and now - self.check_requested < 30 and "DEV_ID=0x423" in text:
                    self.probe_at = now
                    self.check_requested = 0
                    if self.operation and self.operation["state"] == "unknown":
                        self.operation.update(state="unconfirmed", detail="Đã kiểm tra lại STM32; kết quả lần nạp trước vẫn chưa được xác nhận.")
                        (storage_dir() / "active.json").unlink(missing_ok=True)
            op = self.operation
            if op and op["state"] in ("queued", "running"):
                if text.startswith("flash: downloading "):
                    op.update(state="running", detail=text)
                elif text.startswith("flash: FAILED") or text.startswith("flash: no "):
                    op.update(state="failed", detail=text)
                    (storage_dir() / "active.json").unlink(missing_ok=True)
                elif text.startswith("flash: OK ") and op["state"] == "running":
                    op.update(state="succeeded", detail=text)
                    (storage_dir() / "active.json").unlink(missing_ok=True)
                elif text.startswith("flash:"):
                    op["detail"] = text
                elif text.startswith("busy:") or text.startswith("job queue full"):
                    op.update(state="unknown", detail=text)


ota_state = ProgrammerState()


def publish(topic, payload):
    from mqtt import mqtt_handler
    if not mqtt_handler.connected:
        raise HTTPException(503, "Máy chủ chưa kết nối MQTT")
    # No automatic retransmission: the existing programmer cannot deduplicate
    # flash commands. A queued publish is not proof of device execution.
    result = mqtt_handler.client.publish(topic, payload, qos=0, retain=False)
    if result.rc != 0:
        raise HTTPException(503, "Không gửi được lệnh MQTT")


def require_station(device_id):
    if not station_id() or device_id != station_id():
        raise HTTPException(409, "Trạm này chưa được gán ESP32 nạp firmware (OTA_DEVICE_ID)")


@router.get("/status")
def get_status(response: Response, _admin: Annotated[object, Depends(require_admin)]):
    from mqtt import mqtt_handler
    response.headers["Cache-Control"] = "no-store"
    return {**ota_state.snapshot(), "mqtt_connected": mqtt_handler.connected}


@router.post("/upload")
async def upload_image(file: UploadFile, _admin: Annotated[object, Depends(require_admin)]):
    name = (file.filename or "").replace("\\", "/").rsplit("/", 1)[-1]
    if not name.lower().endswith(".bin"):
        raise HTTPException(400, "Chọn firmware dạng .bin")
    raw = await file.read(MAX_IMAGE + 1)
    await file.close()
    if len(raw) < 8 or len(raw) > MAX_IMAGE:
        raise HTTPException(400, "Tệp phải có từ 8 byte đến 256 KiB")
    stack, reset = struct.unpack_from("<II", raw)
    if not (0x20000000 < stack <= 0x20010000 and stack % 4 == 0 and reset & 1 and 0x08000000 <= (reset & ~1) < 0x08000000 + len(raw)):
        raise HTTPException(400, "Vector khởi động không phù hợp ảnh STM32F401xB/C tại 0x08000000. Kiểm tra đúng bản build.")
    image_id = hashlib.sha256(raw).hexdigest()
    metadata = {"id": image_id, "filename": name[:120], "size": len(raw), "sha256": image_id}
    root = storage_dir()
    root.mkdir(parents=True, exist_ok=True)
    (root / f"{image_id}.bin").write_bytes(raw)
    (root / f"{image_id}.json").write_text(json.dumps(metadata), encoding="utf-8")
    return metadata


def image_metadata(image_id):
    if not re.fullmatch(r"[0-9a-f]{64}", image_id):
        raise HTTPException(404, "Không tìm thấy firmware")
    root = storage_dir()
    if not (root / f"{image_id}.bin").is_file() or not (root / f"{image_id}.json").is_file():
        raise HTTPException(404, "Không tìm thấy firmware; hãy tải lại tệp")
    return json.loads((root / f"{image_id}.json").read_text(encoding="utf-8"))


def signature(image_id, expires):
    return hmac.new(get_jwt_secret().encode(), f"{image_id}:{expires}".encode(), hashlib.sha256).hexdigest()


@router.get("/files/{image_id}.bin")
def download_image(image_id: str, expires: int, sig: str):
    if expires < time.time() or not hmac.compare_digest(signature(image_id, expires), sig):
        raise HTTPException(403, "Liên kết tải firmware đã hết hạn hoặc không hợp lệ")
    image_metadata(image_id)
    return FileResponse(storage_dir() / f"{image_id}.bin", media_type="application/octet-stream", headers={"Cache-Control": "no-store"})


class StationRequest(BaseModel):
    device_id: str


class FlashRequest(StationRequest):
    image_id: str
    confirm_target_and_stopped: bool = False


@router.post("/check")
def check_target(req: StationRequest, _admin: Annotated[object, Depends(require_admin)]):
    require_station(req.device_id)
    with ota_state.lock:
        state = ota_state.snapshot()
        if state["operation"] and state["operation"]["state"] in ("queued", "running"):
            raise HTTPException(409, "Đang chờ kết quả nạp; không kiểm tra SWD đồng thời")
        ota_state.probe_at = 0
        ota_state.check_requested = time.time()
        try:
            publish(CHECK_TOPIC, "check")
        except Exception:
            ota_state.check_requested = 0
            raise
    return {"status": "sent", "detail": "Đã gửi yêu cầu, đang chờ ESP32 kiểm tra STM32"}


@router.post("/flash")
def flash_target(req: FlashRequest, _admin: Annotated[object, Depends(require_admin)]):
    require_station(req.device_id)
    if not req.confirm_target_and_stopped:
        raise HTTPException(400, "Cần xác nhận đúng firmware, đã dừng tưới và giữ nguồn ổn định")
    metadata = image_metadata(req.image_id)
    if hashlib.sha256((storage_dir() / f"{req.image_id}.bin").read_bytes()).hexdigest() != req.image_id:
        raise HTTPException(409, "Tệp lưu trên máy chủ đã thay đổi; hãy tải lại firmware")
    # Set an explicit public base behind reverse proxies. It must be reachable
    # by the ESP32, not merely by the administrator's browser.
    base = os.getenv("OTA_PUBLIC_BASE_URL", "").strip().rstrip("/")
    parsed = urlsplit(base)
    if parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise HTTPException(503, "Cấu hình OTA_PUBLIC_BASE_URL bằng URL máy chủ ESP32 truy cập được")
    expires = int(time.time()) + 1800
    url = f"{base}/firmware/files/{req.image_id}.bin?{urlencode({'expires': expires, 'sig': signature(req.image_id, expires)})}"
    if len(url.encode()) >= 512:
        raise HTTPException(400, "URL tải firmware vượt giới hạn 511 byte của ESP32")
    with ota_state.lock:
        state = ota_state.snapshot()
        if state["operation"] and state["operation"]["state"] in ("queued", "running", "unknown"):
            raise HTTPException(409, "Lần nạp trước chưa kết thúc; kiểm tra STM32 trước khi gửi lại")
        if not state["probe_ready"]:
            raise HTTPException(409, "Hãy kiểm tra STM32 và nhận DEV_ID=0x423 trong vòng 60 giây trước khi nạp")
        previous = ota_state.operation
        ota_state.operation = {"state": "queued", "started_at": time.time(), "filename": metadata["filename"], "sha256": req.image_id, "detail": "Đã gửi lệnh MQTT; đang chờ ESP32 nhận và tải tệp"}
        root = storage_dir()
        (root / "active.json").write_text(json.dumps(ota_state.operation), encoding="utf-8")
        try:
            publish(FLASH_TOPIC, json.dumps({"url": url}))
        except Exception:
            ota_state.operation = previous
            (root / "active.json").unlink(missing_ok=True)
            raise
        ota_state.probe_at = 0
        return dict(ota_state.operation)
