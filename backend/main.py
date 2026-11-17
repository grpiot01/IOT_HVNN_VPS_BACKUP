"""
Điểm khởi chạy FastAPI + MQTT + WebSocket cho VPS (tham khảo tue-minh/IoT_Backend).
Chạy song song với Cloudflare Worker D1, cung cấp WebSocket realtime (/ws/sensor_data)
và phát lệnh tức thì (<100ms) xuống thiết bị qua MQTT.
"""

import asyncio
import logging
import os
import secrets
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import inspect

from api import router
from drive_backup import router as drive_backup_router
from firmware_ota import router as firmware_ota_router
from auth import get_password_hash
from bridge import broadcast_worker
from db import Device, SessionLocal, User, create_tables, engine
from mqtt import mqtt_handler
from websocket_manager import manager

load_dotenv()
logger = logging.getLogger(__name__)


def seed_initial_records():
    """Khởi tạo thiết bị mặc định (stm32_01) và 3 tài khoản sẵn có: admin/user/guest."""
    db = SessionLocal()
    try:
        default_dev_id = os.getenv("DEFAULT_DEVICE_ID", "stm32_01").strip() or "stm32_01"
        default_dev_name = os.getenv("DEFAULT_DEVICE_NAME", "Hệ thống tưới cà chua 3 hàng (HVNN)")
        if db.query(Device).count() == 0:
            db.add(Device(id=default_dev_id, name=default_dev_name, type="tomato_irrigation_3row", status="offline"))

        preset_accounts = [
            (os.getenv("ADMIN_USERNAME", "admin").strip() or "admin", "Quản trị (Admin)", "admin", os.getenv("ADMIN_PASSWORD", "REDACTED_ADMIN_PASSWORD_SOURCE_DEFAULT_RESTORE_FROM_ENV")),
            (os.getenv("USER_USERNAME", "user").strip() or "user", "Vận hành (User)", "user", os.getenv("USER_PASSWORD", "REDACTED_USER_PASSWORD_SOURCE_DEFAULT_RESTORE_FROM_ENV")),
            (os.getenv("GUEST_USERNAME", "guest").strip() or "guest", "Khách (Guest)", "guest", os.getenv("GUEST_PASSWORD", "REDACTED_GUEST_PASSWORD_SOURCE_DEFAULT_RESTORE_FROM_ENV")),
        ]
        for uname, display_name, role_val, raw_pw in preset_accounts:
            existing = db.query(User).filter(User.email == uname).first()
            if not existing:
                db.add(
                    User(
                        email=uname,
                        name=display_name,
                        role=role_val,
                        password_hash=get_password_hash(raw_pw),
                    )
                )
                logger.info("[INIT] Đã tạo tài khoản mặc định '%s' (bậc=%s)", uname, role_val)

        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("Lỗi khởi tạo dữ liệu mặc định: %s", exc)
    finally:
        db.close()


async def daily_csv_backup_worker():
    """Tác vụ chạy ngầm tự động sao lưu dữ liệu ra file CSV & Google Drive."""
    while True:
        try:
            await asyncio.sleep(1800)  # Kiểm tra mỗi 30 phút
            from csv_export import perform_daily_backup
            from db import SessionLocal
            with SessionLocal() as db:
                res = perform_daily_backup(db)
                if res.get("files_created"):
                    logger.info(f"[Auto-Backup CSV] Đã tự động tạo {len(res['files_created'])} file CSV backup.")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[Auto-Backup CSV] Lỗi: {e}")
            await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Đang khởi động Tomato IoT MQTT Backend trên VPS...")
    create_tables()
    seed_initial_records()

    loop = asyncio.get_running_loop()
    mqtt_handler.loop = loop
    worker_task = asyncio.create_task(broadcast_worker())
    backup_task = asyncio.create_task(daily_csv_backup_worker())

    mqtt_handler.connect()
    logger.info("Hệ thống VPS MQTT + FastAPI đã sẵn sàng!")

    yield

    logger.info("Đang tắt hệ thống...")
    worker_task.cancel()
    backup_task.cancel()
    mqtt_handler.disconnect()
    logger.info("Đã tắt an toàn.")


app = FastAPI(
    title="Tomato Irrigation IoT Backend (MQTT + WebSocket + Dual-Sync)",
    description="Backend FastAPI chạy song song MQTT Broker, đẩy dữ liệu realtime qua WebSocket và đồng bộ sang Cloudflare D1.",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
app.include_router(drive_backup_router)
app.include_router(firmware_ota_router)
templates = Jinja2Templates(directory="templates")

STATIC_DIR = Path(__file__).resolve().parent / "static"
if (STATIC_DIR / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=str(STATIC_DIR / "assets")), name="assets")


@app.get("/health")
def health_check():
    cf_enabled = os.getenv("CF_SYNC_ENABLED", "true").lower() == "true"
    return {
        "status": "healthy",
        "service": "tomato-mqtt-vps-backend",
        "database": "connected",
        "web_ui_bundled": (STATIC_DIR / "index.html").is_file(),
        "mqtt_configured": mqtt_handler.is_configured(),
        "mqtt_connected": mqtt_handler.connected,
        "mqtt_broker": mqtt_handler.broker or None,
        "ws_clients": len(manager.active_connections),
        "cloudflare_sync_enabled": cf_enabled,
    }


@app.get("/status", response_class=HTMLResponse)
def backend_status_page():
    mqtt_badge = "🟢 Đã kết nối Broker" if mqtt_handler.connected else "🟡 Chưa kết nối MQTT"
    return f"""
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8" />
        <title>VPS MQTT IoT Backend — HVNN</title>
        <style>
            body {{ font-family: system-ui, -apple-system, sans-serif; background: #0f172a; color: #f8fafc; display: flex; min-height: 100vh; align-items: center; justify-content: center; margin: 0; }}
            .card {{ background: #1e293b; padding: 36px; border-radius: 16px; box-shadow: 0 10px 30px rgba(0,0,0,0.4); max-width: 460px; width: 90%; text-align: center; border: 1px solid #334155; }}
            h1 {{ margin-top: 0; font-size: 20px; color: #34d399; }}
            .pill {{ display: inline-block; padding: 6px 14px; border-radius: 999px; background: #0f172a; border: 1px solid #334155; font-size: 13px; margin: 8px 0 18px; }}
            a {{ color: #38bdf8; text-decoration: none; font-weight: 700; display: block; margin-top: 12px; padding: 10px; border-radius: 8px; background: #0f172a; }}
            a:hover {{ background: #334155; }}
        </style>
    </head>
    <body>
        <div class="card">
            <h1>🌱 Tomato Irrigation — VPS MQTT Backend</h1>
            <div class="pill">{mqtt_badge} • WebSocket: {len(manager.active_connections)} client</div>
            <p style="font-size: 13px; color: #94a3b8;">Nhánh chạy song song MQTT + FastAPI WebSocket + Đồng bộ Cloudflare D1</p>
            <a href="/">🖥️ Mở Giao diện Điều khiển (HMI 2.0)</a>
            <a href="/docs">📘 Mở API Swagger (/docs)</a>
            <a href="/docs/table">🗄 Xem cấu trúc CSDL (/docs/table)</a>
            <a href="/health">💓 Kiểm tra trạng thái (/health)</a>
        </div>
    </body>
    </html>
    """


@app.get("/")
def root():
    index_file = STATIC_DIR / "index.html"
    if index_file.is_file():
        return FileResponse(
            str(index_file),
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )
    return backend_status_page()


@app.get("/docs/table", response_class=HTMLResponse)
def table_docs(request: Request):
    inspector = inspect(engine)
    tables = {}
    fks = []
    for table_name in inspector.get_table_names():
        pk_cols = set(inspector.get_pk_constraint(table_name).get("constrained_columns") or [])
        cols = []
        for col in inspector.get_columns(table_name):
            cols.append(
                {
                    "column_name": col["name"],
                    "data_type": str(col["type"]),
                    "is_nullable": "YES" if col.get("nullable", True) else "NO",
                    "primary_key": col["name"] in pk_cols,
                }
            )
        tables[table_name] = cols
        for fk in inspector.get_foreign_keys(table_name):
            for idx, col_name in enumerate(fk.get("constrained_columns") or []):
                ref_cols = fk.get("referred_columns") or [""]
                fks.append(
                    {
                        "table_name": table_name,
                        "column_name": col_name,
                        "foreign_table": fk.get("referred_table", ""),
                        "foreign_column": ref_cols[idx] if idx < len(ref_cols) else "",
                    }
                )
    return templates.TemplateResponse(request=request, name="table_docs.html", context={"tables": tables, "fks": fks})


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "10000"))
    uvicorn.run("main:app", host=host, port=port, reload=False)
