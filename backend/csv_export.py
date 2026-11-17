from urllib.parse import quote
import csv
import io
import json
import logging
import os
import shutil
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import desc, func
from sqlalchemy.orm import Session

from db import TelemetryLog, Device

logger = logging.getLogger("tomato.csv_export")

# Thư mục lưu trữ CSV tự động
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXPORTS_DIR = os.path.join(BASE_DIR, "exports_csv")
os.makedirs(EXPORTS_DIR, exist_ok=True)

# Đường dẫn thư mục Google Drive (nếu người dùng cài Google Drive Desktop)
GOOGLE_DRIVE_PATH = os.getenv("GOOGLE_DRIVE_BACKUP_PATH", "").strip()


def parse_flexible_datetime(dt_str: str) -> Optional[datetime]:
    """Phân giải chuỗi thời gian nhiều định dạng: ISO, dd/mm/yyyy HH:MM, YYYY-MM-DD HH:MM..."""
    if not dt_str:
        return None
    dt_str = dt_str.strip()
    formats = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            pass
    # Thử cắt múi giờ nếu có
    if "T" in dt_str:
        clean = dt_str.split("+")[0].split(".")[0]
        for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"]:
            try:
                return datetime.strptime(clean, fmt)
            except ValueError:
                pass
    return None


def to_iso_search_string(dt: datetime) -> str:
    """Chuyển datetime thành chuỗi so sánh ISO YYYY-MM-DDTHH:MM:SS"""
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def check_range_records(
    db: Session,
    device_id: str,
    start_str: str,
    end_str: str,
) -> Dict[str, Any]:
    """Kiểm tra xem trong khoảng thời gian lựa chọn có dữ liệu hay không."""
    dt_start = parse_flexible_datetime(start_str)
    dt_end = parse_flexible_datetime(end_str)

    if not dt_start or not dt_end:
        return {
            "has_data": False,
            "count": 0,
            "error": "Định dạng thời gian không hợp lệ. Vui lòng nhập dạng YYYY-MM-DD HH:MM hoặc YYYY-MM-DDTHH:MM",
        }

    if dt_start > dt_end:
        dt_start, dt_end = dt_end, dt_start

    # Chuỗi so sánh
    iso_start = to_iso_search_string(dt_start)
    iso_end = to_iso_search_string(dt_end)

    query = (
        db.query(TelemetryLog)
        .filter(TelemetryLog.device_id == device_id)
        .filter(TelemetryLog.recorded_at >= iso_start)
        .filter(TelemetryLog.recorded_at <= iso_end)
    )

    count = query.count()
    if count == 0:
        # Thử tìm theo khoảng thời gian rộng hơn hoặc fallback received_at
        return {
            "device_id": device_id,
            "has_data": False,
            "count": 0,
            "start_time": iso_start,
            "end_time": iso_end,
            "message": f"Không tìm thấy bản ghi nào từ {iso_start} đến {iso_end}.",
        }

    first_rec = query.order_by(TelemetryLog.id.asc()).first()
    last_rec = query.order_by(TelemetryLog.id.desc()).first()

    return {
        "device_id": device_id,
        "has_data": True,
        "count": count,
        "start_time": iso_start,
        "end_time": iso_end,
        "first_record": first_rec.recorded_at if first_rec else None,
        "last_record": last_rec.recorded_at if last_rec else None,
        "message": f"Xác thực thành công: Tìm thấy {count} bản ghi dữ liệu.",
    }


def generate_telemetry_csv(records: List[TelemetryLog], device_name: str = "") -> str:
    """Tạo chuỗi CSV hoàn chỉnh có UTF-8 BOM cho Excel."""
    output = io.StringIO(newline="")
    # Ghi UTF-8 BOM để Excel hiển thị đúng dấu tiếng Việt
    output.write("\ufeff")

    writer = csv.writer(output, delimiter=",", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")

    # Tiêu đề cột chi tiết
    writer.writerow([
        "Mã số (ID)",
        "Thiết bị (Device ID)",
        "Thời gian đo (Recorded At)",
        "Thời gian nhận (Received At)",
        "Nhiệt độ không khí (°C)",
        "Độ ẩm không khí (%)",
        "Áp suất bốc hơi VPD (kPa)",
        "Độ ẩm Hàng B - Chậu 1 (%)",
        "Độ ẩm Hàng B - Chậu 3 (%)",
        "Độ ẩm TB Hàng B (%)",
        "Độ ẩm Hàng C - Chậu 1 (%)",
        "Độ ẩm Hàng C - Chậu 3 (%)",
        "Độ ẩm TB Hàng C (%)",
        "Trạng thái Bơm",
        "Phao bồn",
        "Chế độ điều khiển",
        "Van Hàng A",
        "Nước Hàng A hôm nay (Lít)",
        "Lưu lượng Hàng A (L/phút)",
        "Van Hàng B",
        "Nước Hàng B hôm nay (Lít)",
        "Lưu lượng Hàng B (L/phút)",
        "Van Hàng C",
        "Nước Hàng C hôm nay (Lít)",
        "Lưu lượng Hàng C (L/phút)",
        "Tổng nước tưới hôm nay (Lít)",
        "Sóng 4G RSSI (dBm)",
    ])

    for r in records:
        # Giải mã mảng độ ẩm đất [s1, s2, s3, s4]
        soil = [None, None, None, None]
        try:
            if r.soil_json:
                soil = json.loads(r.soil_json)
        except Exception:
            pass

        # Tính TB đất
        s1 = soil[0] if len(soil) > 0 else None
        s2 = soil[1] if len(soil) > 1 else None
        s3 = soil[2] if len(soil) > 2 else None
        s4 = soil[3] if len(soil) > 3 else None

        h2_avg = (
            round((s1 + s2) / 2, 2)
            if (s1 is not None and s2 is not None)
            else (s1 if s1 is not None else s2)
        )
        h3_avg = (
            round((s3 + s4) / 2, 2)
            if (s3 is not None and s4 is not None)
            else (s3 if s3 is not None else s4)
        )

        # Giải mã rows_json
        rows_data = []
        try:
            if r.rows_json:
                rows_data = json.loads(r.rows_json)
        except Exception:
            pass

        r1 = rows_data[0] if len(rows_data) > 0 else {}
        r2 = rows_data[1] if len(rows_data) > 1 else {}
        r3 = rows_data[2] if len(rows_data) > 2 else {}

        r1_l = float(r1.get("daily_l") or 0.0)
        r2_l = float(r2.get("daily_l") or 0.0)
        r3_l = float(r3.get("daily_l") or 0.0)
        total_l = round(r1_l + r2_l + r3_l, 2)

        writer.writerow([
            r.id,
            r.device_id,
            r.recorded_at,
            r.received_at.strftime("%Y-%m-%d %H:%M:%S") if r.received_at else "",
            f"{r.temperature_c:.1f}" if r.temperature_c is not None else "",
            f"{r.rh_pct:.1f}" if r.rh_pct is not None else "",
            f"{r.vpd_kpa:.2f}" if r.vpd_kpa is not None else "",
            f"{s1:.1f}" if s1 is not None else "",
            f"{s2:.1f}" if s2 is not None else "",
            f"{h2_avg:.1f}" if h2_avg is not None else "",
            f"{s3:.1f}" if s3 is not None else "",
            f"{s4:.1f}" if s4 is not None else "",
            f"{h3_avg:.1f}" if h3_avg is not None else "",
            "ĐANG CHẠY" if r.pump_on else "DỪNG",
            "Bồn OK" if r.tank_ok else "BỒN CẠN",
            "Tự động (AUTO)" if r.control_mode == "auto" else "Thủ công (MANUAL)",
            "Mở" if r1.get("valve") else "Đóng",
            f"{r1_l:.2f}",
            f"{float(r1.get('flow_lpm') or 0.0):.1f}",
            "Mở" if r2.get("valve") else "Đóng",
            f"{r2_l:.2f}",
            f"{float(r2.get('flow_lpm') or 0.0):.1f}",
            "Mở" if r3.get("valve") else "Đóng",
            f"{r3_l:.2f}",
            f"{float(r3.get('flow_lpm') or 0.0):.1f}",
            f"{total_l:.2f}",
            r.rssi_dbm if r.rssi_dbm is not None else "",
        ])

    return output.getvalue()


def perform_daily_backup(db: Session, target_date_str: Optional[str] = None) -> Dict[str, Any]:
    """Tổng hợp dữ liệu của một ngày và lưu file CSV tự động vào thư mục backup & Google Drive."""
    if not target_date_str:
        # Mặc định là ngày hôm nay
        target_date_str = date.today().strftime("%Y-%m-%d")

    iso_start = f"{target_date_str}T00:00:00"
    iso_end = f"{target_date_str}T23:59:59"

    # Lấy danh sách thiết bị
    devices = db.query(Device).all()
    if not devices:
        devices = [Device(id="stm32_01", name="Hệ thống tưới tự động (Trạm 1)")]

    saved_files = []
    drive_copied = []

    for dev in devices:
        records = (
            db.query(TelemetryLog)
            .filter(TelemetryLog.device_id == dev.id)
            .filter(TelemetryLog.recorded_at >= iso_start)
            .filter(TelemetryLog.recorded_at <= iso_end)
            .order_by(TelemetryLog.id.asc())
            .all()
        )

        if not records:
            continue

        csv_text = generate_telemetry_csv(records, dev.name or dev.id)
        filename = f"tuoi_cay_{dev.id}_{target_date_str}.csv"
        file_path = os.path.join(EXPORTS_DIR, filename)

        with open(file_path, "w", encoding="utf-8", newline="") as f:
            f.write(csv_text)

        saved_files.append({
            "filename": filename,
            "device_id": dev.id,
            "date": target_date_str,
            "records_count": len(records),
            "file_size": os.path.getsize(file_path),
        })

        # Nếu có cấu hình Google Drive, sao chép sang thư mục Drive
        gdrive_folder = os.getenv("GOOGLE_DRIVE_BACKUP_PATH", GOOGLE_DRIVE_PATH)
        if gdrive_folder and os.path.isdir(gdrive_folder):
            try:
                dest_path = os.path.join(gdrive_folder, filename)
                shutil.copy2(file_path, dest_path)
                drive_copied.append(dest_path)
                logger.info(f"Đã sao lưu sang Google Drive: {dest_path}")
            except Exception as e:
                logger.warning(f"Lỗi khi sao chép sang Google Drive '{gdrive_folder}': {e}")

    return {
        "status": "success",
        "date": target_date_str,
        "files_created": saved_files,
        "google_drive_copied": drive_copied,
        "exports_dir": EXPORTS_DIR,
    }


def list_saved_backups() -> List[Dict[str, Any]]:
    """Liệt kê các file CSV đã lưu trữ tự động trong thư mục exports_csv."""
    items = []
    if not os.path.exists(EXPORTS_DIR):
        return items

    for fname in os.listdir(EXPORTS_DIR):
        if fname.endswith(".csv"):
            full_path = os.path.join(EXPORTS_DIR, fname)
            stat = os.stat(full_path)
            size = stat.st_size
            mtime = datetime.fromtimestamp(stat.st_mtime)
            items.append({
                "filename": fname,
                "size_bytes": size,
                "size_formatted": f"{size / 1024:.1f} KB" if size >= 1024 else f"{size} B",
                "modified_at": mtime.strftime("%d/%m/%Y %H:%M:%S"),
                "download_url": "/backups/" + quote(fname, safe="") + f"?v={stat.st_mtime_ns}-{stat.st_size}",
            })

    items.sort(key=lambda x: x["modified_at"], reverse=True)
    return items
