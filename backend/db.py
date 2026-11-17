"""
Định nghĩa cơ sở dữ liệu cho nhánh chạy song song MQTT + FastAPI trên VPS.
Tham khảo cấu trúc từ tue-minh/IoT_Backend, tối ưu để chạy mượt trên cả SQLite (VPS NAT)
và PostgreSQL/Supabase với mã thiết bị linh hoạt (ví dụ: stm32_01 hoặc UUID).
"""

import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
_db_env = os.getenv("DATABASE_URL")
if not _db_env or _db_env.strip() in ("sqlite:///./tomato_mqtt.db", "sqlite:///tomato_mqtt.db"):
    DATABASE_URL = f"sqlite:///{((BASE_DIR / 'tomato_mqtt.db').as_posix())}"
else:
    DATABASE_URL = _db_env

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def new_id() -> str:
    return str(uuid.uuid4())


class User(Base):
    __tablename__ = "users"

    id = Column(String(64), primary_key=True, default=new_id)
    email = Column(String(255), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    role = Column(String(32), nullable=False, default="guest")
    password_hash = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utc_now)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)

    devices = relationship("Device", back_populates="owner")
    commands = relationship("Command", back_populates="performer")
    schedules = relationship("Schedule", back_populates="performer")

    @property
    def username(self) -> str:
        return self.email


class Device(Base):
    __tablename__ = "devices"

    id = Column(String(64), primary_key=True, default=new_id)
    user_id = Column(String(64), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    name = Column(String(255), nullable=False)
    type = Column(String(64), default="tomato_irrigation_3row")
    status = Column(String(32), default="offline")
    last_seq = Column(Integer, default=0)
    last_seen_at = Column(DateTime(timezone=True), nullable=True)
    config_json = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=utc_now)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)

    owner = relationship("User", back_populates="devices")
    sensor_data = relationship("SensorData", back_populates="device", cascade="all, delete-orphan")
    telemetry_logs = relationship("TelemetryLog", back_populates="device", cascade="all, delete-orphan")
    commands = relationship("Command", back_populates="device", cascade="all, delete-orphan")
    schedules = relationship("Schedule", back_populates="device", cascade="all, delete-orphan")


class SensorData(Base):
    """Bảng trạng thái giá trị cảm biến mới nhất theo từng loại (tương thích IoT_Backend)."""

    __tablename__ = "sensor_data"

    id = Column(String(64), primary_key=True, default=new_id)
    device_id = Column(String(64), ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True)
    type = Column(String(64), nullable=False, index=True)
    value_number = Column(Float, nullable=True)
    value_string = Column(Text, nullable=True)
    unit = Column(String(32), nullable=True)
    timestamp = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)

    device = relationship("Device", back_populates="sensor_data")


class TelemetryLog(Base):
    """Bảng lưu trữ chuỗi thời gian đầy đủ cho 12 chậu cà chua 3 hàng."""

    __tablename__ = "telemetry_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    device_id = Column(String(64), ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True)
    seq = Column(Integer, nullable=False, default=0)
    recorded_at = Column(String(64), nullable=False)
    received_at = Column(DateTime(timezone=True), default=utc_now, index=True)
    soil_json = Column(Text, nullable=False)  # [s1, s2, s3, s4]
    temperature_c = Column(Float, nullable=True)
    rh_pct = Column(Float, nullable=True)
    vpd_kpa = Column(Float, nullable=True)
    rows_json = Column(Text, nullable=False)  # [{id, valve, daily_l, flow_lpm, mode}, ...]
    tank_ok = Column(Integer, nullable=False, default=1)
    pump_on = Column(Integer, nullable=False, default=0)
    control_mode = Column(String(16), nullable=True, default="manual")
    rssi_dbm = Column(Integer, nullable=True)
    rtc_time = Column(String(32), nullable=True)
    source = Column(String(32), nullable=False, default="mqtt")
    cf_synced = Column(Boolean, nullable=False, default=False)

    device = relationship("Device", back_populates="telemetry_logs")


class EventLog(Base):
    """Nhật ký sự kiện tưới và cảnh báo lỗi từ phần cứng qua MQTT."""

    __tablename__ = "event_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    device_id = Column(String(64), ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True)
    row = Column(Integer, nullable=True)
    type = Column(String(64), nullable=False, index=True)
    liters = Column(Float, nullable=True)
    detail_json = Column(Text, nullable=True)
    recorded_at = Column(String(64), nullable=False)
    received_at = Column(DateTime(timezone=True), default=utc_now, index=True)
    cf_synced = Column(Boolean, nullable=False, default=False)


class MqttMessageLog(Base):
    """Raw inbound MQTT messages for diagnosing device communication."""

    __tablename__ = "mqtt_message_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    device_id = Column(String(64), ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True)
    topic = Column(String(255), nullable=False)
    payload = Column(Text, nullable=False)
    payload_bytes = Column(Integer, nullable=False)
    truncated = Column(Boolean, nullable=False, default=False)
    qos = Column(Integer, nullable=False, default=0)
    retained = Column(Boolean, nullable=False, default=False)
    received_at = Column(DateTime(timezone=True), default=utc_now, nullable=False, index=True)


class Command(Base):
    __tablename__ = "commands"

    id = Column(String(64), primary_key=True, default=new_id)
    device_id = Column(String(64), ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True)
    performer_id = Column(String(64), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    action = Column(String(64), nullable=False)
    params_json = Column(Text, default="{}")
    status = Column(String(32), default="published_mqtt")
    created_at = Column(DateTime(timezone=True), default=utc_now)
    executed_at = Column(DateTime(timezone=True), nullable=True)

    device = relationship("Device", back_populates="commands")
    performer = relationship("User", back_populates="commands")


class Schedule(Base):
    __tablename__ = "schedules"

    id = Column(String(64), primary_key=True, default=new_id)
    device_id = Column(String(64), ForeignKey("devices.id", ondelete="CASCADE"), nullable=False)
    performer_id = Column(String(64), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    action = Column(String(64), nullable=False)
    params_json = Column(Text, default="{}")
    cron = Column(String(64), nullable=False)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=utc_now)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)

    device = relationship("Device", back_populates="schedules")
    performer = relationship("User", back_populates="schedules")


class History(Base):
    __tablename__ = "history"

    id = Column(String(64), primary_key=True, default=new_id)
    device_id = Column(String(64), nullable=True)
    performer_id = Column(String(64), nullable=True)
    event_type = Column(String(64), nullable=False)
    description = Column(Text, nullable=False)
    related_id = Column(String(64), nullable=True)
    timestamp = Column(DateTime(timezone=True), default=utc_now)


class Alert(Base):
    __tablename__ = "alerts"

    id = Column(String(64), primary_key=True, default=new_id)
    device_id = Column(String(64), ForeignKey("devices.id", ondelete="CASCADE"), nullable=False)
    sensor_data_id = Column(String(64), nullable=True)
    type = Column(String(64), nullable=False)
    message = Column(Text, nullable=False)
    timestamp = Column(DateTime(timezone=True), default=utc_now)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_tables():
    from sqlalchemy import inspect, text

    Base.metadata.create_all(bind=engine)
    # Tự động bổ sung cột mới nếu file DB cũ đã tồn tại
    inspector = inspect(engine)
    migrations = {
        "devices": [("config_json", "TEXT")],
        "telemetry_logs": [("rssi_dbm", "INTEGER"), ("rtc_time", "VARCHAR(32)"), ("control_mode", "VARCHAR(16)")],
    }
    with engine.begin() as conn:
        for table_name, cols in migrations.items():
            if not inspector.has_table(table_name):
                continue
            existing = {c["name"] for c in inspector.get_columns(table_name)}
            for col_name, col_type in cols:
                if col_name not in existing:
                    conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"))
