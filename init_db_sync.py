from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import os

# Определяем модели прямо здесь (копия из bot.py)
Base = declarative_base()

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, BigInteger, Float, JSON, Index

# Копируем модели из bot.py
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, unique=True, nullable=False, index=True)
    username = Column(String(100))
    first_name = Column(String(100))
    last_name = Column(String(100))
    language = Column(String(10), default='ru')
    timezone = Column(String(50), default='Europe/Moscow')
    role = Column(String(20), default='user')
    status = Column(String(20), default='active')
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow)
    total_interactions = Column(Integer, default=0)
    notifications_enabled = Column(Boolean, default=True)
    notification_offset = Column(Integer, default=5)
    user_metadata = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

class Medicine(Base):
    __tablename__ = 'medicines'
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    name = Column(String(200), nullable=False)
    dosage = Column(String(100))
    times_per_day = Column(Integer, default=1)
    schedule_times = Column(JSON)
    schedule = Column(String(200))
    course_type = Column(String(20), default='unlimited')
    course_duration = Column(Integer)
    repeat_type = Column(String(20), default='none')
    repeat_interval = Column(Integer)
    start_date = Column(DateTime(timezone=True))
    end_date = Column(DateTime(timezone=True))
    user_timezone = Column(String(50), nullable=False)
    status = Column(String(20), default='active')
    total_taken = Column(Integer, default=0)
    total_skipped = Column(Integer, default=0)
    total_postponed = Column(Integer, default=0)
    stats = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

class Reminder(Base):
    __tablename__ = 'reminders'
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    reminder_type = Column(String(20))
    item_id = Column(Integer, nullable=False)
    scheduled_time = Column(DateTime(timezone=True), nullable=False, index=True)
    user_timezone = Column(String(50), nullable=False)
    status = Column(String(20), default='pending')
    retry_count = Column(Integer, default=0)
    last_error = Column(Text)
    postponed_until = Column(DateTime(timezone=True))
    created_at = Column(DateTime, default=datetime.utcnow)

# Добавьте остальные модели по аналогии (MedicineLog, Analysis, AnalysisLog, MoodLog, SymptomLog, AdminLog, Broadcast)

from datetime import datetime

# Добавляем недостающие модели
class MedicineLog(Base):
    __tablename__ = 'medicine_logs'
    id = Column(Integer, primary_key=True)
    medicine_id = Column(Integer, nullable=False, index=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    log_type = Column(String(20), default='scheduled')
    status = Column(String(20))
    dosage = Column(String(100))
    comment = Column(Text)
    reason = Column(Text)
    side_effects = Column(Text)
    scheduled_time = Column(DateTime(timezone=True))
    taken_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime, default=datetime.utcnow)

class Analysis(Base):
    __tablename__ = 'analyses'
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    analysis_type = Column(String(20), default='analysis')
    name = Column(String(200), nullable=False)
    scheduled_date = Column(DateTime(timezone=True), nullable=False, index=True)
    scheduled_time = Column(String(10), nullable=False)
    repeat_type = Column(String(20), default='once')
    repeat_interval = Column(Integer)
    reminder_before = Column(Integer, default=24)
    notes = Column(Text)
    status = Column(String(20), default='pending')
    user_timezone = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

class AnalysisLog(Base):
    __tablename__ = 'analysis_logs'
    id = Column(Integer, primary_key=True)
    analysis_id = Column(Integer, nullable=False, index=True)
    user_id = Column(BigInteger, nullable=False)
    status = Column(String(20))
    completed_at = Column(DateTime(timezone=True))
    notes = Column(Text)

class MoodLog(Base):
    __tablename__ = 'mood_logs'
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    mood_score = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True))

class SymptomLog(Base):
    __tablename__ = 'symptom_logs'
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    symptom = Column(String(100), nullable=False)
    severity = Column(Integer, nullable=False)
    severity_color = Column(String(20))
    created_at = Column(DateTime(timezone=True))

class AdminLog(Base):
    __tablename__ = 'admin_logs'
    id = Column(Integer, primary_key=True)
    admin_id = Column(BigInteger, nullable=False)
    action = Column(String(100), nullable=False)
    target_user_id = Column(BigInteger)
    details = Column(JSON)
    ip_address = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)

class Broadcast(Base):
    __tablename__ = 'broadcasts'
    id = Column(Integer, primary_key=True)
    admin_id = Column(BigInteger, nullable=False)
    message = Column(Text, nullable=False)
    filters = Column(JSON)
    status = Column(String(20), default='pending')
    total_recipients = Column(Integer, default=0)
    sent_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)

# Создаем таблицы
DATABASE_URL = "sqlite+aiosqlite:///lor_reminder.db"
SYNC_URL = DATABASE_URL.replace("sqlite+aiosqlite://", "sqlite://")

print(f"🔄 Создание таблиц в {SYNC_URL}...")
engine = create_engine(SYNC_URL)
Base.metadata.create_all(engine)
print("✅ Таблицы успешно созданы!")

# Проверяем созданные таблицы
inspector = inspect(engine)
tables = inspector.get_table_names()
print(f"📊 Созданные таблицы: {', '.join(tables)}")
