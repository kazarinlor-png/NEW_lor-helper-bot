#!/bin/bash

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  ЛОР-Помощник - Полная настройка${NC}"
echo -e "${GREEN}========================================${NC}"

# Функция для проверки ошибок
check_error() {
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Ошибка!${NC}"
        exit 1
    fi
}

# Шаг 1: Деактивируем текущее виртуальное окружение, если оно активно
echo -e "\n${YELLOW}🔍 Проверка окружения...${NC}"
if command -v deactivate &> /dev/null; then
    deactivate 2>/dev/null
fi

# Шаг 2: Удаляем старые виртуальные окружения
echo -e "\n${YELLOW}🗑️  Удаление старых виртуальных окружений...${NC}"
rm -rf venv .venv 2>/dev/null
echo -e "${GREEN}✅ Готово${NC}"

# Шаг 3: Создаем новое виртуальное окружение
echo -e "\n${YELLOW}📦 Создание виртуального окружения...${NC}"
python3 -m venv venv
check_error
echo -e "${GREEN}✅ Виртуальное окружение создано${NC}"

# Шаг 4: Активируем виртуальное окружение
echo -e "\n${YELLOW}🚀 Активация виртуального окружения...${NC}"
source venv/bin/activate
check_error
echo -e "${GREEN}✅ Виртуальное окружение активировано${NC}"

# Шаг 5: Обновляем pip
echo -e "\n${YELLOW}📦 Обновление pip...${NC}"
pip install --upgrade pip
check_error
echo -e "${GREEN}✅ Pip обновлен${NC}"

# Шаг 6: Устанавливаем все зависимости
echo -e "\n${YELLOW}📦 Установка зависимостей...${NC}"
echo -e "${YELLOW}Это может занять несколько минут...${NC}"

pip install pytz aiofiles aiosqlite python-telegram-bot==20.7 apscheduler==3.10.4 sqlalchemy redis celery prometheus-client backoff tenacity python-dotenv requests async-timeout

check_error
echo -e "${GREEN}✅ Все зависимости установлены${NC}"

# Шаг 7: Проверяем наличие .env файла
echo -e "\n${YELLOW}🔍 Проверка файла .env...${NC}"
if [ ! -f .env ]; then
    echo -e "${RED}❌ Файл .env не найден!${NC}"
    echo -e "${YELLOW}Создайте файл .env с токеном бота:${NC}"
    echo "BOT_TOKEN=ваш_токен_сюда" > .env.example
    echo -e "${YELLOW}Пример создан в .env.example${NC}"
    echo -e "${YELLOW}Переименуйте его и добавьте токен:${NC}"
    echo "mv .env.example .env"
    echo "nano .env"
else
    echo -e "${GREEN}✅ Файл .env найден${NC}"
    
    # Проверяем, что токен не пустой
    if grep -q "BOT_TOKEN=.*" .env; then
        TOKEN=$(grep "BOT_TOKEN=" .env | cut -d '=' -f2)
        if [ -z "$TOKEN" ] || [ "$TOKEN" == "ваш_токен_сюда" ]; then
            echo -e "${RED}❌ Токен в .env не установлен!${NC}"
            echo -e "${YELLOW}Отредактируйте файл .env и добавьте реальный токен${NC}"
            echo -e "${YELLOW}nano .env${NC}"
        else
            echo -e "${GREEN}✅ Токен найден${NC}"
        fi
    else
        echo -e "${RED}❌ В файле .env нет строки BOT_TOKEN=...${NC}"
    fi
fi

# Шаг 8: Создаем исправленный init_db_sync.py
echo -e "\n${YELLOW}📝 Создание исправленного init_db_sync.py...${NC}"

cat > init_db_sync.py << 'EOF'
from sqlalchemy import create_engine, inspect, Column, Integer, String, DateTime, Text, Boolean, BigInteger, Float, JSON
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

# Модели из bot.py
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

inspector = inspect(engine)
tables = inspector.get_table_names()
print(f"📊 Созданные таблицы: {', '.join(tables)}")
EOF

check_error
echo -e "${GREEN}✅ init_db_sync.py создан${NC}"

# Шаг 9: Создаем таблицы в базе данных
echo -e "\n${YELLOW}🗄️  Создание таблиц в базе данных...${NC}"
python init_db_sync.py
check_error
echo -e "${GREEN}✅ Таблицы созданы${NC}"

# Шаг 10: Даем права на выполнение start.sh
echo -e "\n${YELLOW}🔧 Настройка прав доступа...${NC}"
chmod +x start.sh 2>/dev/null
echo -e "${GREEN}✅ Права установлены${NC}"

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}  🎉 Настройка завершена успешно!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "\n${YELLOW}Для запуска бота выполните:${NC}"
echo -e "  ./start.sh"
echo -e "\n${YELLOW}Или одной командой:${NC}"
echo -e "  source venv/bin/activate && ./start.sh"
echo -e "\n${GREEN}Удачного использования! 🚀${NC}"
EOF
