#!/bin/bash

echo "========================================="
echo "  🚀 Развертывание ЛОР-Помощника"
echo "========================================="

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Функция для проверки ошибок
check_error() {
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Ошибка!${NC}"
        exit 1
    fi
}

# Шаг 1: Проверяем наличие Python
echo -e "\n${YELLOW}🔍 Проверка Python...${NC}"
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python3 не найден!${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Python3 найден: $(python3 --version)${NC}"

# Шаг 2: Удаляем старое виртуальное окружение, если оно есть
echo -e "\n${YELLOW}🗑️  Очистка...${NC}"
rm -rf venv 2>/dev/null
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

# Шаг 6: Устанавливаем зависимости
echo -e "\n${YELLOW}📦 Установка зависимостей...${NC}"
echo -e "${YELLOW}Это может занять несколько минут...${NC}"

pip install pytz aiofiles aiosqlite python-telegram-bot==20.7 apscheduler==3.10.4 sqlalchemy redis celery prometheus-client backoff tenacity python-dotenv requests async-timeout

check_error
echo -e "${GREEN}✅ Все зависимости установлены${NC}"

# Шаг 7: Проверяем наличие файла .env
echo -e "\n${YELLOW}🔍 Проверка файла .env...${NC}"
if [ ! -f ".env" ]; then
    echo -e "${RED}❌ Файл .env не найден!${NC}"
    echo -e "${YELLOW}Создаю шаблон .env...${NC}"
    echo "# Токен бота (получить у @BotFather)" > .env
    echo "BOT_TOKEN=ваш_токен_сюда" >> .env
    echo -e "${YELLOW}⚠️  Отредактируйте файл .env и добавьте реальный токен!${NC}"
    echo -e "${YELLOW}   nano .env${NC}"
else
    echo -e "${GREEN}✅ Файл .env найден${NC}"
    
    # Проверяем, что токен не пустой
    if grep -q "BOT_TOKEN=.\+" .env && ! grep -q "BOT_TOKEN=ваш_токен_сюда" .env; then
        echo -e "${GREEN}✅ Токен найден${NC}"
    else
        echo -e "${RED}❌ В файле .env не указан корректный токен!${NC}"
        echo -e "${YELLOW}Отредактируйте файл .env и добавьте реальный токен${NC}"
        echo -e "${YELLOW}   nano .env${NC}"
    fi
fi

# Шаг 8: Исправляем init_db_sync.py
echo -e "\n${YELLOW}🔧 Исправление init_db_sync.py...${NC}"

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
echo -e "${GREEN}✅ init_db_sync.py исправлен${NC}"

# Шаг 9: Создаем таблицы
echo -e "\n${YELLOW}🗄️  Создание таблиц в базе данных...${NC}"
python init_db_sync.py
check_error
echo -e "${GREEN}✅ Таблицы созданы${NC}"

# Шаг 10: Даем права на выполнение скриптам
echo -e "\n${YELLOW}🔧 Настройка прав доступа...${NC}"
chmod +x start.sh 2>/dev/null
chmod +x auto_restart.sh 2>/dev/null
chmod +x run.sh 2>/dev/null
echo -e "${GREEN}✅ Права установлены${NC}"

echo -e "\n${GREEN}=========================================${NC}"
echo -e "${GREEN}  🎉 Развертывание завершено успешно!${NC}"
echo -e "${GREEN}=========================================${NC}"
echo -e "\n${YELLOW}Для запуска бота выполните одну из команд:${NC}"
echo -e "  ${GREEN}./start.sh${NC} - обычный запуск"
echo -e "  ${GREEN}./auto_restart.sh${NC} - запуск с автоматическим перезапуском"
echo -e "  ${GREEN}python bot.py${NC} - прямой запуск"
echo -e "\n${YELLOW}Или активируйте окружение и запустите вручную:${NC}"
echo -e "  ${GREEN}source venv/bin/activate${NC}"
echo -e "  ${GREEN}python bot.py${NC}"
EOF
