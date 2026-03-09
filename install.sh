#!/bin/bash

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Функции для вывода
print_header() {
    echo -e "\n${BLUE}══════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  $1${NC}"
    echo -e "${BLUE}══════════════════════════════════════════════════${NC}\n"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${CYAN}ℹ️  $1${NC}"
}

# Создание директории для логов установки
mkdir -p install_logs
LOG_FILE="install_logs/install_$(date +%Y%m%d_%H%M%S).log"

# Функция логирования
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Функция проверки ошибок
check_error() {
    if [ $? -ne 0 ]; then
        log "❌ Ошибка: $1"
        print_error "$1"
        exit 1
    fi
}

print_header "УСТАНОВКА ЛОР-ПОМОЩНИКА ДЛЯ CODESPACES"

# Определение окружения
print_info "Проверка окружения..."
if [ -n "$CODESPACES" ] || [ -n "$GITPOD_WORKSPACE_ID" ]; then
    print_info "Обнаружено облачное окружение"
    IS_CLOUD=true
else
    IS_CLOUD=false
fi

# Проверка наличия Python
print_info "Проверка Python..."
if ! command -v python3 &> /dev/null; then
    print_error "Python3 не найден!"
    exit 1
fi
print_success "Python3: $(python3 --version)"

# Создание виртуального окружения
print_header "НАСТРОЙКА ВИРТУАЛЬНОГО ОКРУЖЕНИЯ"
if [ ! -d "venv" ]; then
    python3 -m venv venv
    check_error "Не удалось создать виртуальное окружение"
    print_success "Виртуальное окружение создано"
else
    print_warning "Виртуальное окружение уже существует"
fi

# Активация виртуального окружения
source venv/bin/activate
print_success "Виртуальное окружение активировано"

# Обновление pip
pip install --upgrade pip >> "$LOG_FILE" 2>&1
print_success "Pip обновлен"

# Установка зависимостей
print_header "УСТАНОВКА ЗАВИСИМОСТЕЙ"

# Для Codespaces используем SQLite вместо PostgreSQL
print_info "Установка Python пакетов..."

pip install \
    aiofiles \
    aiosqlite \
    python-telegram-bot==20.7 \
    apscheduler==3.10.4 \
    sqlalchemy \
    prometheus-client \
    python-dotenv \
    backoff \
    tenacity \
    requests \
    psutil \
    pytz >> "$LOG_FILE" 2>&1
check_error "Не удалось установить зависимости"
print_success "Все зависимости установлены"

# Создание файла .env
print_header "СОЗДАНИЕ ФАЙЛА .ENV"
if [ ! -f ".env" ]; then
    # Генерация случайного пароля для админа (если не в Codespaces)
    ADMIN_PASSWORD=$(openssl rand -base64 12 | tr -dc 'a-zA-Z0-9' | head -c 12 2>/dev/null || echo "admin123")
    
    cat > .env << EOF
# Telegram Bot Token (получить у @BotFather)
# Инструкция: https://core.telegram.org/bots#6-botfather
BOT_TOKEN=ВАШ_ТОКЕН_БОТА_СЮДА

# ID администраторов (через запятую)
# Можно узнать у @userinfobot
ADMIN_IDS=

# Версия бота
BOT_VERSION=14.0.0

# Настройки производительности
RATE_LIMIT_USER=1.0
PAGE_SIZE=10

# Для Codespaces используем SQLite (не требует установки)
USE_SQLITE=true
DATABASE_URL=sqlite+aiosqlite:///lor_reminder.db
SYNC_DATABASE_URL=sqlite:///lor_reminder.db

# Redis (опционально) - в Codespaces обычно не доступен
REDIS_ENABLED=false
EOF
    print_success "Файл .env создан"
    print_warning "⚠️  НЕ ЗАБУДЬТЕ ОТРЕДАКТИРОВАТЬ ФАЙЛ .env!"
    print_info "nano .env"
    print_info "Добавьте токен бота от @BotFather"
else
    print_warning "Файл .env уже существует"
fi

# Создание упрощенной версии bot.py для тестирования
print_header "СОЗДАНИЕ ТЕСТОВОЙ ВЕРСИИ БОТА"

if [ ! -f "bot.py" ]; then
    cat > bot.py << 'EOF'
#!/usr/bin/env python3
"""
ЛОР-Помощник - Упрощенная версия для тестирования
Использует SQLite вместо PostgreSQL
"""

import asyncio
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ConversationHandler, MessageHandler, filters, ContextTypes
)
from telegram.constants import ParseMode

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Состояния для диалогов
class States:
    MAIN_MENU = 0
    ADD_MEDICINE_NAME = 1
    ADD_MEDICINE_DOSAGE = 2

# Класс для работы с SQLite
import sqlite3
import json
from pathlib import Path

class Database:
    def __init__(self, db_path="lor_reminder.db"):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Таблица пользователей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                timezone TEXT DEFAULT 'Europe/Moscow',
                role TEXT DEFAULT 'user',
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица лекарств
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS medicines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                dosage TEXT,
                schedule TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица настроения
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mood_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                mood_score INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("База данных инициализирована")
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    async def add_user(self, user_id, username, first_name, last_name=None):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO users (user_id, username, first_name, last_name, last_seen)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (user_id, username, first_name, last_name))
        conn.commit()
        conn.close()
    
    async def add_medicine(self, user_id, name, dosage=None):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO medicines (user_id, name, dosage)
            VALUES (?, ?, ?)
        ''', (user_id, name, dosage))
        conn.commit()
        conn.close()
    
    async def get_medicines(self, user_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT name, dosage FROM medicines 
            WHERE user_id = ? ORDER BY created_at DESC
        ''', (user_id,))
        medicines = cursor.fetchall()
        conn.close()
        return medicines
    
    async def add_mood(self, user_id, score):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO mood_logs (user_id, mood_score)
            VALUES (?, ?)
        ''', (user_id, score))
        conn.commit()
        conn.close()

# Инициализация базы данных
db = Database()

# Обработчики команд
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await db.add_user(user.id, user.username, user.first_name, user.last_name)
    
    text = f"""👋 *Здравствуйте, {user.first_name}!*

Я *ЛОР-Помощник* — персональный медицинский бот.

👶 *Ведет прием детей с 0 лет и взрослых*

*Мои возможности:*
• 💊 Напоминания о приеме лекарств
• 📊 Отслеживание самочувствия
• 📈 Статистика и отчеты"""
    
    keyboard = [
        [InlineKeyboardButton("💊 Добавить лекарство", callback_data="add_medicine")],
        [InlineKeyboardButton("📋 Список лекарств", callback_data="list_medicines")],
        [InlineKeyboardButton("😊 Оценить настроение", callback_data="mood")],
        [InlineKeyboardButton("👨‍⚕️ О враче", callback_data="about")]
    ]
    
    await update.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )

async def about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    text = """👨‍⚕️ *Денис Сергеевич Казарин* - врач-оториноларинголог

👶 *Ведет прием детей с 0 лет и взрослых*

📱 *Социальные сети:*
• 👥 Telegram канал: @KAZARIN_LOR
• 💬 Личный Telegram: @deniskazarin"""
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="start")]]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )

async def mood(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [
            InlineKeyboardButton("1 😢", callback_data="mood_1"),
            InlineKeyboardButton("2 🙁", callback_data="mood_2"),
            InlineKeyboardButton("3 😐", callback_data="mood_3"),
            InlineKeyboardButton("4 🙂", callback_data="mood_4"),
            InlineKeyboardButton("5 😊", callback_data="mood_5")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data="start")]
    ]
    
    await query.edit_message_text(
        "📊 *Как вы себя чувствуете сегодня?*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )

async def mood_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    mood_score = int(query.data.replace("mood_", ""))
    user_id = update.effective_user.id
    
    await db.add_mood(user_id, mood_score)
    
    mood_texts = {
        1: "😢 Очень плохо. Берегите себя!",
        2: "🙁 Плохо. Надеюсь, скоро станет лучше!",
        3: "😐 Нормально. Это уже хорошо!",
        4: "🙂 Хорошо! Отличное настроение!",
        5: "😊 Отлично! Так держать!"
    }
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="mood")]]
    
    await query.edit_message_text(
        f"✅ {mood_texts[mood_score]}",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )

async def list_medicines(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    medicines = await db.get_medicines(user_id)
    
    if not medicines:
        text = "📋 *У вас нет добавленных лекарств*"
    else:
        text = "📋 *Ваши лекарства:*\n\n"
        for name, dosage in medicines:
            dosage_text = f" ({dosage})" if dosage else ""
            text += f"• {name}{dosage_text}\n"
    
    keyboard = [
        [InlineKeyboardButton("💊 Добавить лекарство", callback_data="add_medicine")],
        [InlineKeyboardButton("🔙 Назад", callback_data="start")]
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )

async def add_medicine_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text(
        "💊 *Введите название лекарства:*",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 Отмена", callback_data="start")
        ]]),
        parse_mode=ParseMode.MARKDOWN
    )
    return States.ADD_MEDICINE_NAME

async def add_medicine_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['medicine_name'] = update.message.text
    
    await update.message.reply_text(
        "💧 *Введите дозировку* (или отправьте /skip):",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⏭️ Пропустить", callback_data="skip_dosage"),
            InlineKeyboardButton("🔙 Отмена", callback_data="start")
        ]]),
        parse_mode=ParseMode.MARKDOWN
    )
    return States.ADD_MEDICINE_DOSAGE

async def add_medicine_dosage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = context.user_data.get('medicine_name', '')
    dosage = update.message.text
    
    await db.add_medicine(update.effective_user.id, name, dosage)
    
    await update.message.reply_text(
        f"✅ Лекарство *{name}* добавлено!",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🏠 Главная", callback_data="start")
        ]]),
        parse_mode=ParseMode.MARKDOWN
    )
    return ConversationHandler.END

async def skip_dosage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    name = context.user_data.get('medicine_name', '')
    await db.add_medicine(update.effective_user.id, name, None)
    
    await query.edit_message_text(
        f"✅ Лекарство *{name}* добавлено!",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🏠 Главная", callback_data="start")
        ]]),
        parse_mode=ParseMode.MARKDOWN
    )
    return ConversationHandler.END

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    
    if data == "start":
        await start(update, context)
    elif data == "about":
        await about(update, context)
    elif data == "mood":
        await mood(update, context)
    elif data.startswith("mood_"):
        await mood_select(update, context)
    elif data == "list_medicines":
        await list_medicines(update, context)
    elif data == "add_medicine":
        return await add_medicine_start(update, context)
    
    return States.MAIN_MENU

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "❌ Операция отменена",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🏠 Главная", callback_data="start")
        ]])
    )
    return ConversationHandler.END

def main():
    """Главная функция запуска"""
    token = os.getenv("BOT_TOKEN")
    
    if not token or token == "ВАШ_ТОКЕН_БОТА_СЮДА":
        print("❌ Ошибка: Не указан токен бота!")
        print("📝 Отредактируйте файл .env и добавьте токен от @BotFather")
        print("   nano .env")
        return
    
    print(f"🚀 Запуск ЛОР-Помощника...")
    print(f"📊 Версия: 1.0.0 (упрощенная для тестирования)")
    print(f"💾 База данных: SQLite")
    print("-" * 50)
    
    # Создание приложения
    app = Application.builder().token(token).build()
    
    # Conversation handler для добавления лекарства
    medicine_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(add_medicine_start, pattern="^add_medicine$")],
        states={
            States.ADD_MEDICINE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_medicine_name)],
            States.ADD_MEDICINE_DOSAGE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_medicine_dosage),
                CallbackQueryHandler(skip_dosage, pattern="^skip_dosage$")
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    
    # Добавление обработчиков
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(medicine_conv)
    
    # Запуск бота
    print("✅ Бот запущен! Нажмите Ctrl+C для остановки")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
EOF
    print_success "Тестовая версия bot.py создана"
else
    print_warning "Файл bot.py уже существует"
fi

# Создание вспомогательных скриптов
print_header "СОЗДАНИЕ ВСПОМОГАТЕЛЬНЫХ СКРИПТОВ"

# common.sh
cat > common.sh << 'EOF'
#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

print_header() {
    echo -e "\n${BLUE}══════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  $1${NC}"
    echo -e "${BLUE}══════════════════════════════════════════════════${NC}\n"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${CYAN}ℹ️  $1${NC}"
}

check_venv() {
    if [ ! -d "venv" ]; then
        print_error "Виртуальное окружение не найдено"
        return 1
    fi
    return 0
}

activate_venv() {
    if [ -f "venv/bin/activate" ]; then
        source venv/bin/activate
        return 0
    fi
    return 1
}

check_bot_running() {
    if pgrep -f "python.*bot.py" > /dev/null; then
        return 0
    fi
    return 1
}

get_bot_pid() {
    pgrep -f "python.*bot.py" | head -1
}
EOF

# start.sh
cat > start.sh << 'EOF'
#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

print_header "ЗАПУСК ЛОР-ПОМОЩНИКА"

check_venv || exit 1
activate_venv || exit 1

if [ ! -f ".env" ]; then
    print_error "Файл .env не найден"
    exit 1
fi

# Проверка токена
if grep -q "ВАШ_ТОКЕН_БОТА_СЮДА" .env || ! grep -q "BOT_TOKEN=." .env; then
    print_error "В файле .env не указан токен бота!"
    print_info "Отредактируйте файл .env: nano .env"
    exit 1
fi

if check_bot_running; then
    print_warning "Бот уже запущен (PID: $(get_bot_pid))"
    exit 0
fi

print_success "Запуск бота..."
python bot.py
EOF

# stop.sh
cat > stop.sh << 'EOF'
#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

print_header "ОСТАНОВКА ЛОР-ПОМОЩНИКА"

if ! check_bot_running; then
    print_warning "Бот не запущен"
    exit 0
fi

PID=$(get_bot_pid)
print_info "Остановка процесса PID: $PID"

kill $PID 2>/dev/null
sleep 2

if check_bot_running; then
    print_warning "Принудительная остановка..."
    kill -9 $PID 2>/dev/null
    sleep 1
fi

if check_bot_running; then
    print_error "Не удалось остановить бота"
else
    print_success "Бот остановлен"
fi
EOF

# status.sh
cat > status.sh << 'EOF'
#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

print_header "СТАТУС ЛОР-ПОМОЩНИКА"

if check_bot_running; then
    PID=$(get_bot_pid)
    RUNTIME=$(ps -o etime= -p $PID 2>/dev/null | tr -d ' ')
    CPU=$(ps -o %cpu= -p $PID 2>/dev/null | tr -d ' ')
    MEM=$(ps -o %mem= -p $PID 2>/dev/null | tr -d ' ')
    
    echo -e "${GREEN}✅ Бот запущен${NC}"
    echo -e "  📊 PID: $PID"
    echo -e "  ⏱️  Время работы: ${RUNTIME:-N/A}"
    echo -e "  💻 CPU: ${CPU:-N/A}%"
    echo -e "  🧮 RAM: ${MEM:-N/A}%"
    
    if [ -f "logs/info.log" ]; then
        ERRORS=$(grep -c "ERROR" logs/info.log 2>/dev/null || echo "0")
        echo -e "  📝 Ошибок в логах: $ERRORS"
    fi
else
    echo -e "${RED}❌ Бот не запущен${NC}"
fi

# Проверка базы данных
if [ -f "lor_reminder.db" ]; then
    DB_SIZE=$(ls -lh lor_reminder.db | awk '{print $5}')
    echo -e "  🗄️  База данных: $DB_SIZE"
fi
EOF

# logs.sh
cat > logs.sh << 'EOF'
#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

LINES=${1:-50}
FOLLOW=false

if [ "$1" == "-f" ] || [ "$2" == "-f" ]; then
    FOLLOW=true
fi

print_header "ПРОСМОТР ЛОГОВ"

if [ ! -f "logs/info.log" ]; then
    mkdir -p logs
    print_warning "Файл логов будет создан при запуске бота"
    exit 0
fi

if [ "$FOLLOW" = true ]; then
    tail -f logs/info.log
else
    tail -n $LINES logs/info.log | while IFS= read -r line; do
        if [[ $line == *"ERROR"* ]] || [[ $line == *"CRITICAL"* ]]; then
            echo -e "${RED}$line${NC}"
        elif [[ $line == *"WARNING"* ]]; then
            echo -e "${YELLOW}$line${NC}"
        elif [[ $line == *"INFO"* ]]; then
            echo -e "${GREEN}$line${NC}"
        else
            echo "$line"
        fi
    done
fi
EOF

# backup.sh
cat > backup.sh << 'EOF'
#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

BACKUP_DIR="backups"
DATE=$(date +%Y%m%d_%H%M%S)
RETENTION_DAYS=7

print_header "РЕЗЕРВНОЕ КОПИРОВАНИЕ"

mkdir -p "$BACKUP_DIR"

# Бэкап SQLite базы данных
if [ -f "lor_reminder.db" ]; then
    print_info "Создание бэкапа базы данных..."
    cp lor_reminder.db "$BACKUP_DIR/lor_reminder_$DATE.db"
    gzip "$BACKUP_DIR/lor_reminder_$DATE.db"
    print_success "Бэкап БД: $BACKUP_DIR/lor_reminder_$DATE.db.gz"
fi

# Бэкап конфигурации
if [ -f ".env" ]; then
    cp .env "$BACKUP_DIR/env_$DATE.bak"
    print_success "Конфигурация сохранена"
fi

# Бэкап логов
if [ -d "logs" ]; then
    tar -czf "$BACKUP_DIR/logs_$DATE.tar.gz" logs/ 2>/dev/null
    print_success "Логи сохранены"
fi

# Удаление старых бэкапов
find "$BACKUP_DIR" -name "*.gz" -mtime +$RETENTION_DAYS -delete
find "$BACKUP_DIR" -name "*.bak" -mtime +$RETENTION_DAYS -delete

BACKUP_COUNT=$(find "$BACKUP_DIR" -name "*.gz" | wc -l)
print_success "Всего бэкапов: $BACKUP_COUNT"
EOF

# monitor.sh
cat > monitor.sh << 'EOF'
#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

print_header "МОНИТОРИНГ СИСТЕМЫ"

# Информация о системе
echo -e "${CYAN}📊 СИСТЕМА:${NC}"
echo -e "  🖥️  Хост: $(hostname)"
echo -e "  🕒 Время: $(date)"
echo -e "  📊 Нагрузка: $(uptime | awk '{print $10 $11 $12}')"
echo

# Информация о боте
echo -e "${CYAN}🤖 БОТ:${NC}"
if check_bot_running; then
    PID=$(get_bot_pid)
    CPU=$(ps -p $PID -o %cpu= 2>/dev/null | tr -d ' ' || echo "N/A")
    MEM=$(ps -p $PID -o %mem= 2>/dev/null | tr -d ' ' || echo "N/A")
    RUNTIME=$(ps -p $PID -o etime= 2>/dev/null | tr -d ' ' || echo "N/A")
    
    echo -e "  ✅ Статус: ${GREEN}Работает${NC}"
    echo -e "  📊 PID: $PID"
    echo -e "  💻 CPU: $CPU%"
    echo -e "  🧮 RAM: $MEM%"
    echo -e "  ⏱️  Время: $RUNTIME"
else
    echo -e "  ❌ Статус: ${RED}Не работает${NC}"
fi
echo

# Информация о базе данных
echo -e "${CYAN}🗄️  БАЗА ДАННЫХ:${NC}"
if [ -f "lor_reminder.db" ]; then
    DB_SIZE=$(ls -lh lor_reminder.db | awk '{print $5}')
    DB_MODIFIED=$(date -r lor_reminder.db "+%d.%m.%Y %H:%M:%S")
    echo -e "  ✅ Файл: lor_reminder.db"
    echo -e "  📦 Размер: $DB_SIZE"
    echo -e "  🕒 Изменен: $DB_MODIFIED"
    
    # Проверка целостности SQLite
    if command -v sqlite3 &> /dev/null; then
        if sqlite3 lor_reminder.db "PRAGMA integrity_check;" 2>/dev/null | grep -q "ok"; then
            echo -e "  ✅ Целостность: OK"
        else
            echo -e "  ❌ Целостность: ${RED}Нарушена${NC}"
        fi
    fi
else
    echo -e "  ❌ Файл не найден"
fi
echo

# Информация о диске
echo -e "${CYAN}💿 ДИСК:${NC}"
DISK_USAGE=$(df -h . | awk 'NR==2 {print $5}')
DISK_AVAIL=$(df -h . | awk 'NR==2 {print $4}')
echo -e "  📊 Использовано: $DISK_USAGE"
echo -e "  💿 Доступно: $DISK_AVAIL"

# Сохранение отчета
REPORT_FILE="logs/monitor_$(date +%Y%m%d_%H%M%S).log"
mkdir -p logs
{
    echo "Мониторинг от $(date)"
    echo "================================"
    echo "Бот: $(check_bot_running && echo 'Работает' || echo 'Не работает')"
    echo "База данных: $(ls -lh lor_reminder.db 2>/dev/null | awk '{print $5}')"
    echo "Диск: $DISK_USAGE занято"
} > "$REPORT_FILE"

echo
print_success "Отчет сохранен в $REPORT_FILE"
EOF

# Делаем все скрипты исполняемыми
chmod +x common.sh start.sh stop.sh status.sh logs.sh backup.sh monitor.sh
print_success "Скрипты созданы и настроены"

# Создание директории для логов
mkdir -p logs
print_success "Директория для логов создана"

# Финальное сообщение
print_header "УСТАНОВКА ЗАВЕРШЕНА!"

echo -e "${GREEN}✅ ЛОР-Помощник успешно установлен для Codespaces!${NC}\n"

echo -e "${YELLOW}📋 НЕОБХОДИМЫЕ ДЕЙСТВИЯ:${NC}"
echo -e "  1. ${CYAN}Получите токен бота у @BotFather${NC}"
echo -e "  2. ${CYAN}Отредактируйте файл .env:${NC} nano .env"
echo -e "     Вставьте токен в поле BOT_TOKEN\n"

echo -e "  3. ${CYAN}Запустите бота:${NC} ./start.sh\n"

echo -e "${YELLOW}📊 ДОСТУПНЫЕ КОМАНДЫ:${NC}"
echo -e "  ${GREEN}./start.sh${NC}     - Запуск бота"
echo -e "  ${GREEN}./stop.sh${NC}      - Остановка бота"
echo -e "  ${GREEN}./status.sh${NC}    - Статус бота"
echo -e "  ${GREEN}./logs.sh${NC}      - Просмотр логов"
echo -e "  ${GREEN}./logs.sh -f${NC}    - Логи в реальном времени"
echo -e "  ${GREEN}./backup.sh${NC}     - Резервное копирование"
echo -e "  ${GREEN}./monitor.sh${NC}    - Мониторинг системы"

echo -e "\n${YELLOW}📝 ВАЖНО:${NC}"
echo -e "  • В Codespaces используется SQLite вместо PostgreSQL"
echo -e "  • База данных хранится в файле lor_reminder.db"
echo -e "  • Все логи сохраняются в директории logs/"
echo -e "  • Резервные копии в директории backups/"

echo -e "\n${GREEN}Удачного использования! 🚀${NC}"
