# Обновить start.sh с проверкой venv
cat > start.sh << 'EOF'
#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}🚀 Запуск ЛОР-Помощника...${NC}"

# Проверка наличия виртуального окружения
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}📦 Виртуальное окружение не найдено. Создаю...${NC}"
    python3 -m venv venv
    source venv/bin/activate
    echo -e "${YELLOW}📦 Установка зависимостей...${NC}"
    pip install --upgrade pip
    pip install pytz python-telegram-bot apscheduler sqlalchemy alembic redis celery prometheus-client aiofiles aiosqlite async-timeout tenacity backoff python-dotenv requests
else
    source venv/bin/activate
fi

# Загрузка токена из .env если есть
if [ -f ".env" ]; then
    export $(cat .env | grep -v '^#' | xargs)
else
    export BOT_TOKEN=7374353352:AAHuRJALtv1JwcTdi2VMlziCM25KiLTwb80
    export ADMIN_IDS=123456789
fi

# Проверка установки pytz
python -c "import pytz" 2>/dev/null
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ pytz не установлен. Устанавливаю...${NC}"
    pip install pytz
fi

echo -e "${GREEN}✅ Запуск бота...${NC}"
python bot.py
EOF

# Сделать исполняемым
chmod +x start.sh
