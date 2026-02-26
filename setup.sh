cat > setup.sh << 'EOF'
#!/bin/bash

echo "🚀 Начинаем установку ЛОР-Помощника..."
echo "========================================"

# Создание виртуального окружения
echo "📦 Создание виртуального окружения..."
python3 -m venv venv

# Активация
source venv/bin/activate

# Обновление pip
echo "📦 Обновление pip..."
pip install --upgrade pip

# Установка зависимостей
echo "📦 Установка зависимостей (это может занять несколько минут)..."
pip install pytz python-telegram-bot apscheduler sqlalchemy alembic redis celery prometheus-client aiofiles aiosqlite async-timeout tenacity backoff python-dotenv requests

# Создание файла .env
echo "📝 Создание файла .env..."
cat > .env << ENV_EOF
BOT_TOKEN=7374353352:AAHuRJALtv1JwcTdi2VMlziCM25KiLTwb80
ADMIN_IDS=123456789
DATABASE_URL=sqlite+aiosqlite:///lor_reminder.db
REDIS_URL=redis://localhost:6379/0
ENV_EOF

echo "✅ Установка завершена!"
echo "👉 Для запуска бота используйте: ./start.sh"
EOF

chmod +x setup.sh
