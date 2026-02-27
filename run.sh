#!/bin/bash

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  ЛОР-Помощник - Запуск бота${NC}"
echo -e "${GREEN}========================================${NC}"

# Проверяем, существует ли виртуальное окружение
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}📦 Виртуальное окружение не найдено. Создаю...${NC}"
    python3 -m venv venv
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Ошибка при создании виртуального окружения${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Виртуальное окружение создано${NC}"
    
    # Активируем и устанавливаем зависимости
    echo -e "${YELLOW}📦 Устанавливаю зависимости...${NC}"
    source venv/bin/activate
    pip install --upgrade pip
    pip install pytz aiofiles aiosqlite python-telegram-bot==20.7 apscheduler==3.10.4 sqlalchemy redis celery prometheus-client backoff tenacity python-dotenv requests async-timeout
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Ошибка при установке зависимостей${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Зависимости установлены${NC}"
    
    # Создаем таблицы в базе данных
    echo -e "${YELLOW}🗄️  Создаю таблицы в базе данных...${NC}"
    python init_db_sync.py
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Ошибка при создании таблиц${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Таблицы созданы${NC}"
else
    echo -e "${GREEN}✅ Виртуальное окружение найдено${NC}"
    source venv/bin/activate
fi

# Проверяем наличие файла .env
if [ ! -f ".env" ]; then
    echo -e "${RED}❌ Файл .env не найден!${NC}"
    echo -e "${YELLOW}Создайте файл .env с токеном бота:${NC}"
    echo "BOT_TOKEN=ваш_токен_сюда" > .env.example
    echo -e "${YELLOW}Пример создан в .env.example${NC}"
    echo -e "${YELLOW}Выполните: cp .env.example .env && nano .env${NC}"
    exit 1
fi

# Проверяем наличие токена в .env
if ! grep -q "BOT_TOKEN=." .env; then
    echo -e "${RED}❌ В файле .env не указан токен!${NC}"
    echo -e "${YELLOW}Отредактируйте файл .env и добавьте токен:${NC}"
    echo "BOT_TOKEN=ваш_настоящий_токен"
    exit 1
fi

echo -e "${GREEN}✅ Все проверки пройдены${NC}"
echo -e "${YELLOW}🚀 Запускаю бота...${NC}"
echo -e "${GREEN}========================================${NC}"

# Запускаем бота
python bot.py

# Если бот остановился, деактивируем окружение
deactivate
