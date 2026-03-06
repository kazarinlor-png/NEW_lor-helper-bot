#!/bin/bash

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Запуск ЛОР-Помощника${NC}"
echo -e "${GREEN}========================================${NC}"

# Проверка наличия Docker и Docker Compose
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker не установлен${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ Docker Compose не установлен${NC}"
    exit 1
fi

# Проверка файла .env
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}📝 Файл .env не найден. Создаю из шаблона...${NC}"
    cp .env.example .env
    echo -e "${RED}⚠️  Отредактируйте файл .env и добавьте реальные значения!${NC}"
    echo -e "${YELLOW}   nano .env${NC}"
    exit 1
fi

# Проверка обязательных переменных
source .env
if [ -z "$BOT_TOKEN" ] || [ "$BOT_TOKEN" == "your_bot_token_here" ]; then
    echo -e "${RED}❌ BOT_TOKEN не установлен в .env${NC}"
    exit 1
fi

# Создание необходимых директорий
mkdir -p logs backups ssl

# Остановка старых контейнеров
echo -e "${YELLOW}🛑 Остановка старых контейнеров...${NC}"
docker-compose down

# Сборка и запуск
echo -e "${YELLOW}🚀 Сборка и запуск контейнеров...${NC}"
docker-compose up -d --build

# Проверка статуса
echo -e "${YELLOW}🔍 Проверка статуса...${NC}"
sleep 5
docker-compose ps

# Проверка логов
echo -e "${YELLOW}📋 Последние логи бота:${NC}"
docker-compose logs --tail=20 bot

echo -e "${GREEN}✅ Бот запущен!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "📊 Мониторинг: http://localhost:3000 (admin/admin)"
echo -e "📈 Метрики: http://localhost:9090"
echo -e "📝 Логи: docker-compose logs -f"
echo -e "${GREEN}========================================${NC}"
