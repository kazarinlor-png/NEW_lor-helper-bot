#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}🛑 Остановка ЛОР-Помощника...${NC}"

# Остановка контейнеров
docker-compose down

# Проверка, что все остановлено
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Бот остановлен${NC}"
else
    echo -e "${RED}❌ Ошибка при остановке${NC}"
    exit 1
fi

# Проверка запущенных контейнеров
RUNNING=$(docker ps | grep lor-bot | wc -l)
if [ $RUNNING -eq 0 ]; then
    echo -e "${GREEN}✅ Все контейнеры остановлены${NC}"
else
    echo -e "${RED}⚠️ Некоторые контейнеры все еще запущены${NC}"
    docker ps | grep lor-bot
fi
