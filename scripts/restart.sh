#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}🔄 Перезапуск ЛОР-Помощника...${NC}"

# Остановка
./scripts/stop.sh

# Небольшая пауза
sleep 5

# Запуск
./scripts/start.sh

echo -e "${GREEN}✅ Перезапуск завершен${NC}"
