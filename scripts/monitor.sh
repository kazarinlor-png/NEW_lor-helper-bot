#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}📊 Мониторинг ЛОР-Помощника${NC}"
echo "========================================"

# Статус контейнеров
echo -e "\n${YELLOW}🔍 Статус контейнеров:${NC}"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep lor-bot

# Использование ресурсов
echo -e "\n${YELLOW}📈 Использование ресурсов:${NC}"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" | grep lor-bot

# Логи за последние 10 минут
echo -e "\n${YELLOW}📝 Последние ошибки в логах:${NC}"
docker-compose logs --tail=50 bot | grep -i error || echo "Ошибок не найдено"

# Проверка базы данных
echo -e "\n${YELLOW}🗄️  Статус базы данных:${NC}"
docker exec lor-bot-postgres pg_isready -U lor_bot && echo "✅ PostgreSQL работает" || echo "❌ PostgreSQL не отвечает"

# Проверка Redis
echo -e "\n${YELLOW}💾 Статус Redis:${NC}"
docker exec lor-bot-redis redis-cli ping | grep -q PONG && echo "✅ Redis работает" || echo "❌ Redis не отвечает"

# Метрики Prometheus
echo -e "\n${YELLOW}📊 Метрики Prometheus:${NC}"
curl -s http://localhost:9090/api/v1/query?query=up | grep -q "success" && echo "✅ Prometheus доступен" || echo "❌ Prometheus не отвечает"

# Активные подписки
echo -e "\n${YELLOW}💳 Активные подписки:${NC}"
docker exec lor-bot-postgres psql -U lor_bot -d lor_bot -c "
    SELECT plan_code, COUNT(*) as count 
    FROM user_subscriptions 
    WHERE status = 'active' 
    GROUP BY plan_code
    ORDER BY count DESC;
"

# Пользователи онлайн за последний час
echo -e "\n${YELLOW}👥 Пользователи онлайн:${NC}"
docker exec lor-bot-postgres psql -U lor_bot -d lor_bot -c "
    SELECT COUNT(*) 
    FROM users 
    WHERE last_seen > NOW() - INTERVAL '1 hour';
"

echo "========================================"
