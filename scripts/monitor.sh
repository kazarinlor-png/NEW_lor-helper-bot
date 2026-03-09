#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

print_header "МОНИТОРИНГ СИСТЕМЫ"

# Информация о системе
echo -e "${CYAN}📊 СИСТЕМА:${NC}"
echo -e "  🖥️  Хост: $(hostname)"
echo -e "  🕒 Время: $(date)"
echo -e "  📊 Нагрузка: $(uptime | awk '{print $8 $9 $10}')"
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

# Информация о PostgreSQL
echo -e "${CYAN}🗄️  POSTGRESQL:${NC}"
if command -v psql &> /dev/null; then
    if pg_isready -q 2>/dev/null; then
        echo -e "  ✅ Статус: ${GREEN}Работает${NC}"
        
        # Размер базы данных
        if [ -f ".env" ]; then
            export $(grep -v '^#' .env | xargs)
            DB_SIZE=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT pg_size_pretty(pg_database_size('$DB_NAME'));" 2>/dev/null | tr -d ' ')
            CONNECTIONS=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT count(*) FROM pg_stat_activity;" 2>/dev/null | tr -d ' ')
            
            echo -e "  📦 Размер БД: ${DB_SIZE:-N/A}"
            echo -e "  🔌 Соединений: ${CONNECTIONS:-N/A}"
        fi
    else
        echo -e "  ❌ Статус: ${RED}Не работает${NC}"
    fi
fi
echo

# Информация о Redis
echo -e "${CYAN}💾 REDIS:${NC}"
if command -v redis-cli &> /dev/null; then
    if redis-cli ping &>/dev/null; then
        echo -e "  ✅ Статус: ${GREEN}Работает${NC}"
        
        REDIS_MEM=$(redis-cli info memory | grep "used_memory_human:" | cut -d: -f2 | tr -d ' ')
        REDIS_KEYS=$(redis-cli dbsize)
        
        echo -e "  💾 Память: ${REDIS_MEM:-N/A}"
        echo -e "  🔑 Ключей: ${REDIS_KEYS:-N/A}"
    else
        echo -e "  ❌ Статус: ${RED}Не работает${NC}"
    fi
fi
echo

# Информация о диске
echo -e "${CYAN}💿 ДИСК:${NC}"
DISK_USAGE=$(df -h . | awk 'NR==2 {print $5}')
DISK_AVAIL=$(df -h . | awk 'NR==2 {print $4}')
echo -e "  📊 Использовано: $DISK_USAGE"
echo -e "  💿 Доступно: $DISK_AVAIL"
