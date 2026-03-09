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
if command -v psql &> /dev/null; then
    echo -e "\n${CYAN}🗄️  PostgreSQL:${NC}"
    if pg_isready -q 2>/dev/null; then
        echo -e "  ✅ Статус: ${GREEN}работает${NC}"
    else
        echo -e "  ❌ Статус: ${RED}не работает${NC}"
    fi
fi

# Проверка Redis
if command -v redis-cli &> /dev/null; then
    echo -e "\n${CYAN}💾 Redis:${NC}"
    if redis-cli ping &>/dev/null; then
        echo -e "  ✅ Статус: ${GREEN}работает${NC}"
    else
        echo -e "  ❌ Статус: ${RED}не работает${NC}"
    fi
fi
