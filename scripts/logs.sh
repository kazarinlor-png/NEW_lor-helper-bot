#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

LINES=${1:-50}
FOLLOW=false

if [ "$1" == "-f" ] || [ "$2" == "-f" ]; then
    FOLLOW=true
fi

print_header "ПРОСМОТР ЛОГОВ"

mkdir -p logs

if [ ! -f "logs/info.log" ] && [ ! -f "logs/error.log" ]; then
    print_warning "Файлы логов еще не созданы"
    exit 0
fi

if [ "$FOLLOW" = true ]; then
    if [ -f "logs/info.log" ]; then
        tail -f logs/info.log
    else
        print_error "Файл логов не найден"
    fi
else
    if [ -f "logs/error.log" ]; then
        echo -e "${RED}❌ Последние ошибки:${NC}"
        tail -n $LINES logs/error.log 2>/dev/null | while IFS= read -r line; do
            echo -e "${RED}$line${NC}"
        done
        echo
    fi
    
    if [ -f "logs/info.log" ]; then
        echo -e "${GREEN}📝 Последние события:${NC}"
        tail -n $LINES logs/info.log 2>/dev/null | while IFS= read -r line; do
            if [[ $line == *"ERROR"* ]]; then
                echo -e "${RED}$line${NC}"
            elif [[ $line == *"WARNING"* ]]; then
                echo -e "${YELLOW}$line${NC}"
            else
                echo -e "${GREEN}$line${NC}"
            fi
        done
    fi
fi
