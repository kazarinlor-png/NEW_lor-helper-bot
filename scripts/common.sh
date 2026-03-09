#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

print_header() {
    echo -e "\n${BLUE}══════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  $1${NC}"
    echo -e "${BLUE}══════════════════════════════════════════════════${NC}\n"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${CYAN}ℹ️  $1${NC}"
}

check_venv() {
    if [ ! -d "venv" ]; then
        print_error "Виртуальное окружение не найдено"
        return 1
    fi
    return 0
}

activate_venv() {
    if [ -f "venv/bin/activate" ]; then
        source venv/bin/activate
        return 0
    fi
    return 1
}

check_bot_running() {
    if pgrep -f "python.*bot.py" > /dev/null; then
        return 0
    fi
    return 1
}

get_bot_pid() {
    pgrep -f "python.*bot.py" | head -1
}
