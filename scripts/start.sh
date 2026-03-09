#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

print_header "ЗАПУСК ЛОР-ПОМОЩНИКА"

if [ ! -d "venv" ]; then
    print_info "Создание виртуального окружения..."
    python3 -m venv venv
fi

activate_venv || exit 1

# Проверка наличия bot.py
if [ ! -f "bot.py" ]; then
    print_error "Файл bot.py не найден!"
    exit 1
fi

# Проверка токена в .env
if ! grep -q "BOT_TOKEN=" .env || grep -q "your_bot_token_here" .env; then
    print_error "В файле .env не указан корректный токен бота!"
    print_info "Отредактируйте файл .env: nano .env"
    exit 1
fi

if check_bot_running; then
    print_warning "Бот уже запущен (PID: $(get_bot_pid))"
    exit 0
fi

print_success "Запуск бота..."
python bot.py
