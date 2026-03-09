#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

print_header "ОСТАНОВКА ЛОР-ПОМОЩНИКА"

if ! check_bot_running; then
    print_warning "Бот не запущен"
    exit 0
fi

PID=$(get_bot_pid)
print_info "Остановка процесса PID: $PID"

kill $PID 2>/dev/null
sleep 2

if check_bot_running; then
    print_warning "Принудительная остановка..."
    kill -9 $PID 2>/dev/null
    sleep 1
fi

if check_bot_running; then
    print_error "Не удалось остановить бота"
else
    print_success "Бот остановлен"
fi
