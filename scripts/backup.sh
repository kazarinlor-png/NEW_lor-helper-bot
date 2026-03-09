#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

BACKUP_DIR="backups"
DATE=$(date +%Y%m%d_%H%M%S)
RETENTION_DAYS=7

print_header "РЕЗЕРВНОЕ КОПИРОВАНИЕ"

mkdir -p "$BACKUP_DIR"

# Загрузка переменных из .env
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Бэкап PostgreSQL
if command -v pg_dump &> /dev/null && [ ! -z "$DB_NAME" ]; then
    print_info "Создание бэкапа PostgreSQL..."
    PGPASSWORD="$DB_PASSWORD" pg_dump -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" > "$BACKUP_DIR/postgres_$DATE.sql" 2>/dev/null
    if [ $? -eq 0 ]; then
        gzip "$BACKUP_DIR/postgres_$DATE.sql"
        print_success "Бэкап PostgreSQL: $BACKUP_DIR/postgres_$DATE.sql.gz"
    else
        print_warning "Не удалось создать бэкап PostgreSQL"
    fi
fi

# Бэкап SQLite (если используется)
if [ -f "lor_reminder.db" ]; then
    print_info "Создание бэкапа SQLite..."
    cp lor_reminder.db "$BACKUP_DIR/lor_reminder_$DATE.db"
    gzip "$BACKUP_DIR/lor_reminder_$DATE.db"
    print_success "Бэкап SQLite: $BACKUP_DIR/lor_reminder_$DATE.db.gz"
fi

# Бэкап конфигурации
if [ -f ".env" ]; then
    cp .env "$BACKUP_DIR/env_$DATE.bak"
    print_success "Конфигурация сохранена"
fi

# Бэкап логов
if [ -d "logs" ]; then
    tar -czf "$BACKUP_DIR/logs_$DATE.tar.gz" logs/ 2>/dev/null
    print_success "Логи сохранены"
fi

# Удаление старых бэкапов
find "$BACKUP_DIR" -name "*.gz" -mtime +$RETENTION_DAYS -delete
find "$BACKUP_DIR" -name "*.bak" -mtime +$RETENTION_DAYS -delete

BACKUP_COUNT=$(find "$BACKUP_DIR" -name "*.gz" | wc -l)
print_success "Всего бэкапов: $BACKUP_COUNT"
