#!/bin/bash

# Конфигурация
BACKUP_DIR="/app/backups"
DB_HOST="postgres"
DB_NAME="lor_bot"
DB_USER="lor_bot"
DB_PASSWORD="${DB_PASSWORD}"
RETENTION_DAYS=30

# Создание директории для бэкапов
mkdir -p $BACKUP_DIR

# Имя файла с датой
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/lor_bot_$DATE.sql"
BACKUP_FILE_GZ="$BACKUP_FILE.gz"

echo "📦 Создание резервной копии базы данных..."

# Создание дампа базы данных
PGPASSWORD=$DB_PASSWORD pg_dump \
    -h $DB_HOST \
    -U $DB_USER \
    -d $DB_NAME \
    --clean \
    --if-exists \
    --no-owner \
    --no-privileges \
    > $BACKUP_FILE

if [ $? -eq 0 ]; then
    # Сжатие
    gzip $BACKUP_FILE
    echo "✅ Резервная копия создана: $BACKUP_FILE_GZ"
    
    # Удаление старых бэкапов
    find $BACKUP_DIR -name "lor_bot_*.sql.gz" -mtime +$RETENTION_DAYS -delete
    echo "🧹 Старые бэкапы удалены"
else
    echo "❌ Ошибка при создании резервной копии"
    exit 1
fi

# Создание бэкапа конфигурации
CONFIG_BACKUP="$BACKUP_DIR/config_$DATE.tar.gz"
tar -czf $CONFIG_BACKUP \
    .env \
    docker-compose.yml \
    config/ \
    2>/dev/null

if [ $? -eq 0 ]; then
    echo "✅ Конфигурация сохранена: $CONFIG_BACKUP"
else
    echo "⚠️ Не удалось создать бэкап конфигурации"
fi

# Отправка в облачное хранилище (опционально)
if [ ! -z "$AWS_ACCESS_KEY_ID" ]; then
    aws s3 cp $BACKUP_FILE_GZ s3://your-bucket/backups/
    echo "☁️ Бэкап отправлен в S3"
fi

echo "📊 Статистика бэкапов:"
ls -lh $BACKUP_DIR | grep sql.gz | tail -5
