#!/bin/bash

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  ЛОР-Помощник - Автоматический запуск${NC}"
echo -e "${GREEN}========================================${NC}"

# Функция проверки и установки зависимостей
check_dependencies() {
    echo -e "${YELLOW}🔍 Проверка зависимостей...${NC}"
    
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}❌ Python3 не установлен!${NC}"
        exit 1
    fi
    
    if [ ! -d "venv" ]; then
        echo -e "${YELLOW}📦 Создание виртуального окружения...${NC}"
        python3 -m venv venv
    fi
    
    source venv/bin/activate
    
    # Список необходимых пакетов
    PACKAGES=("pytz" "aiofiles" "aiosqlite" "python-telegram-bot" "apscheduler" "sqlalchemy" "redis" "celery" "prometheus-client" "backoff" "tenacity" "python-dotenv" "requests" "async-timeout")
    
    MISSING_PACKAGES=()
    for package in "${PACKAGES[@]}"; do
        if ! pip list | grep -q "$package"; then
            MISSING_PACKAGES+=("$package")
        fi
    done
    
    if [ ${#MISSING_PACKAGES[@]} -gt 0 ]; then
        echo -e "${YELLOW}📦 Установка отсутствующих пакетов...${NC}"
        pip install --upgrade pip
        pip install "${MISSING_PACKAGES[@]}"
    else
        echo -e "${GREEN}✅ Все зависимости установлены${NC}"
    fi
}

# Функция проверки базы данных
check_database() {
    echo -e "${YELLOW}🔍 Проверка базы данных...${NC}"
    
    if [ ! -f "lor_reminder.db" ]; then
        echo -e "${YELLOW}🗄️  Создание таблиц в базе данных...${NC}"
        python init_db_sync.py
    else
        # Проверяем наличие таблицы reminders
        if ! sqlite3 lor_reminder.db ".tables" | grep -q "reminders"; then
            echo -e "${YELLOW}🗄️  Обновление структуры базы данных...${NC}"
            python init_db_sync.py
        else
            echo -e "${GREEN}✅ База данных в порядке${NC}"
        fi
    fi
}

# Функция проверки файла .env
check_env() {
    echo -e "${YELLOW}🔍 Проверка файла .env...${NC}"
    
    if [ ! -f ".env" ]; then
        echo -e "${RED}❌ Файл .env не найден!${NC}"
        echo -e "${YELLOW}Создаю шаблон .env...${NC}"
        echo "# Токен бота (получить у @BotFather)" > .env
        echo "BOT_TOKEN=ваш_токен_сюда" >> .env
        echo -e "${RED}⚠️  Отредактируйте файл .env и добавьте реальный токен!${NC}"
        echo -e "${YELLOW}   nano .env${NC}"
        exit 1
    fi
    
    if ! grep -q "BOT_TOKEN=." .env || grep -q "BOT_TOKEN=ваш_токен_сюда" .env; then
        echo -e "${RED}❌ В файле .env не указан корректный токен!${NC}"
        echo -e "${YELLOW}Отредактируйте файл .env:${NC}"
        echo "   nano .env"
        exit 1
    fi
    
    echo -e "${GREEN}✅ Файл .env в порядке${NC}"
}

# Функция запуска бота
run_bot() {
    echo -e "${GREEN}✅ Все проверки пройдены${NC}"
    echo -e "${YELLOW}🚀 Запуск бота...${NC}"
    echo -e "${GREEN}========================================${NC}"
    
    # Загружаем переменные из .env
    export $(grep -v '^#' .env | xargs)
    
    # Запускаем бота
    python bot.py
    
    # Если бот упал с ошибкой
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Бот аварийно завершил работу!${NC}"
        return 1
    fi
}

# Основной цикл с автоматическим перезапуском
RESTART_COUNT=0
MAX_RESTARTS=5

while true; do
    # Проверяем все компоненты
    check_dependencies
    check_database
    check_env
    
    # Запускаем бота
    run_bot
    
    # Если бот завершился нормально (не ошибка)
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Бот завершил работу нормально${NC}"
        break
    fi
    
    # Считаем количество перезапусков
    RESTART_COUNT=$((RESTART_COUNT + 1))
    
    if [ $RESTART_COUNT -ge $MAX_RESTARTS ]; then
        echo -e "${RED}❌ Слишком много перезапусков ($MAX_RESTARTS). Останавливаюсь.${NC}"
        break
    fi
    
    # Ждем перед перезапуском
    WAIT_TIME=$((RESTART_COUNT * 5))
    echo -e "${YELLOW}⚠️  Перезапуск через $WAIT_TIME секунд... (попытка $RESTART_COUNT из $MAX_RESTARTS)${NC}"
    sleep $WAIT_TIME
done

deactivate 2>/dev/null
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Работа скрипта завершена${NC}"
echo -e "${GREEN}========================================${NC}"
