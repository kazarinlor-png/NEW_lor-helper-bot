FROM python:3.11-slim

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    libffi-dev \
    libssl-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Создание пользователя для бота
RUN useradd -m -u 1000 botuser && mkdir -p /app && chown botuser:botuser /app

# Установка рабочей директории
WORKDIR /app

# Копирование файла зависимостей
COPY requirements.txt .

# Установка Python зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода бота
COPY bot.py .
COPY init_db.py .

# Создание директорий для логов и бэкапов
RUN mkdir -p /app/logs /app/backups && \
    chown -R botuser:botuser /app

# Переключение на непривилегированного пользователя
USER botuser

# Запуск бота
CMD ["python", "bot.py"]

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import socket; s = socket.socket(); s.connect(('localhost', 9090))" || exit 1
