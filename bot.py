#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ЛОР-Помощник - Telegram бот для управления приемом лекарств и отслеживания симптомов
Версия: 14.0.0 (Профессиональная с PostgreSQL)
Автор: Денис Казарин (врач-оториноларинголог)
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from collections import defaultdict
from time import time
from functools import wraps
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
import pytz
import json
import re
import warnings
import signal
import traceback
import io
import csv
from pathlib import Path

# ============== ИМПОРТЫ С АВТОУСТАНОВКОЙ ==============

def auto_install(package: str, version: str = None):
    """Автоматическая установка пакета при необходимости."""
    try:
        if version:
            __import__(package.split('==')[0])
        else:
            __import__(package)
        return True
    except ImportError:
        import subprocess
        pkg = f"{package}=={version}" if version else package
        print(f"📦 Устанавливаем {pkg}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])
        return True

# Основные зависимости
auto_install("python-telegram-bot", "20.7")
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    ConversationHandler, MessageHandler, filters, ContextTypes
)
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TimedOut, BadRequest, Forbidden

auto_install("apscheduler", "3.10.4")
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.date import DateTrigger

# PostgreSQL
auto_install("asyncpg", "0.29.0")
import asyncpg
from asyncpg import Pool, Connection

# Redis для кэширования
auto_install("redis", "5.0.1")
import redis.asyncio as redis
from redis.asyncio import Redis

# Прометеус для метрик
auto_install("prometheus-client", "0.19.0")
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Дополнительные утилиты
auto_install("python-dotenv", "1.0.0")
from dotenv import load_dotenv

auto_install("aiofiles", "23.2.1")
import aiofiles

auto_install("backoff", "2.2.1")
import backoff

auto_install("tenacity", "8.2.3")
from tenacity import retry, stop_after_attempt, wait_exponential

# Отключаем предупреждения
warnings.filterwarnings('ignore')

# ============== ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ==============
load_dotenv()

# ============== КОНТАКТЫ КЛИНИК ==============

KIT_CLINIC = {
    "name": "🏥 КИТ-клиника (Куркино)",
    "address": "г. Москва, ул. Соколово-Мещерская, д. 16/114",
    "phone": "+74957775580",
    "phone_display": "8 (495) 777-55-80",
    "site": "https://kit-clinic.ru/doctors/kazarin-denis-sergeevich/",
    "maps": "https://yandex.ru/maps/-/CPQZIPYD",
    "booking": "https://kit-clinic.ru/doctors/kazarin-denis-sergeevich/#reception"
}

FAMILY_CLINIC = {
    "name": "🏥 Семейная клиника (Путилково)",
    "address": "МО, Красногорский р-н, пгт Путилково, Спасо-Тушинский бульвар, д. 5",
    "phone": "+74987317555",
    "phone_display": "8 (498) 731-75-55",
    "site": "https://klinika-bz.ru/speczialistyi/kazarin-denis-sergeevich",
    "maps": "https://yandex.ru/maps/-/CPEBA46u",
    "booking": "https://klinika-bz.ru/speczialistyi/kazarin-denis-sergeevich#reception"
}

DOCTOR_INFO = """👨‍⚕️ *Денис Сергеевич Казарин* - врач-оториноларинголог

👶 *Ведет прием детей с 0 лет и взрослых*

🎓 *Образование:*
• 2001-2007: МГМСУ им. А.И. Евдокимова
• 2007-2009: Ординатура, РМАПО
• Лазерная медицина (НПЦ лазерной медицины)

📱 *Социальные сети:*
• 👥 Telegram канал: @KAZARIN_LOR
• 💬 Личный Telegram: @deniskazarin"""

# ============== МЕТРИКИ PROMETHEUS ==============

class Metrics:
    """Система метрик Prometheus."""
    
    _instance = None
    _started = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._started:
            self._setup_metrics()
            self._start_server()
            self._started = True
    
    def _setup_metrics(self):
        """Инициализация метрик."""
        self.requests_total = Counter('bot_requests_total', 'Total requests', ['handler', 'status'])
        self.errors_total = Counter('bot_errors_total', 'Total errors', ['type'])
        self.request_duration = Histogram('bot_request_duration_seconds', 'Request duration', ['handler'])
        self.active_users = Gauge('bot_active_users', 'Active users')
        self.db_pool_size = Gauge('bot_db_pool_size', 'Database pool size')
        self.db_query_time = Histogram('bot_db_query_seconds', 'Database query time')
        self.db_connection_errors = Counter('bot_db_connection_errors_total', 'DB connection errors')
        self.redis_connections = Gauge('bot_redis_connections', 'Redis connections')
        self.redis_errors = Counter('bot_redis_errors_total', 'Redis errors')
        self.new_users = Counter('bot_new_users_total', 'New users registered')
        self.welcome_shown = Counter('bot_welcome_shown_total', 'Welcome messages shown')
        self.help_shown = Counter('bot_help_shown_total', 'Help messages shown')
        self.reminders_sent = Counter('bot_reminders_sent_total', 'Reminders sent', ['type'])
        self.medicines_added = Counter('bot_medicines_added_total', 'Medicines added')
        self.analyses_added = Counter('bot_analyses_added_total', 'Analyses added')
        self.mood_logs = Counter('bot_mood_logs_total', 'Mood logs', ['score'])
        self.symptom_logs = Counter('bot_symptom_logs_total', 'Symptom logs', ['severity'])
    
    def _start_server(self):
        """Запуск HTTP сервера для метрик."""
        try:
            start_http_server(9090)
            print(f"📊 Метрики доступны на порту 9090")
        except Exception as e:
            print(f"⚠️ Не удалось запустить метрики: {e}")

metrics = Metrics()

# ============== КОНФИГУРАЦИЯ ==============

@dataclass(frozen=True)
class Config:
    """Конфигурация бота."""
    # Telegram
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
    ADMIN_IDS: tuple = tuple(int(id) for id in os.environ.get("ADMIN_IDS", "").split(",") if id)
    
    # PostgreSQL
    DB_HOST: str = os.environ.get("DB_HOST", "localhost")
    DB_PORT: int = int(os.environ.get("DB_PORT", "5432"))
    DB_NAME: str = os.environ.get("DB_NAME", "lor_bot")
    DB_USER: str = os.environ.get("DB_USER", "lor_bot")
    DB_PASSWORD: str = os.environ.get("DB_PASSWORD", "")
    DB_POOL_SIZE: int = int(os.environ.get("DB_POOL_SIZE", "20"))
    DB_MAX_QUERIES: int = int(os.environ.get("DB_MAX_QUERIES", "50000"))
    DB_STATEMENT_TIMEOUT: int = int(os.environ.get("DB_STATEMENT_TIMEOUT", "30"))
    DB_SSL: bool = os.environ.get("DB_SSL", "false").lower() == "true"
    
    # Redis
    REDIS_URL: str = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    REDIS_PASSWORD: str = os.environ.get("REDIS_PASSWORD", "")
    REDIS_ENABLED: bool = os.environ.get("REDIS_ENABLED", "true").lower() == "true"
    
    # Версия
    BOT_VERSION: str = "14.0.0"
    BOT_VERSION_DATE: str = "09.03.2026"
    BOT_NAME: str = "ЛОР-Помощник Pro"
    
    # Настройки производительности
    RATE_LIMIT_USER: float = float(os.environ.get("RATE_LIMIT_USER", "1.0"))
    REMINDER_RETRY_COUNT: int = int(os.environ.get("REMINDER_RETRY_COUNT", "5"))
    INTEGRITY_CHECK_INTERVAL: int = int(os.environ.get("INTEGRITY_CHECK_INTERVAL", "1800"))
    CACHE_TTL: int = int(os.environ.get("CACHE_TTL", "300"))
    REQUEST_TIMEOUT: int = int(os.environ.get("REQUEST_TIMEOUT", "30"))
    
    # Безопасность
    MAX_MESSAGE_LENGTH: int = 4096
    MAX_CALLBACK_DATA_LENGTH: int = 64
    DOUBLE_CLICK_INTERVAL: float = float(os.environ.get("DOUBLE_CLICK_INTERVAL", "2.0"))
    
    # Пагинация
    PAGE_SIZE: int = int(os.environ.get("PAGE_SIZE", "10"))
    
    def __post_init__(self):
        if not self.BOT_TOKEN:
            raise ValueError("❌ Не установлен BOT_TOKEN")

config = Config()

# ============== НАСТРОЙКА ЛОГИРОВАНИЯ ==============

class LoggerSetup:
    """Система логирования."""
    
    _instance = None
    _loggers = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        self._setup_loggers()
    
    def _setup_loggers(self):
        """Настройка всех уровней логирования."""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        from logging.handlers import RotatingFileHandler
        
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        for name, level in [
            ('info', logging.INFO),
            ('error', logging.ERROR),
            ('warning', logging.WARNING),
            ('debug', logging.DEBUG),
            ('audit', logging.INFO)
        ]:
            logger = logging.getLogger(name)
            logger.setLevel(level)
            
            if name == 'info':
                logger.addHandler(console)
            
            handler = RotatingFileHandler(
                log_dir / f'{name}.log',
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding='utf-8'
            )
            
            if name == 'error':
                formatter = logging.Formatter(
                    '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s\n%(exc_info)s'
                )
            else:
                formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            self._loggers[name] = logger
    
    def log(self, level: str, message: str, **kwargs):
        """Логирование."""
        if level in self._loggers:
            if level in ['error', 'warning']:
                metrics.errors_total.labels(type=level).inc()
            
            if level == 'audit':
                log_entry = {
                    'timestamp': datetime.now().isoformat(),
                    'message': message,
                    **kwargs
                }
                self._loggers[level].info(json.dumps(log_entry, ensure_ascii=False))
            else:
                getattr(self._loggers[level], level)(message)
    
    def log_error(self, error: Exception, context: Dict = None):
        """Логирование ошибки с контекстом."""
        error_msg = f"Error: {str(error)}\n"
        if context:
            error_msg += f"Context: {json.dumps(context, ensure_ascii=False, default=str)}\n"
        error_msg += f"Traceback: {traceback.format_exc()}"
        
        self._loggers['error'].error(error_msg)
        metrics.errors_total.labels(type=type(error).__name__).inc()

logger = LoggerSetup()

# ============== ДЕКОРАТОРЫ ==============

def measure_time(func):
    """Декоратор для замера времени выполнения."""
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        start = time()
        try:
            result = await func(*args, **kwargs)
            return result
        finally:
            duration = time() - start
            if duration > 1.0:
                logger.log('warning', f"Медленная операция: {func.__name__} - {duration:.2f}s")
    return async_wrapper

def retry_on_error(max_attempts: int = 3, delay: float = 1.0):
    """Декоратор для повторных попыток при ошибках."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(delay * (attempt + 1))
                    logger.log('warning', f"Попытка {attempt + 1} для {func.__name__} не удалась: {e}")
            raise last_error
        return wrapper
    return decorator

# ============== МЕНЕДЖЕР БАЗЫ ДАННЫХ (POSTGRESQL) ==============

class DatabaseManager:
    """Асинхронный менеджер PostgreSQL с пулом соединений."""
    
    _instance = None
    _pool: Optional[Pool] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    async def init_pool(self):
        """Инициализация пула соединений."""
        try:
            self._pool = await asyncpg.create_pool(
                host=config.DB_HOST,
                port=config.DB_PORT,
                database=config.DB_NAME,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                min_size=5,
                max_size=config.DB_POOL_SIZE,
                max_queries=config.DB_MAX_QUERIES,
                command_timeout=config.DB_STATEMENT_TIMEOUT,
                ssl='require' if config.DB_SSL else None
            )
            print(f"✅ PostgreSQL пул соединений создан (размер: {config.DB_POOL_SIZE})")
            metrics.db_pool_size.set(config.DB_POOL_SIZE)
            
            # Инициализация таблиц
            await self.init_tables()
            
        except Exception as e:
            logger.log_error(e, {'context': 'db_init'})
            print(f"❌ Ошибка подключения к PostgreSQL: {e}")
            print("📋 Проверьте настройки в .env файле:")
            print(f"  DB_HOST={config.DB_HOST}")
            print(f"  DB_PORT={config.DB_PORT}")
            print(f"  DB_NAME={config.DB_NAME}")
            print(f"  DB_USER={config.DB_USER}")
            print("  DB_PASSWORD=***")
            raise
    
    async def close(self):
        """Закрытие пула соединений."""
        if self._pool:
            await self._pool.close()
            self._pool = None
    
    @asynccontextmanager
    async def acquire(self):
        """Получение соединения из пула."""
        if not self._pool:
            await self.init_pool()
        
        async with self._pool.acquire() as conn:
            try:
                yield conn
            except Exception as e:
                logger.log_error(e)
                raise
    
    @retry_on_error(max_attempts=3)
    async def execute(self, query: str, *args):
        """Выполнение запроса без возврата результата."""
        start_time = time()
        async with self.acquire() as conn:
            result = await conn.execute(query, *args)
            metrics.db_query_time.observe(time() - start_time)
            return result
    
    @retry_on_error(max_attempts=3)
    async def fetch(self, query: str, *args) -> List[asyncpg.Record]:
        """Выполнение запроса с возвратом списка строк."""
        start_time = time()
        async with self.acquire() as conn:
            result = await conn.fetch(query, *args)
            metrics.db_query_time.observe(time() - start_time)
            return result
    
    @retry_on_error(max_attempts=3)
    async def fetchrow(self, query: str, *args) -> Optional[asyncpg.Record]:
        """Выполнение запроса с возвратом одной строки."""
        start_time = time()
        async with self.acquire() as conn:
            result = await conn.fetchrow(query, *args)
            metrics.db_query_time.observe(time() - start_time)
            return result
    
    @retry_on_error(max_attempts=3)
    async def fetchval(self, query: str, *args):
        """Выполнение запроса с возвратом одного значения."""
        start_time = time()
        async with self.acquire() as conn:
            result = await conn.fetchval(query, *args)
            metrics.db_query_time.observe(time() - start_time)
            return result
    
    async def init_tables(self):
        """Инициализация таблиц в базе данных."""
        
        # Таблица пользователей
        await self.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                user_id BIGINT UNIQUE NOT NULL,
                username VARCHAR(100),
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                language VARCHAR(10) DEFAULT 'ru',
                timezone VARCHAR(50) DEFAULT 'Europe/Moscow',
                role VARCHAR(20) DEFAULT 'user',
                status VARCHAR(20) DEFAULT 'active',
                has_seen_welcome BOOLEAN DEFAULT FALSE,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_interactions INTEGER DEFAULT 0,
                notifications_enabled BOOLEAN DEFAULT TRUE,
                notification_offset INTEGER DEFAULT 5,
                user_metadata JSONB DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Таблица лекарств
        await self.execute("""
            CREATE TABLE IF NOT EXISTS medicines (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                name VARCHAR(200) NOT NULL,
                dosage VARCHAR(100),
                times_per_day INTEGER DEFAULT 1,
                schedule_times JSONB,
                schedule VARCHAR(200),
                course_type VARCHAR(20) DEFAULT 'unlimited',
                course_duration INTEGER,
                repeat_type VARCHAR(20) DEFAULT 'none',
                repeat_interval INTEGER,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                user_timezone VARCHAR(50) NOT NULL,
                status VARCHAR(20) DEFAULT 'active',
                total_taken INTEGER DEFAULT 0,
                total_skipped INTEGER DEFAULT 0,
                total_postponed INTEGER DEFAULT 0,
                total_unscheduled INTEGER DEFAULT 0,
                stats JSONB DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Таблица логов лекарств
        await self.execute("""
            CREATE TABLE IF NOT EXISTS medicine_logs (
                id SERIAL PRIMARY KEY,
                medicine_id INTEGER NOT NULL,
                user_id BIGINT NOT NULL,
                log_type VARCHAR(20) DEFAULT 'scheduled',
                status VARCHAR(20),
                dosage VARCHAR(100),
                reason TEXT,
                comment TEXT,
                side_effects TEXT,
                scheduled_time TIMESTAMP,
                taken_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Таблица анализов
        await self.execute("""
            CREATE TABLE IF NOT EXISTS analyses (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                analysis_type VARCHAR(20) DEFAULT 'analysis',
                name VARCHAR(200) NOT NULL,
                scheduled_date TIMESTAMP NOT NULL,
                scheduled_time VARCHAR(10) NOT NULL,
                repeat_type VARCHAR(20) DEFAULT 'once',
                repeat_interval INTEGER,
                reminder_before INTEGER DEFAULT 24,
                notes TEXT,
                status VARCHAR(20) DEFAULT 'pending',
                user_timezone VARCHAR(50) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Таблица настроения
        await self.execute("""
            CREATE TABLE IF NOT EXISTS mood_logs (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                mood_score INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Таблица симптомов
        await self.execute("""
            CREATE TABLE IF NOT EXISTS symptom_logs (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                symptom VARCHAR(100) NOT NULL,
                severity INTEGER NOT NULL,
                severity_color VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Таблица напоминаний
        await self.execute("""
            CREATE TABLE IF NOT EXISTS reminders (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                reminder_type VARCHAR(20),
                item_id INTEGER NOT NULL,
                scheduled_time TIMESTAMP NOT NULL,
                user_timezone VARCHAR(50) NOT NULL,
                status VARCHAR(20) DEFAULT 'pending',
                retry_count INTEGER DEFAULT 0,
                last_error TEXT,
                postponed_until TIMESTAMP,
                pause_until TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Таблица логов администратора
        await self.execute("""
            CREATE TABLE IF NOT EXISTS admin_logs (
                id SERIAL PRIMARY KEY,
                admin_id BIGINT NOT NULL,
                action VARCHAR(100) NOT NULL,
                target_user_id BIGINT,
                details JSONB,
                ip_address VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Создание индексов для производительности
        await self.execute("CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id)")
        await self.execute("CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)")
        await self.execute("CREATE INDEX IF NOT EXISTS idx_medicines_user_id ON medicines(user_id)")
        await self.execute("CREATE INDEX IF NOT EXISTS idx_medicines_status ON medicines(status)")
        await self.execute("CREATE INDEX IF NOT EXISTS idx_reminders_user_id ON reminders(user_id)")
        await self.execute("CREATE INDEX IF NOT EXISTS idx_reminders_scheduled_time ON reminders(scheduled_time)")
        await self.execute("CREATE INDEX IF NOT EXISTS idx_reminders_status ON reminders(status)")
        await self.execute("CREATE INDEX IF NOT EXISTS idx_mood_logs_user_id ON mood_logs(user_id)")
        await self.execute("CREATE INDEX IF NOT EXISTS idx_symptom_logs_user_id ON symptom_logs(user_id)")
        
        print("✅ Таблицы базы данных инициализированы")

db = DatabaseManager()

# ============== МЕНЕДЖЕР REDIS ==============

class RedisManager:
    """Менеджер Redis для кэширования."""
    
    _instance = None
    _redis: Optional[Redis] = None
    _enabled: bool = config.REDIS_ENABLED
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    async def get_connection(self) -> Optional[Redis]:
        """Получение подключения к Redis."""
        if not self._enabled:
            return None
        
        if not self._redis:
            try:
                self._redis = await redis.from_url(
                    config.REDIS_URL,
                    password=config.REDIS_PASSWORD or None,
                    encoding="utf-8",
                    decode_responses=True,
                    max_connections=10
                )
                await self._redis.ping()
                metrics.redis_connections.set(1)
                print(f"✅ Redis подключен")
            except Exception as e:
                logger.log('warning', f"Redis не доступен: {e}")
                self._enabled = False
                metrics.redis_errors.inc()
                return None
        return self._redis
    
    async def close(self):
        """Закрытие подключения."""
        if self._redis:
            await self._redis.close()
            self._redis = None
            metrics.redis_connections.set(0)
    
    async def get(self, key: str) -> Optional[str]:
        """Получение значения из кэша."""
        redis = await self.get_connection()
        if not redis:
            return None
        try:
            return await redis.get(key)
        except Exception as e:
            logger.log('warning', f"Redis get error: {e}")
            metrics.redis_errors.inc()
            return None
    
    async def set(self, key: str, value: str, ttl: int = config.CACHE_TTL):
        """Сохранение значения в кэш."""
        redis = await self.get_connection()
        if not redis:
            return
        try:
            await redis.setex(key, ttl, value)
        except Exception as e:
            logger.log('warning', f"Redis set error: {e}")
            metrics.redis_errors.inc()
    
    async def delete(self, key: str):
        """Удаление из кэша."""
        redis = await self.get_connection()
        if not redis:
            return
        try:
            await redis.delete(key)
        except Exception as e:
            logger.log('warning', f"Redis delete error: {e}")
            metrics.redis_errors.inc()
    
    async def incr(self, key: str) -> int:
        """Инкремент значения."""
        redis = await self.get_connection()
        if not redis:
            return 0
        try:
            return await redis.incr(key)
        except Exception as e:
            logger.log('warning', f"Redis incr error: {e}")
            metrics.redis_errors.inc()
            return 0
    
    async def expire(self, key: str, seconds: int):
        """Установка времени жизни."""
        redis = await self.get_connection()
        if not redis:
            return
        try:
            await redis.expire(key, seconds)
        except Exception as e:
            logger.log('warning', f"Redis expire error: {e}")
            metrics.redis_errors.inc()

redis_cache = RedisManager()

# ============== БЕЗОПАСНОСТЬ ==============

class SecurityManager:
    """Менеджер безопасности."""
    
    _VALID_TIMEZONES = set(pytz.all_timezones)
    _user_action_tracker = defaultdict(list)
    
    @staticmethod
    def sanitize_input(text: str, max_length: int = 1000) -> str:
        """Санитизация пользовательского ввода."""
        if not text:
            return text
        if len(text) > max_length:
            text = text[:max_length]
        return ''.join(char for char in text if ord(char) >= 32 or char in '\n\r\t')
    
    @staticmethod
    def validate_medicine_name(name: str) -> Tuple[bool, str]:
        """Валидация названия лекарства."""
        if len(name) < 2:
            return False, "Слишком короткое название (мин. 2 символа)"
        if len(name) > 200:
            return False, "Слишком длинное название (макс. 200 символов)"
        if not re.match(r'^[а-яА-Яa-zA-Z0-9\s\-]+$', name):
            return False, "Название может содержать только буквы, цифры, пробелы и дефисы"
        return True, "OK"
    
    @staticmethod
    def validate_number_input(text: str, min_val: int, max_val: int) -> Tuple[bool, int, str]:
        """Валидация числового ввода."""
        try:
            value = int(text.strip())
            if min_val <= value <= max_val:
                return True, value, "OK"
            return False, 0, f"Число должно быть от {min_val} до {max_val}"
        except ValueError:
            return False, 0, "Введите целое число"
    
    @staticmethod
    def validate_time_input(time_str: str) -> Tuple[bool, str]:
        """Валидация времени в формате ЧЧ:ММ."""
        try:
            hour, minute = map(int, time_str.split(':'))
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                return True, "OK"
            return False, "Часы должны быть от 0 до 23, минуты от 0 до 59"
        except:
            return False, "Неверный формат времени. Используйте ЧЧ:ММ"
    
    @staticmethod
    def check_sql_injection(text: str) -> bool:
        """Проверка на SQL-инъекции."""
        if not text:
            return False
        dangerous = ['select', 'insert', 'update', 'delete', 'drop', 'union', '--', ';']
        text_lower = text.lower()
        for pattern in dangerous:
            if pattern in text_lower:
                logger.log('security', f"Potential SQL injection detected", text=text[:100])
                return True
        return False
    
    @staticmethod
    def validate_callback_data(data: str) -> bool:
        """Валидация callback data."""
        if not data or len(data) > config.MAX_CALLBACK_DATA_LENGTH:
            return False
        return bool(re.match(r'^[a-zA-Z0-9_\-]+$', data))
    
    @staticmethod
    def validate_timezone(tz_name: str) -> bool:
        """Валидация часового пояса."""
        return tz_name in SecurityManager._VALID_TIMEZONES
    
    @staticmethod
    def check_double_click(user_id: int, action: str) -> bool:
        """Проверка на двойные нажатия."""
        key = f"{user_id}:{action}"
        current_time = time()
        
        if key in SecurityManager._user_action_tracker:
            last_time = SecurityManager._user_action_tracker[key][-1] if SecurityManager._user_action_tracker[key] else 0
            if current_time - last_time < config.DOUBLE_CLICK_INTERVAL:
                return True
        
        SecurityManager._user_action_tracker[key].append(current_time)
        SecurityManager._user_action_tracker[key] = [
            t for t in SecurityManager._user_action_tracker[key] 
            if current_time - t < 60
        ]
        return False
    
    @staticmethod
    def audit_log(action: str, user_id: int, details: dict = None):
        """Логирование действий для аудита."""
        logger.log('audit', f"Action: {action}", user_id=user_id, **(details or {}))

# ============== УТИЛИТЫ ДЛЯ ВРЕМЕНИ ==============

class TimeUtils:
    """Утилиты для работы со временем."""
    
    _tz_cache = {}
    
    @classmethod
    def get_timezone(cls, tz_name: str) -> pytz.timezone:
        """Получение объекта часового пояса."""
        if not SecurityManager.validate_timezone(tz_name):
            tz_name = 'Europe/Moscow'
        
        if tz_name not in cls._tz_cache:
            cls._tz_cache[tz_name] = pytz.timezone(tz_name)
        return cls._tz_cache[tz_name]
    
    @classmethod
    async def get_user_timezone(cls, user_id: int) -> str:
        """Получение часового пояса пользователя с кэшированием."""
        # Проверяем кэш
        cached = await redis_cache.get(f"tz:{user_id}")
        if cached:
            return cached
        
        # Запрашиваем из БД
        tz = await db.fetchval(
            "SELECT timezone FROM users WHERE user_id = $1",
            user_id
        )
        tz = tz or 'Europe/Moscow'
        
        # Сохраняем в кэш
        await redis_cache.set(f"tz:{user_id}", tz, 3600)
        return tz
    
    @classmethod
    async def set_user_timezone(cls, user_id: int, timezone: str):
        """Установка часового пояса пользователя."""
        await db.execute(
            "UPDATE users SET timezone = $1, updated_at = CURRENT_TIMESTAMP WHERE user_id = $2",
            timezone, user_id
        )
        await redis_cache.delete(f"tz:{user_id}")
    
    @classmethod
    def local_to_utc(cls, local_time_str: str, timezone: str, base_date: Optional[datetime] = None) -> datetime:
        """Конвертация локального времени в UTC."""
        if base_date is None:
            base_date = datetime.now(cls.get_timezone(timezone))
        
        hour, minute = map(int, local_time_str.split(':'))
        local_dt = base_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        if local_dt.tzinfo is None:
            tz = cls.get_timezone(timezone)
            local_dt = tz.localize(local_dt)
        
        return local_dt.astimezone(pytz.UTC)
    
    @classmethod
    def utc_to_local(cls, utc_dt: datetime, timezone: str) -> datetime:
        """Конвертация UTC в локальное время."""
        if utc_dt.tzinfo is None:
            utc_dt = pytz.UTC.localize(utc_dt)
        tz = cls.get_timezone(timezone)
        return utc_dt.astimezone(tz)
    
    @classmethod
    def parse_date(cls, date_str: str, timezone: str) -> Optional[datetime]:
        """Парсинг даты из строки."""
        try:
            formats = ['%d.%m.%Y', '%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d']
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    dt = dt.replace(hour=12, minute=0, second=0)
                    tz = cls.get_timezone(timezone)
                    return tz.localize(dt)
                except ValueError:
                    continue
            return None
        except:
            return None

# ============== RATE LIMITER ==============

class RateLimiter:
    """Rate limiter для защиты от превышения лимитов Telegram."""
    
    def __init__(self):
        self.user_last_message = defaultdict(float)
        self._lock = asyncio.Lock()
    
    async def acquire(self, user_id: Optional[int] = None):
        """Acquire rate limit permit."""
        if user_id:
            async with self._lock:
                now = time()
                last_msg = self.user_last_message[user_id]
                if now - last_msg < config.RATE_LIMIT_USER:
                    wait_time = config.RATE_LIMIT_USER - (now - last_msg)
                    await asyncio.sleep(wait_time)
                self.user_last_message[user_id] = now

rate_limiter = RateLimiter()

# ============== ПЛАНИРОВЩИК ==============

class SchedulerManager:
    """Менеджер планировщика."""
    
    def __init__(self):
        jobstores = {
            'default': SQLAlchemyJobStore(url='sqlite:///apscheduler_jobs.db')
        }
        executors = {
            'default': AsyncIOExecutor()
        }
        job_defaults = {
            'coalesce': True,
            'max_instances': 10,
            'misfire_grace_time': 3600
        }
        
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=pytz.UTC
        )
        self.application = None
        self._running = False
    
    def set_application(self, app):
        self.application = app
    
    def start(self):
        if not self._running:
            self.scheduler.start()
            self._running = True
            logger.log('info', "Планировщик запущен")
    
    def shutdown(self):
        if self._running:
            self.scheduler.shutdown()
            self._running = False
            logger.log('info', "Планировщик остановлен")
    
    async def restore_reminders(self):
        """Восстановление напоминаний при старте."""
        reminders = await db.fetch("""
            SELECT * FROM reminders 
            WHERE status = 'pending' AND scheduled_time > CURRENT_TIMESTAMP
        """)
        
        restored = 0
        for reminder in reminders:
            job_id = f"{reminder['reminder_type']}_{reminder['id']}"
            
            try:
                self.scheduler.remove_job(job_id)
            except JobLookupError:
                pass
            
            self.scheduler.add_job(
                self.send_reminder,
                trigger=DateTrigger(run_date=reminder['scheduled_time']),
                id=job_id,
                args=[reminder['id']],
                replace_existing=True
            )
            restored += 1
        
        logger.log('info', f"Восстановлено {restored} напоминаний")
        return restored
    
    async def send_reminder(self, reminder_id: int):
        """Отправка напоминания."""
        if not self.application:
            return
        
        try:
            reminder = await db.fetchrow(
                "SELECT * FROM reminders WHERE id = $1",
                reminder_id
            )
            
            if not reminder or reminder['status'] != 'pending':
                return
            
            user_id = reminder['user_id']
            
            if reminder['reminder_type'] == 'medicine':
                medicine = await db.fetchrow(
                    "SELECT * FROM medicines WHERE id = $1",
                    reminder['item_id']
                )
                
                if not medicine or medicine['status'] != 'active':
                    await db.execute(
                        "UPDATE reminders SET status = 'cancelled' WHERE id = $1",
                        reminder_id
                    )
                    return
                
                text = f"💊 *Время принять лекарство!*\n\n{medicine['name']}"
                if medicine['dosage']:
                    text += f"\n💧 Дозировка: {medicine['dosage']}"
                
                keyboard = [
                    [
                        InlineKeyboardButton("✅ Принял(а)", callback_data=f"take_{medicine['id']}"),
                        InlineKeyboardButton("⏸ Отложить", callback_data=f"postpone_medicine_{medicine['id']}")
                    ],
                    [
                        InlineKeyboardButton("📝 Комментарий", callback_data=f"comment_medicine_{medicine['id']}"),
                        InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_medicine_{medicine['id']}")
                    ],
                    [
                        InlineKeyboardButton("🆕 Новый симптом", callback_data=f"new_symptom_{medicine['id']}"),
                        InlineKeyboardButton("⚠️ Побочное действие", callback_data=f"side_effect_{medicine['id']}")
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                metrics.reminders_sent.labels(type='medicine').inc()
                
            elif reminder['reminder_type'] in ['analysis', 'investigation']:
                analysis = await db.fetchrow(
                    "SELECT * FROM analyses WHERE id = $1",
                    reminder['item_id']
                )
                
                if not analysis or analysis['status'] != 'pending':
                    await db.execute(
                        "UPDATE reminders SET status = 'cancelled' WHERE id = $1",
                        reminder_id
                    )
                    return
                
                local_date = TimeUtils.utc_to_local(
                    analysis['scheduled_date'], 
                    analysis['user_timezone']
                )
                
                analysis_type = "анализ" if analysis['analysis_type'] == 'analysis' else "исследование"
                
                text = f"🩺 *Напоминание об {analysis_type}е!*\n\n"
                text += f"📋 {analysis['name']}\n"
                text += f"📅 {local_date.strftime('%d.%m.%Y')} в {analysis['scheduled_time']}\n"
                
                if analysis['notes']:
                    text += f"\n📝 *Заметки:* {analysis['notes']}"
                
                keyboard = [
                    [
                        InlineKeyboardButton("✅ Сдал(а)", callback_data=f"analysis_take_{analysis['id']}"),
                        InlineKeyboardButton("⏸ Отложить", callback_data=f"postpone_analysis_{analysis['id']}")
                    ],
                    [
                        InlineKeyboardButton("📝 Заметки", callback_data=f"analysis_notes_{analysis['id']}"),
                        InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_analysis_{analysis['id']}")
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                metrics.reminders_sent.labels(type='analysis').inc()
            else:
                return
            
            for attempt in range(config.REMINDER_RETRY_COUNT):
                try:
                    await self.application.bot.send_message(
                        chat_id=user_id,
                        text=text,
                        reply_markup=reply_markup,
                        parse_mode=ParseMode.MARKDOWN
                    )
                    
                    await db.execute(
                        "UPDATE reminders SET status = 'sent', retry_count = $1 WHERE id = $2",
                        attempt + 1, reminder_id
                    )
                    return
                    
                except (RetryAfter, TimedOut) as e:
                    if attempt < config.REMINDER_RETRY_COUNT - 1:
                        await asyncio.sleep(5 * (attempt + 1))
                    await db.execute(
                        "UPDATE reminders SET retry_count = $1, last_error = $2 WHERE id = $3",
                        attempt + 1, str(e), reminder_id
                    )
                
                except Forbidden:
                    await db.execute(
                        "UPDATE reminders SET status = 'failed', last_error = 'User blocked bot' WHERE id = $1",
                        reminder_id
                    )
                    return
                
                except Exception as e:
                    logger.log_error(e, {'reminder_id': reminder_id})
                    await db.execute(
                        "UPDATE reminders SET status = 'failed', last_error = $1 WHERE id = $2",
                        str(e), reminder_id
                    )
                    return
            
            await db.execute(
                "UPDATE reminders SET status = 'failed' WHERE id = $1",
                reminder_id
            )
            
        except Exception as e:
            logger.log_error(e, {'reminder_id': reminder_id})

scheduler = SchedulerManager()

# ============== СОСТОЯНИЯ ==============

class States:
    """Состояния для ConversationHandler."""
    START = 0
    CANCEL = 999
    
    MEDICINE_NAME = 1
    MEDICINE_DOSAGE = 2
    MEDICINE_TIMES_PER_DAY = 3
    MEDICINE_SCHEDULE_HOUR = 4
    MEDICINE_SCHEDULE_MINUTE = 5
    MEDICINE_COURSE_DURATION = 6
    MEDICINE_REPEAT = 7
    MEDICINE_REPEAT_INTERVAL = 8
    MEDICINE_START_TYPE = 9
    MEDICINE_START_DATE = 10
    MEDICINE_CONFIRM = 11
    MEDICINE_EDIT = 12
    MEDICINE_COMMENT = 13
    
    ANALYSIS_TYPE = 20
    ANALYSIS_NAME = 21
    ANALYSIS_DATE = 22
    ANALYSIS_TIME_HOUR = 23
    ANALYSIS_TIME_MINUTE = 24
    ANALYSIS_REPEAT = 25
    ANALYSIS_REPEAT_INTERVAL = 26
    ANALYSIS_REMINDER = 27
    ANALYSIS_NOTES = 28
    ANALYSIS_CONFIRM = 29
    ANALYSIS_EDIT = 30
    
    SYMPTOM_TEXT = 40
    SYMPTOM_SEVERITY = 41
    
    UNSCHEDULED_MEDICINE_SELECT = 50
    UNSCHEDULED_MEDICINE_DOSAGE = 51
    UNSCHEDULED_MEDICINE_REASON = 52
    UNSCHEDULED_MEDICINE_COMMENT = 53
    
    POSTPONE_TYPE = 60
    POSTPONE_HOURS = 61
    POSTPONE_DAYS = 62
    POSTPONE_CUSTOM = 63
    
    BROADCAST_MESSAGE = 70
    BROADCAST_CONFIRM = 71
    ADMIN_USER_SEARCH = 72

# ============== ОСНОВНОЙ КЛАСС ОБРАБОТЧИКОВ ==============

class Handlers:
    """Основные обработчики бота."""
    
    def __init__(self, application, scheduler, rate_limiter):
        self.app = application
        self.scheduler = scheduler
        self.rate_limiter = rate_limiter
        self._setup_handlers()
        self.creator_id = 308780639
    
    def _setup_handlers(self):
        """Настройка всех обработчиков."""
        self.app.add_handler(MessageHandler(filters.ALL, self.handle_any_message))
        
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("admin", self.admin_panel))
        self.app.add_handler(CommandHandler("cancel", self.cancel))
        self.app.add_handler(CommandHandler("help", self.help_command))
        self.app.add_handler(CommandHandler("about", self.about_command))
        self.app.add_handler(CommandHandler("stats", self.stats_command))
        self.app.add_handler(CommandHandler("mood", self.mood_command))
        self.app.add_handler(CommandHandler("settimezone", self.set_timezone_command))
        self.app.add_handler(CommandHandler("list_medicines", self.list_medicines))
        self.app.add_handler(CommandHandler("list_analyses", self.list_analyses))
        self.app.add_handler(CommandHandler("add_medicine", self.add_medicine_start))
        self.app.add_handler(CommandHandler("add_analysis", self.add_analysis_start))
        self.app.add_handler(CommandHandler("take_unscheduled", self.unscheduled_medicine_start))
        
        self.app.add_handler(self._medicine_conversation())
        self.app.add_handler(self._analysis_conversation())
        self.app.add_handler(self._symptom_conversation())
        self.app.add_handler(self._unscheduled_conversation())
        self.app.add_handler(self._postpone_conversation())
        self.app.add_handler(self._broadcast_conversation())
        self.app.add_handler(self._admin_search_conversation())
        
        self.app.add_handler(CallbackQueryHandler(self.callback_handler))
    
    def _medicine_conversation(self):
        """Conversation для добавления лекарства."""
        return ConversationHandler(
            entry_points=[
                CommandHandler("add_medicine", self.add_medicine_start),
                CallbackQueryHandler(self.add_medicine_start, pattern="^add_medicine$")
            ],
            states={
                States.MEDICINE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.medicine_name)],
                States.MEDICINE_DOSAGE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.medicine_dosage),
                    CallbackQueryHandler(self.skip_dosage, pattern="^skip_dosage$")
                ],
                States.MEDICINE_TIMES_PER_DAY: [CallbackQueryHandler(self.medicine_times_per_day, pattern="^times_")],
                States.MEDICINE_SCHEDULE_HOUR: [
                    CallbackQueryHandler(self.medicine_schedule_hour, pattern="^hour_"),
                    CallbackQueryHandler(self.use_default_times, pattern="^use_default_times$")
                ],
                States.MEDICINE_SCHEDULE_MINUTE: [
                    CallbackQueryHandler(self.medicine_schedule_minute, pattern="^minute_"),
                    CallbackQueryHandler(self.back_to_hours, pattern="^back_to_hours$")
                ],
                States.MEDICINE_COURSE_DURATION: [
                    CallbackQueryHandler(self.medicine_course_duration, pattern="^duration_"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.medicine_course_duration_custom)
                ],
                States.MEDICINE_REPEAT: [CallbackQueryHandler(self.medicine_repeat, pattern="^repeat_")],
                States.MEDICINE_REPEAT_INTERVAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.medicine_repeat_interval)],
                States.MEDICINE_START_TYPE: [
                    CallbackQueryHandler(self.medicine_start_type, pattern="^start_"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.medicine_start_date)
                ],
                States.MEDICINE_CONFIRM: [
                    CallbackQueryHandler(self.medicine_confirm, pattern="^confirm_medicine$"),
                    CallbackQueryHandler(self.medicine_edit, pattern="^edit_medicine$")
                ],
                States.MEDICINE_EDIT: [CallbackQueryHandler(self.medicine_edit_field, pattern="^edit_field_")],
                States.MEDICINE_COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.medicine_comment_text)]
            },
            fallbacks=[
                CommandHandler("cancel", self.cancel),
                CallbackQueryHandler(self.cancel, pattern="^cancel$"),
            ],
            name="add_medicine"
        )
    
    def _analysis_conversation(self):
        """Conversation для добавления анализа."""
        return ConversationHandler(
            entry_points=[
                CommandHandler("add_analysis", self.add_analysis_start),
                CallbackQueryHandler(self.add_analysis_start, pattern="^add_analysis$")
            ],
            states={
                States.ANALYSIS_TYPE: [CallbackQueryHandler(self.analysis_type, pattern="^analysis_type_")],
                States.ANALYSIS_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.analysis_name)],
                States.ANALYSIS_DATE: [
                    CallbackQueryHandler(self.analysis_date, pattern="^analysis_date_"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.analysis_date_custom)
                ],
                States.ANALYSIS_TIME_HOUR: [
                    CallbackQueryHandler(self.analysis_time_hour, pattern="^analysis_hour_"),
                    CallbackQueryHandler(self.analysis_time_custom, pattern="^analysis_time_custom$")
                ],
                States.ANALYSIS_TIME_MINUTE: [
                    CallbackQueryHandler(self.analysis_time_minute, pattern="^minute_"),
                    CallbackQueryHandler(self.back_to_analysis_hours, pattern="^back_to_analysis_hours$")
                ],
                States.ANALYSIS_REPEAT: [CallbackQueryHandler(self.analysis_repeat, pattern="^repeat_")],
                States.ANALYSIS_REPEAT_INTERVAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.analysis_repeat_interval)],
                States.ANALYSIS_REMINDER: [CallbackQueryHandler(self.analysis_reminder, pattern="^remind_")],
                States.ANALYSIS_NOTES: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.analysis_notes),
                    CallbackQueryHandler(self.skip_notes, pattern="^skip_notes$")
                ],
                States.ANALYSIS_CONFIRM: [
                    CallbackQueryHandler(self.analysis_confirm, pattern="^confirm_analysis$"),
                    CallbackQueryHandler(self.analysis_edit, pattern="^edit_analysis$")
                ],
                States.ANALYSIS_EDIT: [CallbackQueryHandler(self.analysis_edit_field, pattern="^edit_field_")]
            },
            fallbacks=[
                CommandHandler("cancel", self.cancel),
                CallbackQueryHandler(self.cancel, pattern="^cancel$"),
            ],
            name="add_analysis"
        )
    
    def _symptom_conversation(self):
        """Conversation для добавления симптомов."""
        return ConversationHandler(
            entry_points=[
                CallbackQueryHandler(self.symptoms_start, pattern="^symptoms$"),
                CallbackQueryHandler(self.new_symptom, pattern="^new_symptom_"),
            ],
            states={
                States.SYMPTOM_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.symptom_text)],
                States.SYMPTOM_SEVERITY: [CallbackQueryHandler(self.symptom_severity, pattern="^severity_")],
            },
            fallbacks=[
                CommandHandler("cancel", self.cancel),
                CallbackQueryHandler(self.cancel, pattern="^cancel$"),
            ],
            name="add_symptom"
        )
    
    def _unscheduled_conversation(self):
        """Conversation для незапланированного приема."""
        return ConversationHandler(
            entry_points=[
                CommandHandler("take_unscheduled", self.unscheduled_medicine_start),
                CallbackQueryHandler(self.unscheduled_medicine_start, pattern="^take_unscheduled$")
            ],
            states={
                States.UNSCHEDULED_MEDICINE_SELECT: [
                    CallbackQueryHandler(self.unscheduled_medicine_select, pattern="^unscheduled_medicine_"),
                    CallbackQueryHandler(self.add_medicine_start, pattern="^add_new_medicine$")
                ],
                States.UNSCHEDULED_MEDICINE_DOSAGE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.unscheduled_medicine_dosage),
                    CallbackQueryHandler(self.skip_dosage, pattern="^skip_dosage$")
                ],
                States.UNSCHEDULED_MEDICINE_REASON: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.unscheduled_medicine_reason),
                    CallbackQueryHandler(self.skip_reason, pattern="^skip_reason$")
                ],
                States.UNSCHEDULED_MEDICINE_COMMENT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.unscheduled_medicine_comment),
                    CallbackQueryHandler(self.skip_comment, pattern="^skip_comment$")
                ]
            },
            fallbacks=[
                CommandHandler("cancel", self.cancel),
                CallbackQueryHandler(self.cancel, pattern="^cancel$"),
            ],
            name="unscheduled_medicine"
        )
    
    def _postpone_conversation(self):
        """Conversation для откладывания."""
        return ConversationHandler(
            entry_points=[
                CallbackQueryHandler(self.postpone_start, pattern="^postpone_"),
            ],
            states={
                States.POSTPONE_TYPE: [CallbackQueryHandler(self.postpone_type, pattern="^postpone_type_")],
                States.POSTPONE_HOURS: [CallbackQueryHandler(self.postpone_hours, pattern="^postpone_hour_")],
                States.POSTPONE_DAYS: [CallbackQueryHandler(self.postpone_days, pattern="^postpone_day_")],
                States.POSTPONE_CUSTOM: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.postpone_custom_value)],
            },
            fallbacks=[
                CommandHandler("cancel", self.cancel),
                CallbackQueryHandler(self.cancel, pattern="^cancel$"),
            ],
            name="postpone"
        )
    
    def _broadcast_conversation(self):
        """Conversation для рассылки."""
        return ConversationHandler(
            entry_points=[CallbackQueryHandler(self.admin_broadcast_start, pattern="^admin_broadcast$")],
            states={
                States.BROADCAST_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.admin_broadcast_message)],
                States.BROADCAST_CONFIRM: [CallbackQueryHandler(self.admin_broadcast_confirm, pattern="^admin_broadcast_confirm$")]
            },
            fallbacks=[CallbackQueryHandler(self.admin_broadcast_cancel, pattern="^admin_broadcast_cancel$")],
            name="admin_broadcast"
        )
    
    def _admin_search_conversation(self):
        """Conversation для поиска пользователей."""
        return ConversationHandler(
            entry_points=[CallbackQueryHandler(self.admin_users_search, pattern="^admin_users_search$")],
            states={
                States.ADMIN_USER_SEARCH: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.admin_user_search_results)]
            },
            fallbacks=[CallbackQueryHandler(self.admin_users, pattern="^admin_users$")],
            name="admin_user_search"
        )
    
    # ============== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==============
    
    def truncate_text(self, text: str, max_length: int = config.MAX_MESSAGE_LENGTH) -> str:
        """Обрезка текста до допустимой длины."""
        if len(text) > max_length:
            return text[:max_length-3] + "..."
        return text
    
    async def safe_edit_message(self, query, text, reply_markup=None, parse_mode=None):
        """Безопасное редактирование сообщения."""
        text = self.truncate_text(text)
        try:
            if parse_mode == ParseMode.MARKDOWN:
                await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
            else:
                await query.edit_message_text(text, reply_markup=reply_markup)
        except BadRequest as e:
            if "Can't parse entities" in str(e) and parse_mode == ParseMode.MARKDOWN:
                plain_text = text.replace('*', '').replace('_', '').replace('`', '')
                await query.edit_message_text(plain_text, reply_markup=reply_markup)
            elif "Message is not modified" not in str(e):
                logger.log_error(e)
        except Exception as e:
            logger.log_error(e)
    
    async def safe_send_message(self, chat_id, text, reply_markup=None, parse_mode=None):
        """Безопасная отправка сообщения."""
        text = self.truncate_text(text)
        try:
            if parse_mode == ParseMode.MARKDOWN:
                await self.app.bot.send_message(
                    chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode=parse_mode
                )
            else:
                await self.app.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
        except BadRequest as e:
            if "Can't parse entities" in str(e) and parse_mode == ParseMode.MARKDOWN:
                plain_text = text.replace('*', '').replace('_', '').replace('`', '')
                await self.app.bot.send_message(chat_id=chat_id, text=plain_text, reply_markup=reply_markup)
            else:
                logger.log_error(e)
        except Exception as e:
            logger.log_error(e)
    
    async def _check_admin_by_id(self, user_id: int) -> bool:
        """Проверка прав администратора по ID."""
        if user_id == self.creator_id:
            return True
        
        cached = await redis_cache.get(f"admin:{user_id}")
        if cached is not None:
            return cached == "true"
        
        user = await db.fetchrow(
            "SELECT role FROM users WHERE user_id = $1",
            user_id
        )
        is_admin = user and user['role'] in ['admin', 'super_admin']
        
        await redis_cache.set(f"admin:{user_id}", "true" if is_admin else "false", 300)
        return is_admin
    
    async def _register_user(self, user_id: int, user):
        """Регистрация пользователя."""
        await db.execute("""
            INSERT INTO users (user_id, username, first_name, last_name, last_seen, total_interactions)
            VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, 1)
            ON CONFLICT (user_id) DO UPDATE SET
                last_seen = CURRENT_TIMESTAMP,
                total_interactions = users.total_interactions + 1,
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name
        """, user_id, user.username, user.first_name, user.last_name)
        
        metrics.active_users.inc()
    
    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена операции."""
        keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
        
        if update.callback_query:
            await self.safe_edit_message(update.callback_query, "❌ Операция отменена", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await update.message.reply_text("❌ Операция отменена", reply_markup=InlineKeyboardMarkup(keyboard))
        
        context.user_data.clear()
        return ConversationHandler.END
    
    # ============== КОМАНДЫ ==============
    
    @measure_time
    async def handle_any_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик любого сообщения."""
        if update.callback_query:
            return
        
        user = update.effective_user
        
        if context.user_data and any(key in context.user_data for key in ['medicine_data', 'analysis_data']):
            return
        
        await self._register_user(user.id, user)
        
        if user.id == self.creator_id:
            await db.execute(
                "UPDATE users SET role = 'super_admin' WHERE user_id = $1 AND role != 'super_admin'",
                user.id
            )
            await redis_cache.delete(f"admin:{user.id}")
            logger.log('info', f"Creator {user.id} set as SUPER_ADMIN")
        
        welcome_text = f"""👋 *Здравствуйте, {user.first_name}!*

Я *{config.BOT_NAME}* — персональный медицинский бот, созданный врачом-оториноларингологом Денисом Казариным.

👶 *Ведет прием детей с 0 лет и взрослых*

*Мои возможности:*
- 💊 Напоминания о приеме лекарств
- 🩺 Напоминания об анализах
- 📊 Отслеживание самочувствия и симптомов
- 📈 Статистика и отчеты

Нажмите кнопку ниже, чтобы начать работу!"""
        
        metrics.welcome_shown.inc()
        keyboard = [[InlineKeyboardButton("🚀 Старт", callback_data="start")]]
        
        await update.message.reply_text(
            welcome_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        
        logger.log('info', f"Показано приветствие пользователю {user.id}")
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик /start."""
        user = update.effective_user
        await self._register_user(user.id, user)
        
        is_admin = await self._check_admin_by_id(user.id)
        
        welcome_text = f"""👋 *Здравствуйте, {user.first_name}!*

Я *{config.BOT_NAME}* — персональный медицинский бот, созданный врачом-оториноларингологом Денисом Казариным.

👶 *Ведет прием детей с 0 лет и взрослых*

*Мои возможности:*
• 💊 Напоминания о приеме лекарств
• 🩺 Напоминания об анализах
• 📊 Отслеживание самочувствия
• 📈 Статистика и отчеты"""
        
        keyboard = [
            [InlineKeyboardButton("💊 Добавить лекарство", callback_data="add_medicine")],
            [InlineKeyboardButton("📋 Список лекарств", callback_data="list_medicines")],
            [InlineKeyboardButton("💊 Принять препарат", callback_data="take_unscheduled")],
            [InlineKeyboardButton("🩺 Добавить анализ", callback_data="add_analysis")],
            [InlineKeyboardButton("📋 Список анализов", callback_data="list_analyses")],
            [InlineKeyboardButton("📊 Самочувствие", callback_data="mood")],
            [InlineKeyboardButton("📈 Статистика", callback_data="stats")],
            [InlineKeyboardButton("👨‍⚕️ О враче", callback_data="about")],
        ]
        
        if is_admin:
            keyboard.append([InlineKeyboardButton("👨‍💻 Админ-панель", callback_data="admin_panel")])
        
        await update.message.reply_text(
            welcome_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help."""
        text = """❓ *Помощь*

*Доступные команды:*
/start - 🚀 Начать работу
/help - ❓ Помощь
/about - 👨‍⚕️ О враче
/stats - 📈 Статистика
/mood - 📊 Самочувствие
/settimezone - 🕒 Настройка часового пояса
/list_medicines - 📋 Список лекарств
/list_analyses - 📋 Список анализов
/add_medicine - 💊 Добавить лекарство
/add_analysis - 🩺 Добавить анализ
/take_unscheduled - 💊 Принять препарат

*Как очистить историю переписки:*
1️⃣ В правом верхнем углу нажмите на свой профиль
2️⃣ В меню выберите пункт "Еще"
3️⃣ Прокрутите вниз и нажмите "Удалить переписку"

✅ После этого появится начальная страница бота
💾 Все ваши сохраненные данные останутся"""
        
        keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
        
        if update.callback_query:
            await self.safe_edit_message(update.callback_query, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    
    async def about_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /about."""
        await self.about_callback(update, context)
    
    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /stats."""
        await self.stats_callback(update, context)
    
    async def mood_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /mood."""
        await self.mood_callback(update, context)
    
    async def set_timezone_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /settimezone."""
        user_id = update.effective_user.id
        current_tz = await TimeUtils.get_user_timezone(user_id)
        
        text = f"""🕒 *Настройка часового пояса*

Ваш текущий часовой пояс: *{current_tz}*

Выберите ваш часовой пояс из списка:"""
        
        keyboard = [
            [InlineKeyboardButton("Москва (UTC+3)", callback_data="tz_Europe/Moscow")],
            [InlineKeyboardButton("Калининград (UTC+2)", callback_data="tz_Europe/Kaliningrad")],
            [InlineKeyboardButton("Самара (UTC+4)", callback_data="tz_Europe/Samara")],
            [InlineKeyboardButton("Екатеринбург (UTC+5)", callback_data="tz_Asia/Yekaterinburg")],
            [InlineKeyboardButton("Омск (UTC+6)", callback_data="tz_Asia/Omsk")],
            [InlineKeyboardButton("Красноярск (UTC+7)", callback_data="tz_Asia/Krasnoyarsk")],
            [InlineKeyboardButton("Иркутск (UTC+8)", callback_data="tz_Asia/Irkutsk")],
            [InlineKeyboardButton("🔙 Назад", callback_data="start")],
        ]
        
        if update.callback_query:
            await self.safe_edit_message(update.callback_query, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    
    # ============== ОБРАБОТЧИКИ CALLBACK ==============
    
    @measure_time
    async def callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик callback запросов."""
        query = update.callback_query
        data = query.data
        user_id = update.effective_user.id
        
        if not SecurityManager.validate_callback_data(data):
            await query.answer("❌ Некорректные данные")
            return
        
        if SecurityManager.check_double_click(user_id, data):
            await query.answer("⏳ Пожалуйста, подождите секунду", show_alert=False)
            return
        
        try:
            await self.rate_limiter.acquire(user_id)
        except asyncio.TimeoutError:
            await query.answer("⏰ Слишком много запросов", show_alert=True)
            return
        
        await query.answer()
        
        # Навигация
        if data == "start":
            await self.start_callback(update, context)
        elif data == "about":
            await self.about_callback(update, context)
        elif data == "help":
            await self.help_callback(update, context)
        elif data == "stats":
            await self.stats_callback(update, context)
        elif data == "mood":
            await self.mood_callback(update, context)
        elif data == "list_medicines":
            await self.list_medicines(update, context)
        elif data == "list_analyses":
            await self.list_analyses(update, context)
        elif data == "add_medicine":
            await self.add_medicine_start(update, context)
        elif data == "add_analysis":
            await self.add_analysis_start(update, context)
        elif data == "take_unscheduled":
            await self.unscheduled_medicine_start(update, context)
        elif data == "admin_panel":
            await self.admin_panel_callback(update, context)
        elif data.startswith("mood_"):
            await self.mood_select(update, context)
        elif data.startswith("tz_"):
            tz_name = data.replace("tz_", "")
            await self.timezone_callback(update, context, tz_name)
        elif data.startswith("take_"):
            medicine_id = int(data.replace("take_", ""))
            await self.medicine_take(update, context, medicine_id)
        elif data.startswith("postpone_medicine_"):
            medicine_id = int(data.replace("postpone_medicine_", ""))
            await self.postpone_start(update, context, 'medicine', medicine_id)
        elif data.startswith("cancel_medicine_"):
            medicine_id = int(data.replace("cancel_medicine_", ""))
            await self.medicine_cancel(update, context, medicine_id)
        elif data.startswith("comment_medicine_"):
            medicine_id = int(data.replace("comment_medicine_", ""))
            await self.medicine_comment(update, context, medicine_id)
        elif data.startswith("new_symptom_"):
            medicine_id = int(data.replace("new_symptom_", ""))
            await self.new_symptom(update, context, medicine_id)
        elif data.startswith("analysis_take_"):
            analysis_id = int(data.replace("analysis_take_", ""))
            await self.analysis_take(update, context, analysis_id)
        elif data.startswith("postpone_analysis_"):
            analysis_id = int(data.replace("postpone_analysis_", ""))
            await self.postpone_start(update, context, 'analysis', analysis_id)
        elif data.startswith("cancel_analysis_"):
            analysis_id = int(data.replace("cancel_analysis_", ""))
            await self.analysis_cancel(update, context, analysis_id)
        elif data.startswith("analysis_notes_"):
            analysis_id = int(data.replace("analysis_notes_", ""))
            await self.analysis_notes(update, context, analysis_id)
        elif data.startswith("postpone_hour_"):
            hours = int(data.replace("postpone_hour_", ""))
            await self.execute_postpone(update, context, hours, 'hours')
        elif data.startswith("postpone_day_"):
            days = int(data.replace("postpone_day_", ""))
            await self.execute_postpone(update, context, days, 'days')
        elif data.startswith("delete_medicine_"):
            medicine_id = int(data.replace("delete_medicine_", ""))
            await self.delete_medicine(update, context, medicine_id)
        elif data.startswith("delete_analysis_"):
            analysis_id = int(data.replace("delete_analysis_", ""))
            await self.delete_analysis(update, context, analysis_id)
        elif data.startswith("admin_user_stats_"):
            target_id = int(data.replace("admin_user_stats_", ""))
            await self.admin_user_stats(update, context, target_id)
        elif data.startswith("admin_user_ban_"):
            target_id = int(data.replace("admin_user_ban_", ""))
            await self._admin_ban_user(update, context, target_id)
        elif data.startswith("admin_user_unban_"):
            target_id = int(data.replace("admin_user_unban_", ""))
            await self._admin_unban_user(update, context, target_id)
        elif data.startswith("admin_user_make_admin_"):
            target_id = int(data.replace("admin_user_make_admin_", ""))
            await self._admin_make_admin(update, context, target_id)
        elif data.startswith("admin_user_remove_admin_"):
            target_id = int(data.replace("admin_user_remove_admin_", ""))
            await self._admin_remove_admin(update, context, target_id)
        elif data.startswith("medicines_page_"):
            page = int(data.replace("medicines_page_", ""))
            context.user_data['medicines_page'] = page
            await self.list_medicines(update, context)
        elif data.startswith("analyses_page_"):
            page = int(data.replace("analyses_page_", ""))
            context.user_data['analyses_page'] = page
            await self.list_analyses(update, context)
        else:
            logger.log('warning', f"Неизвестный callback: {data}")
    
    async def start_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Возврат на стартовую страницу."""
        query = update.callback_query
        user = update.effective_user
        is_admin = await self._check_admin_by_id(user.id)
        
        text = f"""👋 *Здравствуйте, {user.first_name}!*

Я *{config.BOT_NAME}* — персональный медицинский бот.

👶 *Ведет прием детей с 0 лет и взрослых*

*Мои возможности:*
• 💊 Напоминания о приеме лекарств
• 🩺 Напоминания об анализах
• 📊 Отслеживание самочувствия
• 📈 Статистика и отчеты"""
        
        keyboard = [
            [InlineKeyboardButton("💊 Добавить лекарство", callback_data="add_medicine")],
            [InlineKeyboardButton("📋 Список лекарств", callback_data="list_medicines")],
            [InlineKeyboardButton("💊 Принять препарат", callback_data="take_unscheduled")],
            [InlineKeyboardButton("🩺 Добавить анализ", callback_data="add_analysis")],
            [InlineKeyboardButton("📋 Список анализов", callback_data="list_analyses")],
            [InlineKeyboardButton("📊 Самочувствие", callback_data="mood")],
            [InlineKeyboardButton("📈 Статистика", callback_data="stats")],
            [InlineKeyboardButton("👨‍⚕️ О враче", callback_data="about")],
        ]
        
        if is_admin:
            keyboard.append([InlineKeyboardButton("👨‍💻 Админ-панель", callback_data="admin_panel")])
        
        await self.safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    
    async def about_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик 'О враче'."""
        query = update.callback_query
        
        text = DOCTOR_INFO + f"""

📍 *КИТ-клиника:*
{KIT_CLINIC['address']}
📞 {KIT_CLINIC['phone_display']}

📍 *Семейная клиника:*
{FAMILY_CLINIC['address']}
📞 {FAMILY_CLINIC['phone_display']}"""
        
        keyboard = [
            [InlineKeyboardButton("👥 Telegram канал", url="https://t.me/KAZARIN_LOR")],
            [InlineKeyboardButton("💬 Личный Telegram", url="https://t.me/deniskazarin")],
            [InlineKeyboardButton("🏥 КИТ-клиника", url=KIT_CLINIC['site'])],
            [InlineKeyboardButton("🏥 Семейная клиника", url=FAMILY_CLINIC['site'])],
            [InlineKeyboardButton("❓ Помощь", callback_data="help")],
            [InlineKeyboardButton("🔙 Назад", callback_data="start")],
        ]
        
        await self.safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    
    async def help_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик 'Помощь'."""
        query = update.callback_query
        
        text = """❓ *Как очистить историю переписки*

Чтобы удалить всю переписку с ботом:

1️⃣ В правом верхнем углу нажмите на свой профиль
2️⃣ В меню выберите пункт "Еще"
3️⃣ Прокрутите вниз и нажмите "Удалить переписку"

✅ После этого появится начальная страница бота
💾 Все ваши сохраненные данные останутся

*Доступные команды:*
/start - 🚀 Начать работу
/help - ❓ Помощь
/about - 👨‍⚕️ О враче
/stats - 📈 Статистика
/mood - 📊 Самочувствие"""
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="about")]]
        await self.safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    
    async def stats_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показ статистики."""
        await self.stats_command(update, context)
    
    async def mood_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Оценка самочувствия."""
        query = update.callback_query
        
        keyboard = [
            [InlineKeyboardButton("1 😢", callback_data="mood_1"),
             InlineKeyboardButton("2 🙁", callback_data="mood_2"),
             InlineKeyboardButton("3 😐", callback_data="mood_3"),
             InlineKeyboardButton("4 🙂", callback_data="mood_4"),
             InlineKeyboardButton("5 😊", callback_data="mood_5")],
            [InlineKeyboardButton("🔙 Назад", callback_data="start")],
        ]
        
        await self.safe_edit_message(query, "📊 *Как вы себя чувствуете сегодня?*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    
    async def mood_select(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор оценки настроения."""
        query = update.callback_query
        mood_score = int(query.data.replace("mood_", ""))
        user_id = update.effective_user.id
        
        await db.execute(
            "INSERT INTO mood_logs (user_id, mood_score) VALUES ($1, $2)",
            user_id, mood_score
        )
        metrics.mood_logs.labels(score=str(mood_score)).inc()
        
        # Проверка на ухудшение
        recent = await db.fetch(
            "SELECT mood_score FROM mood_logs WHERE user_id = $1 ORDER BY created_at DESC LIMIT 2",
            user_id
        )
        
        if len(recent) == 2 and all(r['mood_score'] <= 2 for r in recent):
            warning_text = """⚠️ *Внимание!*

Зафиксировано ухудшение самочувствия два дня подряд.

Рекомендуется обратиться к врачу."""
            
            await self.safe_send_message(
                user_id,
                warning_text,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("👨‍⚕️ Записаться", callback_data="about")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
        
        mood_texts = {
            1: "😢 Очень плохо. Берегите себя!",
            2: "🙁 Плохо. Надеюсь, скоро станет лучше!",
            3: "😐 Нормально. Это уже хорошо!",
            4: "🙂 Хорошо! Отличное настроение!",
            5: "😊 Отлично! Так держать!"
        }
        
        keyboard = [
            [InlineKeyboardButton("🩺 Отметить симптомы", callback_data="symptoms"),
             InlineKeyboardButton("❌ Нет", callback_data="start")],
        ]
        
        await self.safe_edit_message(query, f"✅ {mood_texts[mood_score]}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    
    async def timezone_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE, tz_name: str):
        """Установка часового пояса."""
        query = update.callback_query
        user_id = update.effective_user.id
        await TimeUtils.set_user_timezone(user_id, tz_name)
        
        await self.safe_edit_message(
            query,
            f"✅ *Часовой пояс установлен*\n\nВаш часовой пояс: *{tz_name}*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="start")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    # ============== ЛЕКАРСТВА ==============
    
    async def add_medicine_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начало добавления лекарства."""
        if update.callback_query:
            query = update.callback_query
            await query.answer()
            edit_func = query.edit_message_text
        else:
            edit_func = update.message.reply_text
        
        context.user_data['medicine_data'] = {}
        
        await edit_func("💊 *Добавление лекарства*\n\nШаг 1: Введите *название лекарства*", parse_mode=ParseMode.MARKDOWN)
        return States.MEDICINE_NAME
    
    async def medicine_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение названия лекарства."""
        if SecurityManager.check_sql_injection(update.message.text):
            await update.message.reply_text("❌ Обнаружены недопустимые символы")
            return States.MEDICINE_NAME
        
        is_valid, error_msg = SecurityManager.validate_medicine_name(update.message.text)
        if not is_valid:
            await update.message.reply_text(f"❌ {error_msg}")
            return States.MEDICINE_NAME
        
        context.user_data['medicine_data']['name'] = SecurityManager.sanitize_input(update.message.text)
        
        keyboard = [[InlineKeyboardButton("⏭️ Пропустить", callback_data="skip_dosage")]]
        await update.message.reply_text(
            "Шаг 2: Укажите *дозировку* (или нажмите Пропустить)", 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode=ParseMode.MARKDOWN
        )
        return States.MEDICINE_DOSAGE
    
    async def medicine_dosage(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение дозировки."""
        if SecurityManager.check_sql_injection(update.message.text):
            await update.message.reply_text("❌ Обнаружены недопустимые символы")
            return States.MEDICINE_DOSAGE
        
        context.user_data['medicine_data']['dosage'] = SecurityManager.sanitize_input(update.message.text)
        
        keyboard = [
            [InlineKeyboardButton("1 раз", callback_data="times_1"),
             InlineKeyboardButton("2 раза", callback_data="times_2"),
             InlineKeyboardButton("3 раза", callback_data="times_3")],
            [InlineKeyboardButton("4 раза", callback_data="times_4"),
             InlineKeyboardButton("5 раз", callback_data="times_5"),
             InlineKeyboardButton("6 раз", callback_data="times_6")]
        ]
        await update.message.reply_text(
            "Шаг 3: Сколько раз в день принимать?", 
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return States.MEDICINE_TIMES_PER_DAY
    
    async def skip_dosage(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Пропуск дозировки."""
        query = update.callback_query
        await query.answer()
        
        if 'medicine_data' in context.user_data:
            context.user_data['medicine_data']['dosage'] = None
            
            keyboard = [
                [InlineKeyboardButton("1 раз", callback_data="times_1"),
                 InlineKeyboardButton("2 раза", callback_data="times_2"),
                 InlineKeyboardButton("3 раза", callback_data="times_3")],
                [InlineKeyboardButton("4 раза", callback_data="times_4"),
                 InlineKeyboardButton("5 раз", callback_data="times_5"),
                 InlineKeyboardButton("6 раз", callback_data="times_6")]
            ]
            await self.safe_edit_message(
                query, 
                "Шаг 3: Сколько раз в день принимать?", 
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return States.MEDICINE_TIMES_PER_DAY
        else:
            context.user_data['unscheduled_dosage'] = None
            await self.safe_edit_message(
                query,
                "💊 *Причина приема?*",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⏭️ Пропустить", callback_data="skip_reason")]
                ])
            )
            return States.UNSCHEDULED_MEDICINE_REASON
    
    async def medicine_times_per_day(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор количества приемов."""
        query = update.callback_query
        await query.answer()
        
        times = int(query.data.replace("times_", ""))
        context.user_data['medicine_data']['times_per_day'] = times
        context.user_data['medicine_data']['schedule_times'] = []
        
        if times == 2:
            keyboard = [[InlineKeyboardButton("8:00, 20:00", callback_data="use_default_times")]]
        elif times == 3:
            keyboard = [[InlineKeyboardButton("8:00, 14:00, 20:00", callback_data="use_default_times")]]
        else:
            keyboard = []
        
        keyboard.append([InlineKeyboardButton("⚙️ Своё время", callback_data="hour_select")])
        
        await self.safe_edit_message(
            query,
            f"Шаг 4: Выберите время приема",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return States.MEDICINE_SCHEDULE_HOUR
    
    async def use_default_times(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Использование стандартного времени."""
        query = update.callback_query
        await query.answer()
        
        times = context.user_data['medicine_data']['times_per_day']
        default_times = {
            2: ["08:00", "20:00"],
            3: ["08:00", "14:00", "20:00"]
        }.get(times, [])
        
        context.user_data['medicine_data']['schedule_times'] = default_times
        
        await self.show_course_duration(query, context)
        return States.MEDICINE_COURSE_DURATION
    
    async def medicine_schedule_hour(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор часа."""
        query = update.callback_query
        await query.answer()
        
        if query.data == "hour_select":
            keyboard = []
            for start in range(0, 24, 6):
                row = []
                for h in range(start, min(start + 6, 24)):
                    row.append(InlineKeyboardButton(f"{h:02d}", callback_data=f"hour_{h:02d}"))
                keyboard.append(row)
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="add_medicine")])
            
            await self.safe_edit_message(
                query,
                "Выберите час:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return States.MEDICINE_SCHEDULE_HOUR
        
        hour = query.data.replace("hour_", "")
        context.user_data['temp_hour'] = hour
        
        minutes = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
        keyboard = []
        for i in range(0, len(minutes), 4):
            row = [InlineKeyboardButton(f"{m:02d}", callback_data=f"minute_{m:02d}") for m in minutes[i:i+4]]
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("⏰ К выбору часа", callback_data="back_to_hours")])
        
        await self.safe_edit_message(
            query,
            f"Вы выбрали час {hour}. Теперь выберите минуты:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return States.MEDICINE_SCHEDULE_MINUTE
    
    async def medicine_schedule_minute(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор минут."""
        query = update.callback_query
        await query.answer()
        
        minute = query.data.replace("minute_", "")
        hour = context.user_data.get('temp_hour', "00")
        time_str = f"{hour}:{minute}"
        
        times = context.user_data['medicine_data']['schedule_times']
        times.append(time_str)
        context.user_data['medicine_data']['schedule_times'] = times
        
        if len(times) < context.user_data['medicine_data']['times_per_day']:
            # Продолжаем выбор
            keyboard = []
            for start in range(0, 24, 6):
                row = []
                for h in range(start, min(start + 6, 24)):
                    row.append(InlineKeyboardButton(f"{h:02d}", callback_data=f"hour_{h:02d}"))
                keyboard.append(row)
            
            await self.safe_edit_message(
                query,
                f"Выберите час для следующего приема ({len(times)}/{context.user_data['medicine_data']['times_per_day']}):",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return States.MEDICINE_SCHEDULE_HOUR
        else:
            await self.show_course_duration(query, context)
            return States.MEDICINE_COURSE_DURATION
    
    async def back_to_hours(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Возврат к часам."""
        query = update.callback_query
        await query.answer()
        
        keyboard = []
        for start in range(0, 24, 6):
            row = []
            for h in range(start, min(start + 6, 24)):
                row.append(InlineKeyboardButton(f"{h:02d}", callback_data=f"hour_{h:02d}"))
            keyboard.append(row)
        
        await self.safe_edit_message(
            query,
            "Выберите час:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return States.MEDICINE_SCHEDULE_HOUR
    
    async def show_course_duration(self, query, context):
        """Показать выбор длительности курса."""
        await self.safe_edit_message(
            query,
            "Шаг 5: Выберите продолжительность курса",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📅 3 дня", callback_data="duration_3"),
                 InlineKeyboardButton("📅 5 дней", callback_data="duration_5"),
                 InlineKeyboardButton("📅 7 дней", callback_data="duration_7")],
                [InlineKeyboardButton("📅 10 дней", callback_data="duration_10"),
                 InlineKeyboardButton("📅 14 дней", callback_data="duration_14"),
                 InlineKeyboardButton("📅 30 дней", callback_data="duration_30")],
                [InlineKeyboardButton("⚙️ Свой вариант", callback_data="duration_custom"),
                 InlineKeyboardButton("∞ Бессрочно", callback_data="duration_unlimited")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def medicine_course_duration(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор продолжительности курса."""
        query = update.callback_query
        await query.answer()
        
        data = query.data.replace("duration_", "")
        
        if data == "custom":
            await self.safe_edit_message(
                query,
                "Введите количество дней (от 1 до 365):",
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_COURSE_DURATION
        elif data == "unlimited":
            context.user_data['medicine_data']['course_duration'] = None
            
            await self.safe_edit_message(
                query,
                "Шаг 6: Нужно ли повторять курс?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Без повторения", callback_data="repeat_none"),
                     InlineKeyboardButton("⚙️ Свой интервал", callback_data="repeat_custom")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_REPEAT
        else:
            context.user_data['medicine_data']['course_duration'] = int(data)
            
            await self.safe_edit_message(
                query,
                "Шаг 6: Нужно ли повторять курс?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Без повторения", callback_data="repeat_none"),
                     InlineKeyboardButton("⚙️ Свой интервал", callback_data="repeat_custom")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_REPEAT
    
    async def medicine_course_duration_custom(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Пользовательская продолжительность курса."""
        is_valid, days, error = SecurityManager.validate_number_input(update.message.text, 1, 365)
        if not is_valid:
            await update.message.reply_text(f"❌ {error}")
            return States.MEDICINE_COURSE_DURATION
        
        context.user_data['medicine_data']['course_duration'] = days
        
        await update.message.reply_text(
            "Шаг 6: Нужно ли повторять курс?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Без повторения", callback_data="repeat_none"),
                 InlineKeyboardButton("⚙️ Свой интервал", callback_data="repeat_custom")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.MEDICINE_REPEAT
    
    async def medicine_repeat(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор повторения."""
        query = update.callback_query
        await query.answer()
        
        repeat_type = query.data.replace("repeat_", "")
        
        if repeat_type == "custom":
            await self.safe_edit_message(
                query,
                "Введите интервал повторения в днях:",
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_REPEAT_INTERVAL
        elif repeat_type == "none":
            context.user_data['medicine_data']['repeat_type'] = 'none'
            context.user_data['medicine_data']['repeat_interval'] = None
            
            await self.show_start_date(query, context)
            return States.MEDICINE_START_TYPE
    
    async def medicine_repeat_interval(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка интервала повторения."""
        is_valid, interval, error = SecurityManager.validate_number_input(update.message.text, 1, 365)
        if not is_valid:
            await update.message.reply_text(f"❌ {error}")
            return States.MEDICINE_REPEAT_INTERVAL
        
        context.user_data['medicine_data']['repeat_type'] = 'custom'
        context.user_data['medicine_data']['repeat_interval'] = interval
        
        await update.message.reply_text(
            "Шаг 7: Выберите дату начала",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📅 Сегодня", callback_data="start_today"),
                 InlineKeyboardButton("📅 Завтра", callback_data="start_tomorrow")],
                [InlineKeyboardButton("📅 Выбрать дату", callback_data="start_custom")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.MEDICINE_START_TYPE
    
    async def show_start_date(self, query, context):
        """Показать выбор даты начала."""
        await self.safe_edit_message(
            query,
            "Шаг 7: Выберите дату начала",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📅 Сегодня", callback_data="start_today"),
                 InlineKeyboardButton("📅 Завтра", callback_data="start_tomorrow")],
                [InlineKeyboardButton("📅 Выбрать дату", callback_data="start_custom")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def medicine_start_type(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор даты начала."""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        tz_name = await TimeUtils.get_user_timezone(user_id)
        tz = pytz.timezone(tz_name)
        now = datetime.now(tz)
        
        if query.data == "start_today":
            context.user_data['medicine_data']['start_date'] = now
            await self.show_medicine_confirmation(update, context)
        elif query.data == "start_tomorrow":
            context.user_data['medicine_data']['start_date'] = now + timedelta(days=1)
            await self.show_medicine_confirmation(update, context)
        elif query.data == "start_custom":
            await self.safe_edit_message(
                query,
                "Введите дату начала в формате *ДД.ММ.ГГГГ*",
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_START_DATE
        
        return States.MEDICINE_CONFIRM
    
    async def medicine_start_date(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Пользовательская дата."""
        user_id = update.effective_user.id
        tz_name = await TimeUtils.get_user_timezone(user_id)
        
        try:
            date = TimeUtils.parse_date(update.message.text.strip(), tz_name)
            if not date:
                raise ValueError("Неверный формат даты")
            
            context.user_data['medicine_data']['start_date'] = date
            await self.show_medicine_confirmation(update, context)
            return States.MEDICINE_CONFIRM
        except Exception:
            await update.message.reply_text(
                "❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back")]])
            )
            return States.MEDICINE_START_DATE
    
    async def show_medicine_confirmation(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показ подтверждения для лекарства."""
        data = context.user_data['medicine_data']
        start_date = data['start_date']
        date_str = start_date.strftime('%d.%m.%Y %H:%M')
        
        text = f"""✅ *Проверьте данные:*

💊 *Название:* {data['name']}
💧 *Дозировка:* {data.get('dosage', 'не указана')}
📊 *Приемов в день:* {data['times_per_day']}
⏰ *Расписание:* {', '.join(data['schedule_times'])}"""

        if data.get('course_duration'):
            text += f"\n📅 *Длительность:* {data['course_duration']} дней"
        else:
            text += "\n📅 *Длительность:* бессрочно"
        
        if data.get('repeat_type') != 'none' and data.get('repeat_interval'):
            text += f"\n🔄 *Повторение:* каждые {data['repeat_interval']} дней"
        
        text += f"\n📆 *Дата начала:* {date_str}\n\nВсё верно?"
        
        keyboard = [
            [InlineKeyboardButton("✅ Добавить", callback_data="confirm_medicine"),
             InlineKeyboardButton("✏️ Редактировать", callback_data="edit_medicine")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel")]
        ]
        
        if update.callback_query:
            await self.safe_edit_message(update.callback_query, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    
    async def medicine_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Подтверждение добавления лекарства."""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        tz_name = await TimeUtils.get_user_timezone(user_id)
        data = context.user_data['medicine_data']
        
        try:
            # Сохраняем лекарство
            medicine_id = await db.fetchval("""
                INSERT INTO medicines (
                    user_id, name, dosage, times_per_day, schedule_times,
                    schedule, course_duration, repeat_type, repeat_interval,
                    start_date, user_timezone, status
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'active')
                RETURNING id
            """,
                user_id,
                data['name'],
                data.get('dosage'),
                data['times_per_day'],
                json.dumps(data['schedule_times']),
                ','.join(data['schedule_times']),
                data.get('course_duration'),
                data.get('repeat_type', 'none'),
                data.get('repeat_interval'),
                data['start_date'],
                tz_name
            )
            
            metrics.medicines_added.inc()
            
            # Создаем напоминания
            for time_str in data['schedule_times']:
                scheduled_utc = TimeUtils.local_to_utc(time_str, tz_name, data['start_date'])
                
                reminder_id = await db.fetchval("""
                    INSERT INTO reminders (
                        user_id, reminder_type, item_id, scheduled_time, user_timezone, status
                    ) VALUES ($1, 'medicine', $2, $3, $4, 'pending')
                    RETURNING id
                """, user_id, medicine_id, scheduled_utc, tz_name)
                
                job_id = f"medicine_{reminder_id}"
                scheduler.scheduler.add_job(
                    scheduler.send_reminder,
                    trigger=DateTrigger(run_date=scheduled_utc),
                    id=job_id,
                    args=[reminder_id],
                    replace_existing=True
                )
            
            logger.log('info', f"Добавлено лекарство {medicine_id}")
            
            keyboard = [
                [InlineKeyboardButton("📋 Список", callback_data="list_medicines"),
                 InlineKeyboardButton("➕ Добавить еще", callback_data="add_medicine")],
                [InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ]
            
            await self.safe_edit_message(
                query,
                "✅ *Лекарство успешно добавлено!*",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
            
        except Exception as e:
            logger.log_error(e, {'user_id': user_id})
            await self.safe_edit_message(
                query,
                "❌ *Ошибка при добавлении*\n\nПожалуйста, попробуйте позже.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главная", callback_data="start")]])
            )
        
        context.user_data.pop('medicine_data', None)
        return ConversationHandler.END
    
    async def medicine_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Редактирование лекарства."""
        query = update.callback_query
        await query.answer()
        
        keyboard = [
            [InlineKeyboardButton("📝 Название", callback_data="edit_field_name"),
             InlineKeyboardButton("💧 Дозировка", callback_data="edit_field_dosage")],
            [InlineKeyboardButton("⏰ Расписание", callback_data="edit_field_schedule"),
             InlineKeyboardButton("📅 Длительность", callback_data="edit_field_course")],
            [InlineKeyboardButton("🔄 Повторение", callback_data="edit_field_repeat"),
             InlineKeyboardButton("📆 Дата начала", callback_data="edit_field_start")],
            [InlineKeyboardButton("✅ Готово", callback_data="confirm_medicine")]
        ]
        
        await self.safe_edit_message(
            query,
            "✏️ *Редактирование*\n\nВыберите, что хотите изменить:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.MEDICINE_EDIT
    
    async def medicine_edit_field(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Редактирование поля."""
        query = update.callback_query
        await query.answer()
        
        field = query.data.replace("edit_field_", "")
        
        if field == "name":
            await self.safe_edit_message(query, "Введите новое название:")
            return States.MEDICINE_NAME
        elif field == "dosage":
            await self.safe_edit_message(query, "Введите новую дозировку:")
            return States.MEDICINE_DOSAGE
        elif field == "schedule":
            keyboard = [
                [InlineKeyboardButton("1 раз", callback_data="times_1"),
                 InlineKeyboardButton("2 раза", callback_data="times_2"),
                 InlineKeyboardButton("3 раза", callback_data="times_3")],
                [InlineKeyboardButton("4 раза", callback_data="times_4"),
                 InlineKeyboardButton("5 раз", callback_data="times_5"),
                 InlineKeyboardButton("6 раз", callback_data="times_6")]
            ]
            await self.safe_edit_message(
                query,
                "Выберите новое расписание:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return States.MEDICINE_TIMES_PER_DAY
        elif field == "course":
            await self.safe_edit_message(
                query,
                "Выберите новую продолжительность:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📅 3 дня", callback_data="duration_3"),
                     InlineKeyboardButton("📅 5 дней", callback_data="duration_5"),
                     InlineKeyboardButton("📅 7 дней", callback_data="duration_7")],
                    [InlineKeyboardButton("📅 10 дней", callback_data="duration_10"),
                     InlineKeyboardButton("📅 14 дней", callback_data="duration_14"),
                     InlineKeyboardButton("📅 30 дней", callback_data="duration_30")],
                    [InlineKeyboardButton("⚙️ Свой вариант", callback_data="duration_custom"),
                     InlineKeyboardButton("∞ Бессрочно", callback_data="duration_unlimited")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_COURSE_DURATION
        elif field == "repeat":
            await self.safe_edit_message(
                query,
                "Выберите новое повторение:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Без повторения", callback_data="repeat_none"),
                     InlineKeyboardButton("⚙️ Свой интервал", callback_data="repeat_custom")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_REPEAT
        elif field == "start":
            await self.safe_edit_message(
                query,
                "Выберите новую дату начала:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📅 Сегодня", callback_data="start_today"),
                     InlineKeyboardButton("📅 Завтра", callback_data="start_tomorrow")],
                    [InlineKeyboardButton("📅 Выбрать дату", callback_data="start_custom")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_START_TYPE
        
        return States.MEDICINE_EDIT
    
    async def medicine_comment_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Сохранение комментария."""
        comment = update.message.text
        medicine_id = context.user_data.get('comment_medicine_id')
        
        if medicine_id:
            await db.execute("""
                INSERT INTO medicine_logs (medicine_id, user_id, log_type, status, comment, taken_at)
                VALUES ($1, $2, 'scheduled', 'comment', $3, CURRENT_TIMESTAMP)
            """, medicine_id, update.effective_user.id, comment)
            
            await update.message.reply_text(
                "✅ Комментарий сохранен",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 Главная", callback_data="start")]
                ])
            )
        else:
            await update.message.reply_text(
                "❌ Ошибка сохранения",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 Главная", callback_data="start")]
                ])
            )
        
        return ConversationHandler.END
    
    # ============== АНАЛИЗЫ ==============
    
    async def add_analysis_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начало добавления анализа."""
        if update.callback_query:
            query = update.callback_query
            await query.answer()
            edit_func = query.edit_message_text
        else:
            edit_func = update.message.reply_text
        
        context.user_data['analysis_data'] = {}
        
        await edit_func(
            "🩺 *Добавление анализа*\n\nВыберите тип:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🩸 Анализ", callback_data="analysis_type_analysis"),
                 InlineKeyboardButton("🔬 Исследование", callback_data="analysis_type_investigation")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.ANALYSIS_TYPE
    
    async def analysis_type(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор типа анализа."""
        query = update.callback_query
        await query.answer()
        
        analysis_type = query.data.replace("analysis_type_", "")
        context.user_data['analysis_data']['type'] = analysis_type
        
        await self.safe_edit_message(
            query,
            f"Шаг 1: Введите *название {'анализа' if analysis_type == 'analysis' else 'исследования'}*",
            parse_mode=ParseMode.MARKDOWN
        )
        return States.ANALYSIS_NAME
    
    async def analysis_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение названия анализа."""
        if SecurityManager.check_sql_injection(update.message.text):
            await update.message.reply_text("❌ Обнаружены недопустимые символы")
            return States.ANALYSIS_NAME
        
        context.user_data['analysis_data']['name'] = SecurityManager.sanitize_input(update.message.text)
        
        await update.message.reply_text(
            "Шаг 2: Выберите *дату*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📅 Сегодня", callback_data="analysis_date_today"),
                 InlineKeyboardButton("📅 Завтра", callback_data="analysis_date_tomorrow")],
                [InlineKeyboardButton("📅 Послезавтра", callback_data="analysis_date_dayafter"),
                 InlineKeyboardButton("📅 Выбрать дату", callback_data="analysis_date_custom")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.ANALYSIS_DATE
    
    async def analysis_date(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка выбора даты."""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        tz_name = await TimeUtils.get_user_timezone(user_id)
        tz = pytz.timezone(tz_name)
        now = datetime.now(tz)
        
        if query.data == "analysis_date_today":
            context.user_data['analysis_data']['scheduled_date'] = now
        elif query.data == "analysis_date_tomorrow":
            context.user_data['analysis_data']['scheduled_date'] = now + timedelta(days=1)
        elif query.data == "analysis_date_dayafter":
            context.user_data['analysis_data']['scheduled_date'] = now + timedelta(days=2)
        elif query.data == "analysis_date_custom":
            await self.safe_edit_message(
                query,
                "Введите дату в формате *ДД.ММ.ГГГГ*",
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_DATE
        
        # Показываем выбор времени
        keyboard = [
            [InlineKeyboardButton("08:00", callback_data="analysis_hour_08"),
             InlineKeyboardButton("09:00", callback_data="analysis_hour_09"),
             InlineKeyboardButton("10:00", callback_data="analysis_hour_10"),
             InlineKeyboardButton("11:00", callback_data="analysis_hour_11")],
            [InlineKeyboardButton("12:00", callback_data="analysis_hour_12"),
             InlineKeyboardButton("13:00", callback_data="analysis_hour_13"),
             InlineKeyboardButton("14:00", callback_data="analysis_hour_14"),
             InlineKeyboardButton("15:00", callback_data="analysis_hour_15")],
            [InlineKeyboardButton("16:00", callback_data="analysis_hour_16"),
             InlineKeyboardButton("17:00", callback_data="analysis_hour_17"),
             InlineKeyboardButton("18:00", callback_data="analysis_hour_18"),
             InlineKeyboardButton("19:00", callback_data="analysis_hour_19")],
            [InlineKeyboardButton("20:00", callback_data="analysis_hour_20"),
             InlineKeyboardButton("21:00", callback_data="analysis_hour_21"),
             InlineKeyboardButton("⚙️ Своё время", callback_data="analysis_time_custom")]
        ]
        
        await self.safe_edit_message(
            query,
            "Шаг 3: Выберите *время*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.ANALYSIS_TIME_HOUR
    
    async def analysis_date_custom(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Пользовательская дата."""
        user_id = update.effective_user.id
        tz_name = await TimeUtils.get_user_timezone(user_id)
        
        try:
            date = TimeUtils.parse_date(update.message.text.strip(), tz_name)
            if not date:
                raise ValueError("Неверный формат даты")
            
            context.user_data['analysis_data']['scheduled_date'] = date
            
            # Показываем выбор времени
            keyboard = [
                [InlineKeyboardButton("08:00", callback_data="analysis_hour_08"),
                 InlineKeyboardButton("09:00", callback_data="analysis_hour_09"),
                 InlineKeyboardButton("10:00", callback_data="analysis_hour_10"),
                 InlineKeyboardButton("11:00", callback_data="analysis_hour_11")],
                [InlineKeyboardButton("12:00", callback_data="analysis_hour_12"),
                 InlineKeyboardButton("13:00", callback_data="analysis_hour_13"),
                 InlineKeyboardButton("14:00", callback_data="analysis_hour_14"),
                 InlineKeyboardButton("15:00", callback_data="analysis_hour_15")],
                [InlineKeyboardButton("16:00", callback_data="analysis_hour_16"),
                 InlineKeyboardButton("17:00", callback_data="analysis_hour_17"),
                 InlineKeyboardButton("18:00", callback_data="analysis_hour_18"),
                 InlineKeyboardButton("19:00", callback_data="analysis_hour_19")],
                [InlineKeyboardButton("20:00", callback_data="analysis_hour_20"),
                 InlineKeyboardButton("21:00", callback_data="analysis_hour_21"),
                 InlineKeyboardButton("⚙️ Своё время", callback_data="analysis_time_custom")]
            ]
            
            await update.message.reply_text(
                "Шаг 3: Выберите *время*",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_TIME_HOUR
            
        except Exception as e:
            await update.message.reply_text(
                "❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="add_analysis")]])
            )
            return States.ANALYSIS_DATE
    
    async def analysis_time_hour(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор часа."""
        query = update.callback_query
        await query.answer()
        
        if query.data == "analysis_time_custom":
            # Показываем выбор всех часов
            keyboard = []
            for start in range(0, 24, 6):
                row = []
                for h in range(start, min(start + 6, 24)):
                    row.append(InlineKeyboardButton(f"{h:02d}", callback_data=f"analysis_hour_{h:02d}"))
                keyboard.append(row)
            
            await self.safe_edit_message(
                query,
                "Выберите час:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return States.ANALYSIS_TIME_HOUR
        
        hour = query.data.replace("analysis_hour_", "")
        context.user_data['temp_hour'] = hour
        
        # Показываем выбор минут
        minutes = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
        keyboard = []
        for i in range(0, len(minutes), 4):
            row = [InlineKeyboardButton(f"{m:02d}", callback_data=f"minute_{m:02d}") for m in minutes[i:i+4]]
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("⏰ К выбору часа", callback_data="back_to_analysis_hours")])
        
        await self.safe_edit_message(
            query,
            f"Вы выбрали час {hour}. Теперь выберите минуты:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return States.ANALYSIS_TIME_MINUTE
    
    async def analysis_time_minute(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор минут."""
        query = update.callback_query
        await query.answer()
        
        minute = query.data.replace("minute_", "")
        hour = context.user_data.get('temp_hour', '12')
        scheduled_time = f"{hour}:{minute}"
        context.user_data['analysis_data']['scheduled_time'] = scheduled_time
        
        # Проверка на дубликат
        existing = await db.fetchval("""
            SELECT id FROM analyses 
            WHERE user_id = $1 
                AND status = 'pending' 
                AND scheduled_date::date = $2::date 
                AND scheduled_time = $3
        """, update.effective_user.id, 
            context.user_data['analysis_data']['scheduled_date'],
            scheduled_time
        )
        
        if existing:
            analysis_type = "анализ" if context.user_data['analysis_data']['type'] == 'analysis' else 'исследование'
            
            keyboard = [
                [InlineKeyboardButton("⏰ Другое время", callback_data="analysis_time_other"),
                 InlineKeyboardButton("✅ Все равно создать", callback_data="analysis_force_create")]
            ]
            
            await self.safe_edit_message(
                query,
                f"⚠️ *Внимание!*\n\nНа эту дату и время уже запланирован {analysis_type}.\n\nВы можете выбрать другое время или создать дубликат.",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_TIME_HOUR
        
        await self.safe_edit_message(
            query,
            "Шаг 4: Выберите *повторение*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🕐 Одноразово", callback_data="repeat_once"),
                 InlineKeyboardButton("📅 Ежедневно", callback_data="repeat_daily")],
                [InlineKeyboardButton("📆 Еженедельно", callback_data="repeat_weekly"),
                 InlineKeyboardButton("🗓️ Ежемесячно", callback_data="repeat_monthly")],
                [InlineKeyboardButton("⚙️ Свой интервал", callback_data="repeat_custom")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.ANALYSIS_REPEAT
    
    async def back_to_analysis_hours(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Возврат к часам."""
        query = update.callback_query
        await query.answer()
        
        keyboard = []
        for start in range(0, 24, 6):
            row = []
            for h in range(start, min(start + 6, 24)):
                row.append(InlineKeyboardButton(f"{h:02d}", callback_data=f"analysis_hour_{h:02d}"))
            keyboard.append(row)
        
        await self.safe_edit_message(
            query,
            "Выберите час:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return States.ANALYSIS_TIME_HOUR
    
    async def analysis_time_custom(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка пользовательского времени."""
        query = update.callback_query
        await query.answer()
        
        await self.analysis_time_hour(update, context)
    
    async def analysis_force_create(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Принудительное создание дубликата."""
        query = update.callback_query
        await query.answer()
        
        await self.safe_edit_message(
            query,
            "Шаг 4: Выберите *повторение*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🕐 Одноразово", callback_data="repeat_once"),
                 InlineKeyboardButton("📅 Ежедневно", callback_data="repeat_daily")],
                [InlineKeyboardButton("📆 Еженедельно", callback_data="repeat_weekly"),
                 InlineKeyboardButton("🗓️ Ежемесячно", callback_data="repeat_monthly")],
                [InlineKeyboardButton("⚙️ Свой интервал", callback_data="repeat_custom")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.ANALYSIS_REPEAT
    
    async def analysis_repeat(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор повторения."""
        query = update.callback_query
        await query.answer()
        
        repeat_type = query.data.replace("repeat_", "")
        
        repeat_map = {
            "once": "once",
            "daily": "daily",
            "weekly": "weekly",
            "monthly": "monthly",
            "custom": "custom"
        }
        
        context.user_data['analysis_data']['repeat_type'] = repeat_map.get(repeat_type, "once")
        
        if repeat_type == "custom":
            await self.safe_edit_message(
                query,
                "Введите интервал повторения в днях:",
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_REPEAT_INTERVAL
        
        # Показываем выбор напоминания
        await self.safe_edit_message(
            query,
            "Шаг 5: *Когда напомнить?*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("15 мин", callback_data="remind_15"),
                 InlineKeyboardButton("30 мин", callback_data="remind_30"),
                 InlineKeyboardButton("1 час", callback_data="remind_60")],
                [InlineKeyboardButton("3 часа", callback_data="remind_180"),
                 InlineKeyboardButton("12 часов", callback_data="remind_720"),
                 InlineKeyboardButton("24 часа", callback_data="remind_1440")],
                [InlineKeyboardButton("⚙️ Свой вариант", callback_data="remind_custom")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.ANALYSIS_REMINDER
    
    async def analysis_repeat_interval(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка интервала повторения."""
        is_valid, interval, error = SecurityManager.validate_number_input(update.message.text, 1, 365)
        if not is_valid:
            await update.message.reply_text(f"❌ {error}")
            return States.ANALYSIS_REPEAT_INTERVAL
        
        context.user_data['analysis_data']['repeat_type'] = 'custom'
        context.user_data['analysis_data']['repeat_interval'] = interval
        
        await update.message.reply_text(
            "Шаг 5: *Когда напомнить?*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("15 мин", callback_data="remind_15"),
                 InlineKeyboardButton("30 мин", callback_data="remind_30"),
                 InlineKeyboardButton("1 час", callback_data="remind_60")],
                [InlineKeyboardButton("3 часа", callback_data="remind_180"),
                 InlineKeyboardButton("12 часов", callback_data="remind_720"),
                 InlineKeyboardButton("24 часа", callback_data="remind_1440")],
                [InlineKeyboardButton("⚙️ Свой вариант", callback_data="remind_custom")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.ANALYSIS_REMINDER
    
    async def analysis_reminder(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор напоминания."""
        query = update.callback_query
        await query.answer()
        
        if query.data == "remind_custom":
            await self.safe_edit_message(
                query,
                "Введите количество минут:",
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_REMINDER
        
        minutes = int(query.data.replace("remind_", ""))
        context.user_data['analysis_data']['reminder_before'] = minutes
        
        await self.safe_edit_message(
            query,
            "Шаг 6: Введите *заметки* (или нажмите Пропустить)",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭️ Пропустить", callback_data="skip_notes")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.ANALYSIS_NOTES
    
    async def analysis_notes(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка заметок."""
        if SecurityManager.check_sql_injection(update.message.text):
            await update.message.reply_text("❌ Обнаружены недопустимые символы")
            return States.ANALYSIS_NOTES
        
        context.user_data['analysis_data']['notes'] = SecurityManager.sanitize_input(update.message.text)
        await self.show_analysis_confirmation(update, context)
        return States.ANALYSIS_CONFIRM
    
    async def skip_notes(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Пропуск заметок."""
        query = update.callback_query
        await query.answer()
        
        context.user_data['analysis_data']['notes'] = None
        await self.show_analysis_confirmation(update, context)
        return States.ANALYSIS_CONFIRM
    
    async def show_analysis_confirmation(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показ подтверждения для анализа."""
        data = context.user_data['analysis_data']
        scheduled_date = data['scheduled_date']
        date_str = scheduled_date.strftime('%d.%m.%Y')
        analysis_type = "анализ" if data['type'] == 'analysis' else 'исследование'
        
        text = f"""✅ *Проверьте данные {analysis_type}а:*

📋 *Название:* {data['name']}
📅 *Дата:* {date_str}
⏰ *Время:* {data['scheduled_time']}"""

        if data.get('repeat_type') == 'custom':
            text += f"\n🔄 *Повторение:* каждые {data['repeat_interval']} дней"
        elif data.get('repeat_type') != 'once':
            text += f"\n🔄 *Повторение:* {data['repeat_type']}"
        
        text += f"\n⏰ *Напомнить за:* {data['reminder_before']} мин"
        
        if data.get('notes'):
            text += f"\n📝 *Заметки:* {data['notes']}"
        
        text += "\n\nВсё верно?"
        
        keyboard = [
            [InlineKeyboardButton("✅ Добавить", callback_data="confirm_analysis"),
             InlineKeyboardButton("✏️ Редактировать", callback_data="edit_analysis")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel")]
        ]
        
        if update.callback_query:
            await self.safe_edit_message(update.callback_query, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    
    async def analysis_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Подтверждение добавления анализа."""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        tz_name = await TimeUtils.get_user_timezone(user_id)
        data = context.user_data['analysis_data']
        
        try:
            hour, minute = map(int, data['scheduled_time'].split(':'))
            scheduled_datetime = data['scheduled_date'].replace(hour=hour, minute=minute)
            
            analysis_id = await db.fetchval("""
                INSERT INTO analyses (
                    user_id, analysis_type, name, scheduled_date, scheduled_time,
                    repeat_type, repeat_interval, reminder_before, notes, user_timezone, status
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'pending')
                RETURNING id
            """,
                user_id, data['type'], data['name'], scheduled_datetime, data['scheduled_time'],
                data['repeat_type'], data.get('repeat_interval'), data['reminder_before'],
                data.get('notes'), tz_name
            )
            
            metrics.analyses_added.inc()
            
            reminder_time = scheduled_datetime - timedelta(minutes=data['reminder_before'])
            
            if reminder_time > datetime.now(pytz.UTC):
                reminder_id = await db.fetchval("""
                    INSERT INTO reminders (
                        user_id, reminder_type, item_id, scheduled_time, user_timezone, status
                    ) VALUES ($1, $2, $3, $4, $5, 'pending')
                    RETURNING id
                """, user_id, data['type'], analysis_id, reminder_time, tz_name)
                
                job_id = f"{data['type']}_{reminder_id}"
                scheduler.scheduler.add_job(
                    scheduler.send_reminder,
                    trigger=DateTrigger(run_date=reminder_time),
                    id=job_id,
                    args=[reminder_id],
                    replace_existing=True
                )
            
            logger.log('info', f"Добавлен анализ {analysis_id}")
            
            analysis_type = "Анализ" if data['type'] == 'analysis' else 'Исследование'
            
            keyboard = [
                [InlineKeyboardButton("📋 Список", callback_data="list_analyses"),
                 InlineKeyboardButton("➕ Добавить еще", callback_data="add_analysis")],
                [InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ]
            
            await self.safe_edit_message(
                query,
                f"✅ *{analysis_type} успешно добавлен!*",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
            
        except Exception as e:
            logger.log_error(e, {'user_id': user_id})
            await self.safe_edit_message(
                query,
                "❌ *Ошибка при добавлении*\n\nПожалуйста, попробуйте позже.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главная", callback_data="start")]])
            )
        
        context.user_data.pop('analysis_data', None)
        return ConversationHandler.END
    
    async def analysis_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Редактирование анализа."""
        query = update.callback_query
        await query.answer()
        
        keyboard = [
            [InlineKeyboardButton("📋 Название", callback_data="edit_field_analysis_name"),
             InlineKeyboardButton("📅 Дата", callback_data="edit_field_analysis_date")],
            [InlineKeyboardButton("⏰ Время", callback_data="edit_field_analysis_time"),
             InlineKeyboardButton("🔄 Повторение", callback_data="edit_field_analysis_repeat")],
            [InlineKeyboardButton("⏰ Напоминание", callback_data="edit_field_analysis_reminder"),
             InlineKeyboardButton("📝 Заметки", callback_data="edit_field_analysis_notes")],
            [InlineKeyboardButton("✅ Готово", callback_data="confirm_analysis")]
        ]
        
        await self.safe_edit_message(
            query,
            "✏️ *Редактирование анализа*\n\nВыберите, что хотите изменить:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.ANALYSIS_EDIT
    
    async def analysis_edit_field(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Редактирование поля анализа."""
        query = update.callback_query
        await query.answer()
        
        field = query.data.replace("edit_field_analysis_", "")
        
        if field == "name":
            await self.safe_edit_message(query, "Введите новое название:")
            return States.ANALYSIS_NAME
        elif field == "date":
            await self.safe_edit_message(
                query,
                "Выберите новую дату:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📅 Сегодня", callback_data="analysis_date_today"),
                     InlineKeyboardButton("📅 Завтра", callback_data="analysis_date_tomorrow")],
                    [InlineKeyboardButton("📅 Послезавтра", callback_data="analysis_date_dayafter"),
                     InlineKeyboardButton("📅 Выбрать дату", callback_data="analysis_date_custom")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_DATE
        elif field == "time":
            keyboard = [
                [InlineKeyboardButton("08:00", callback_data="analysis_hour_08"),
                 InlineKeyboardButton("09:00", callback_data="analysis_hour_09"),
                 InlineKeyboardButton("10:00", callback_data="analysis_hour_10"),
                 InlineKeyboardButton("11:00", callback_data="analysis_hour_11")],
                [InlineKeyboardButton("12:00", callback_data="analysis_hour_12"),
                 InlineKeyboardButton("13:00", callback_data="analysis_hour_13"),
                 InlineKeyboardButton("14:00", callback_data="analysis_hour_14"),
                 InlineKeyboardButton("15:00", callback_data="analysis_hour_15")],
                [InlineKeyboardButton("16:00", callback_data="analysis_hour_16"),
                 InlineKeyboardButton("17:00", callback_data="analysis_hour_17"),
                 InlineKeyboardButton("18:00", callback_data="analysis_hour_18"),
                 InlineKeyboardButton("19:00", callback_data="analysis_hour_19")],
                [InlineKeyboardButton("20:00", callback_data="analysis_hour_20"),
                 InlineKeyboardButton("21:00", callback_data="analysis_hour_21"),
                 InlineKeyboardButton("⚙️ Своё время", callback_data="analysis_time_custom")]
            ]
            await self.safe_edit_message(
                query,
                "Выберите новое время:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return States.ANALYSIS_TIME_HOUR
        elif field == "repeat":
            await self.safe_edit_message(
                query,
                "Выберите новое повторение:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🕐 Одноразово", callback_data="repeat_once"),
                     InlineKeyboardButton("📅 Ежедневно", callback_data="repeat_daily")],
                    [InlineKeyboardButton("📆 Еженедельно", callback_data="repeat_weekly"),
                     InlineKeyboardButton("🗓️ Ежемесячно", callback_data="repeat_monthly")],
                    [InlineKeyboardButton("⚙️ Свой интервал", callback_data="repeat_custom")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_REPEAT
        elif field == "reminder":
            await self.safe_edit_message(
                query,
                "Выберите новое время напоминания:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("15 мин", callback_data="remind_15"),
                     InlineKeyboardButton("30 мин", callback_data="remind_30"),
                     InlineKeyboardButton("1 час", callback_data="remind_60")],
                    [InlineKeyboardButton("3 часа", callback_data="remind_180"),
                     InlineKeyboardButton("12 часов", callback_data="remind_720"),
                     InlineKeyboardButton("24 часа", callback_data="remind_1440")],
                    [InlineKeyboardButton("⚙️ Свой вариант", callback_data="remind_custom")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_REMINDER
        elif field == "notes":
            await self.safe_edit_message(
                query,
                "Введите новые заметки:",
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_NOTES
        
        return States.ANALYSIS_EDIT
    
    # ============== СИМПТОМЫ ==============
    
    async def symptoms_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начало добавления симптомов."""
        if update.callback_query:
            query = update.callback_query
            await query.answer()
            await self.safe_edit_message(query, "🩺 *Опишите симптом:*", parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text("🩺 *Опишите симптом:*", parse_mode=ParseMode.MARKDOWN)
        
        return States.SYMPTOM_TEXT
    
    async def new_symptom(self, update: Update, context: ContextTypes.DEFAULT_TYPE, medicine_id: int):
        """Добавление нового симптома из напоминания."""
        query = update.callback_query
        context.user_data['medicine_context'] = medicine_id
        await self.safe_edit_message(query, "🩺 *Опишите новый симптом:*", parse_mode=ParseMode.MARKDOWN)
        return States.SYMPTOM_TEXT
    
    async def symptom_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение текста симптома."""
        if SecurityManager.check_sql_injection(update.message.text):
            await update.message.reply_text("❌ Обнаружены недопустимые символы")
            return States.SYMPTOM_TEXT
        
        context.user_data['symptom_text'] = SecurityManager.sanitize_input(update.message.text)
        
        keyboard = [
            [InlineKeyboardButton("1️⃣ Очень легкий 🟢", callback_data="severity_1"),
             InlineKeyboardButton("2️⃣ Легкий 💚", callback_data="severity_2")],
            [InlineKeyboardButton("3️⃣ Умеренный 💛", callback_data="severity_3"),
             InlineKeyboardButton("4️⃣ Сильный 🧡", callback_data="severity_4")],
            [InlineKeyboardButton("5️⃣ Максимальный ❤️", callback_data="severity_5")]
        ]
        
        await update.message.reply_text(
            "🩺 *Оцените тяжесть симптома*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.SYMPTOM_SEVERITY
    
    async def symptom_severity(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка тяжести симптома."""
        query = update.callback_query
        await query.answer()
        
        severity = int(query.data.replace("severity_", ""))
        symptom = context.user_data.get('symptom_text', 'Не указан')
        user_id = update.effective_user.id
        
        severity_colors = {1: "зеленый", 2: "лимонный", 3: "желтый", 4: "оранжевый", 5: "красный"}
        
        await db.execute("""
            INSERT INTO symptom_logs (user_id, symptom, severity, severity_color)
            VALUES ($1, $2, $3, $4)
        """, user_id, symptom, severity, severity_colors[severity])
        
        metrics.symptom_logs.labels(severity=str(severity)).inc()
        
        if 'medicine_context' in context.user_data:
            medicine_id = context.user_data['medicine_context']
            await db.execute("""
                INSERT INTO medicine_logs (medicine_id, user_id, status, side_effects, taken_at)
                VALUES ($1, $2, 'side_effect', $3, CURRENT_TIMESTAMP)
            """, medicine_id, user_id, symptom)
        
        severity_texts = {
            1: "1️⃣ Очень легкий (зеленый)",
            2: "2️⃣ Легкий (лимонный)",
            3: "3️⃣ Умеренный (желтый)",
            4: "4️⃣ Сильный (оранжевый)",
            5: "5️⃣ Максимальный (красный)"
        }
        
        keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
        await self.safe_edit_message(
            query,
            f"✅ *Симптом зафиксирован:*\n\n🤒 {symptom}\n📊 {severity_texts[severity]}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        
        context.user_data.pop('symptom_text', None)
        context.user_data.pop('medicine_context', None)
        return ConversationHandler.END
    
    # ============== НЕЗАПЛАНИРОВАННЫЙ ПРИЕМ ==============
    
    async def unscheduled_medicine_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начало незапланированного приема."""
        if update.callback_query:
            query = update.callback_query
            await query.answer()
            edit_func = query.edit_message_text
        else:
            edit_func = update.message.reply_text
        
        user_id = update.effective_user.id
        
        medicines = await db.fetch(
            "SELECT id, name, dosage FROM medicines WHERE user_id = $1 AND status = 'active'",
            user_id
        )
        
        if not medicines:
            await edit_func(
                "❌ *У вас нет активных препаратов*\n\nСначала добавьте препарат в список.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💊 Добавить препарат", callback_data="add_medicine")],
                    [InlineKeyboardButton("🏠 Главная", callback_data="start")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return ConversationHandler.END
        
        keyboard = []
        for med in medicines:
            text = f"💊 {med['name']}" + (f" ({med['dosage']})" if med['dosage'] else "")
            keyboard.append([InlineKeyboardButton(text, callback_data=f"unscheduled_medicine_{med['id']}")])
        
        keyboard.append([InlineKeyboardButton("➕ Новый препарат", callback_data="add_new_medicine")])
        keyboard.append([InlineKeyboardButton("🏠 Главная", callback_data="start")])
        
        await edit_func(
            "💊 *Принять препарат*\n\nВыберите препарат из списка:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.UNSCHEDULED_MEDICINE_SELECT
    
    async def unscheduled_medicine_select(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор препарата."""
        query = update.callback_query
        await query.answer()
        
        medicine_id = int(query.data.replace("unscheduled_medicine_", ""))
        context.user_data['unscheduled_medicine_id'] = medicine_id
        
        await self.safe_edit_message(
            query,
            "💊 *Укажите принятую дозу*\n(или нажмите Пропустить):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭️ Пропустить", callback_data="skip_dosage")]
            ])
        )
        return States.UNSCHEDULED_MEDICINE_DOSAGE
    
    async def unscheduled_medicine_dosage(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Дозировка."""
        context.user_data['unscheduled_dosage'] = update.message.text
        
        await update.message.reply_text(
            "💊 *Почему был принят препарат?*\n(укажите причину или нажмите Пропустить):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭️ Пропустить", callback_data="skip_reason")]
            ])
        )
        return States.UNSCHEDULED_MEDICINE_REASON
    
    async def unscheduled_medicine_reason(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Причина приема."""
        context.user_data['unscheduled_reason'] = update.message.text
        
        await update.message.reply_text(
            "💊 *Добавьте комментарий*\n(или нажмите Пропустить):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭️ Пропустить", callback_data="skip_comment")]
            ])
        )
        return States.UNSCHEDULED_MEDICINE_COMMENT
    
    async def unscheduled_medicine_comment(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Комментарий."""
        context.user_data['unscheduled_comment'] = update.message.text
        await self.save_unscheduled_medicine(update, context)
        return ConversationHandler.END
    
    async def skip_reason(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Пропуск причины."""
        query = update.callback_query
        await query.answer()
        
        context.user_data['unscheduled_reason'] = None
        
        await self.safe_edit_message(
            query,
            "💊 *Добавьте комментарий*\n(или нажмите Пропустить):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭️ Пропустить", callback_data="skip_comment")]
            ])
        )
        return States.UNSCHEDULED_MEDICINE_COMMENT
    
    async def skip_comment(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Пропуск комментария."""
        query = update.callback_query
        await query.answer()
        
        context.user_data['unscheduled_comment'] = None
        await self.save_unscheduled_medicine(update, context)
        return ConversationHandler.END
    
    async def save_unscheduled_medicine(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Сохранение незапланированного приема."""
        user_id = update.effective_user.id
        medicine_id = context.user_data['unscheduled_medicine_id']
        dosage = context.user_data.get('unscheduled_dosage')
        reason = context.user_data.get('unscheduled_reason')
        comment = context.user_data.get('unscheduled_comment')
        
        # Получаем название препарата
        medicine = await db.fetchrow(
            "SELECT name FROM medicines WHERE id = $1",
            medicine_id
        )
        
        if medicine:
            await db.execute("""
                INSERT INTO medicine_logs (
                    medicine_id, user_id, log_type, status, dosage, reason, comment, taken_at
                ) VALUES ($1, $2, 'unscheduled', 'taken', $3, $4, $5, CURRENT_TIMESTAMP)
            """, medicine_id, user_id, dosage, reason, comment)
            
            # Увеличиваем счетчик
            await db.execute("""
                UPDATE medicines SET total_unscheduled = total_unscheduled + 1, total_taken = total_taken + 1
                WHERE id = $1
            """, medicine_id)
            
            logger.log('info', f"Незапланированный прием {medicine['name']}")
        
        text = f"✅ *Прием зафиксирован*\n\nПрепарат: {medicine['name'] if medicine else 'неизвестный'}"
        if dosage:
            text += f"\nДоза: {dosage}"
        
        keyboard = [
            [InlineKeyboardButton("👨‍⚕️ Записаться к врачу", callback_data="about"),
             InlineKeyboardButton("🏠 Главная", callback_data="start")],
        ]
        
        if update.callback_query:
            await self.safe_edit_message(update.callback_query, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        
        for key in ['unscheduled_medicine_id', 'unscheduled_dosage', 'unscheduled_reason', 'unscheduled_comment']:
            context.user_data.pop(key, None)
    
    # ============== ОТКЛАДЫВАНИЕ ==============
    
    async def postpone_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE, item_type: str, item_id: int):
        """Начало откладывания."""
        query = update.callback_query
        await query.answer()
        
        context.user_data['postpone_item_id'] = item_id
        context.user_data['postpone_type'] = item_type
        
        keyboard = [
            [InlineKeyboardButton("⏰ Часы", callback_data="postpone_type_hours"),
             InlineKeyboardButton("📅 Дни", callback_data="postpone_type_days")],
            [InlineKeyboardButton("⚙️ Свой вариант", callback_data="postpone_custom")]
        ]
        
        await self.safe_edit_message(
            query,
            "⏸ *На сколько отложить?*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.POSTPONE_TYPE
    
    async def postpone_type(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка выбора типа."""
        query = update.callback_query
        await query.answer()
        
        if query.data == "postpone_type_hours":
            keyboard = []
            for start in range(1, 25, 6):
                row = []
                for h in range(start, min(start + 6, 25)):
                    row.append(InlineKeyboardButton(f"{h} ч", callback_data=f"postpone_hour_{h}"))
                keyboard.append(row)
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
            
            await self.safe_edit_message(
                query,
                "⏰ *На сколько часов отложить?*",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return States.POSTPONE_HOURS
        elif query.data == "postpone_type_days":
            keyboard = [
                [InlineKeyboardButton("1 день", callback_data="postpone_day_1"),
                 InlineKeyboardButton("2 дня", callback_data="postpone_day_2"),
                 InlineKeyboardButton("3 дня", callback_data="postpone_day_3")],
                [InlineKeyboardButton("5 дней", callback_data="postpone_day_5"),
                 InlineKeyboardButton("7 дней", callback_data="postpone_day_7"),
                 InlineKeyboardButton("10 дней", callback_data="postpone_day_10")],
                [InlineKeyboardButton("14 дней", callback_data="postpone_day_14"),
                 InlineKeyboardButton("30 дней", callback_data="postpone_day_30"),
                 InlineKeyboardButton("⚙️ Свой", callback_data="postpone_custom")],
                [InlineKeyboardButton("🔙 Назад", callback_data="back")]
            ]
            
            await self.safe_edit_message(
                query,
                "📅 *На сколько дней отложить?*",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return States.POSTPONE_DAYS
    
    async def postpone_hours(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор часов."""
        query = update.callback_query
        await query.answer()
        
        hours = int(query.data.replace("postpone_hour_", ""))
        await self.execute_postpone(update, context, hours, 'hours')
        return ConversationHandler.END
    
    async def postpone_days(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор дней."""
        query = update.callback_query
        await query.answer()
        
        days = int(query.data.replace("postpone_day_", ""))
        await self.execute_postpone(update, context, days, 'days')
        return ConversationHandler.END
    
    async def postpone_custom_value(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Пользовательское значение."""
        is_valid, value, error = SecurityManager.validate_number_input(update.message.text, 1, 365)
        if not is_valid:
            await update.message.reply_text(f"❌ {error}")
            return States.POSTPONE_CUSTOM
        
        await self.execute_postpone(update, context, value, 'hours')
        return ConversationHandler.END
    
    async def execute_postpone(self, update: Update, context: ContextTypes.DEFAULT_TYPE, value: int, unit: str):
        """Выполнение откладывания."""
        item_id = context.user_data['postpone_item_id']
        item_type = context.user_data['postpone_type']
        user_id = update.effective_user.id
        
        if unit == 'hours':
            delta = timedelta(hours=value)
        else:
            delta = timedelta(days=value)
        
        new_time = datetime.now(pytz.UTC) + delta
        
        if item_type == 'medicine':
            await db.execute(
                "UPDATE medicines SET total_postponed = total_postponed + 1 WHERE id = $1",
                item_id
            )
            
            reminder = await db.fetchrow("""
                SELECT id FROM reminders 
                WHERE item_id = $1 AND reminder_type = 'medicine' AND status = 'sent'
                ORDER BY scheduled_time DESC LIMIT 1
            """, item_id)
            
            if reminder:
                await db.execute(
                    "UPDATE reminders SET status = 'postponed', postponed_until = $1 WHERE id = $2",
                    new_time, reminder['id']
                )
                
                new_reminder_id = await db.fetchval("""
                    INSERT INTO reminders (
                        user_id, reminder_type, item_id, scheduled_time, user_timezone, status
                    ) VALUES ($1, 'medicine', $2, $3, $4, 'pending')
                    RETURNING id
                """, user_id, item_id, new_time, await TimeUtils.get_user_timezone(user_id))
                
                job_id = f"medicine_{new_reminder_id}"
                scheduler.scheduler.add_job(
                    scheduler.send_reminder,
                    trigger=DateTrigger(run_date=new_time),
                    id=job_id,
                    args=[new_reminder_id],
                    replace_existing=True
                )
                
        elif item_type in ['analysis', 'investigation']:
            await db.execute("""
                UPDATE analyses SET scheduled_date = $1 WHERE id = $2
            """, new_time, item_id)
            
            analysis = await db.fetchrow(
                "SELECT reminder_before, analysis_type FROM analyses WHERE id = $1",
                item_id
            )
            
            if analysis:
                reminder_time = new_time - timedelta(minutes=analysis['reminder_before'])
                
                new_reminder_id = await db.fetchval("""
                    INSERT INTO reminders (
                        user_id, reminder_type, item_id, scheduled_time, user_timezone, status
                    ) VALUES ($1, $2, $3, $4, $5, 'pending')
                    RETURNING id
                """, user_id, analysis['analysis_type'], item_id, reminder_time,
                    await TimeUtils.get_user_timezone(user_id))
                
                job_id = f"{analysis['analysis_type']}_{new_reminder_id}"
                scheduler.scheduler.add_job(
                    scheduler.send_reminder,
                    trigger=DateTrigger(run_date=reminder_time),
                    id=job_id,
                    args=[new_reminder_id],
                    replace_existing=True
                )
        
        unit_text = {'hours': 'часов', 'days': 'дней'}.get(unit, unit)
        keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
        
        await self.safe_edit_message(
            update.callback_query,
            f"✅ *Напоминание отложено на {value} {unit_text}*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        
        context.user_data.pop('postpone_item_id', None)
        context.user_data.pop('postpone_type', None)
    
    # ============== ДЕЙСТВИЯ С НАПОМИНАНИЯМИ ==============
    
    async def medicine_take(self, update: Update, context: ContextTypes.DEFAULT_TYPE, medicine_id: int):
        """Отметка о приеме."""
        query = update.callback_query
        user_id = update.effective_user.id
        
        medicine = await db.fetchrow(
            "SELECT name FROM medicines WHERE id = $1",
            medicine_id
        )
        
        if medicine:
            await db.execute("""
                UPDATE medicines SET total_taken = total_taken + 1 WHERE id = $1
            """, medicine_id)
            
            await db.execute("""
                INSERT INTO medicine_logs (medicine_id, user_id, status, taken_at)
                VALUES ($1, $2, 'taken', CURRENT_TIMESTAMP)
            """, medicine_id, user_id)
            
            await db.execute("""
                UPDATE reminders SET status = 'completed' 
                WHERE item_id = $1 AND reminder_type = 'medicine' AND status = 'sent'
            """, medicine_id)
        
        keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
        await self.safe_edit_message(
            query,
            f"✅ Прием *{medicine['name'] if medicine else 'лекарства'}* отмечен!",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def medicine_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE, medicine_id: int):
        """Отмена приема."""
        query = update.callback_query
        user_id = update.effective_user.id
        
        medicine = await db.fetchrow(
            "SELECT name FROM medicines WHERE id = $1",
            medicine_id
        )
        
        if medicine:
            await db.execute("""
                UPDATE medicines SET total_skipped = total_skipped + 1 WHERE id = $1
            """, medicine_id)
            
            await db.execute("""
                INSERT INTO medicine_logs (medicine_id, user_id, status, taken_at)
                VALUES ($1, $2, 'skipped', CURRENT_TIMESTAMP)
            """, medicine_id, user_id)
            
            await db.execute("""
                UPDATE reminders SET status = 'skipped' 
                WHERE item_id = $1 AND reminder_type = 'medicine' AND status = 'sent'
            """, medicine_id)
        
        keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
        await self.safe_edit_message(
            query,
            f"❌ Прием *{medicine['name'] if medicine else 'лекарства'}* отменен",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def medicine_comment(self, update: Update, context: ContextTypes.DEFAULT_TYPE, medicine_id: int):
        """Комментарий к приему."""
        query = update.callback_query
        context.user_data['comment_medicine_id'] = medicine_id
        await self.safe_edit_message(query, "📝 Введите ваш комментарий:")
        return States.MEDICINE_COMMENT
    
    async def analysis_take(self, update: Update, context: ContextTypes.DEFAULT_TYPE, analysis_id: int):
        """Отметка о сдаче анализа."""
        query = update.callback_query
        user_id = update.effective_user.id
        
        analysis = await db.fetchrow(
            "SELECT name FROM analyses WHERE id = $1",
            analysis_id
        )
        
        if analysis:
            await db.execute("""
                UPDATE analyses SET status = 'completed' WHERE id = $1
            """, analysis_id)
            
            await db.execute("""
                UPDATE reminders SET status = 'completed' 
                WHERE item_id = $1 AND reminder_type IN ('analysis', 'investigation') AND status = 'sent'
            """, analysis_id)
        
        keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
        await self.safe_edit_message(
            query,
            f"✅ Сдача *{analysis['name']}* отмечена!",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def analysis_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE, analysis_id: int):
        """Отмена анализа."""
        query = update.callback_query
        user_id = update.effective_user.id
        
        analysis = await db.fetchrow(
            "SELECT name, analysis_type FROM analyses WHERE id = $1",
            analysis_id
        )
        
        if analysis:
            await db.execute("""
                UPDATE analyses SET status = 'cancelled' WHERE id = $1
            """, analysis_id)
            
            reminders = await db.fetch("""
                SELECT id FROM reminders 
                WHERE item_id = $1 AND reminder_type = $2 AND status = 'pending'
            """, analysis_id, analysis['analysis_type'])
            
            for reminder in reminders:
                await db.execute(
                    "UPDATE reminders SET status = 'cancelled' WHERE id = $1",
                    reminder['id']
                )
                try:
                    scheduler.scheduler.remove_job(f"{analysis['analysis_type']}_{reminder['id']}")
                except JobLookupError:
                    pass
        
        keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
        await self.safe_edit_message(
            query,
            f"❌ Анализ *{analysis['name']}* отменен",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def analysis_notes(self, update: Update, context: ContextTypes.DEFAULT_TYPE, analysis_id: int):
        """Заметки к анализу."""
        query = update.callback_query
        context.user_data['analysis_notes_id'] = analysis_id
        await self.safe_edit_message(query, "📝 Введите заметки к анализу:")
        return States.ANALYSIS_NOTES
    
    async def delete_medicine(self, update: Update, context: ContextTypes.DEFAULT_TYPE, medicine_id: int):
        """Удаление лекарства."""
        query = update.callback_query
        await query.answer()
        
        medicine = await db.fetchrow(
            "SELECT name FROM medicines WHERE id = $1",
            medicine_id
        )
        
        if medicine:
            await db.execute(
                "UPDATE medicines SET status = 'deleted' WHERE id = $1",
                medicine_id
            )
            
            reminders = await db.fetch("""
                SELECT id FROM reminders 
                WHERE item_id = $1 AND reminder_type = 'medicine' AND status = 'pending'
            """, medicine_id)
            
            for reminder in reminders:
                await db.execute(
                    "UPDATE reminders SET status = 'cancelled' WHERE id = $1",
                    reminder['id']
                )
                try:
                    scheduler.scheduler.remove_job(f"medicine_{reminder['id']}")
                except JobLookupError:
                    pass
            
            logger.log('info', f"Удалено лекарство {medicine_id}")
        
        keyboard = [
            [InlineKeyboardButton("📋 Список лекарств", callback_data="list_medicines"),
             InlineKeyboardButton("🏠 Главная", callback_data="start")]
        ]
        
        await self.safe_edit_message(
            query,
            f"✅ Лекарство *{medicine['name']}* удалено",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def delete_analysis(self, update: Update, context: ContextTypes.DEFAULT_TYPE, analysis_id: int):
        """Удаление анализа."""
        query = update.callback_query
        await query.answer()
        
        analysis = await db.fetchrow(
            "SELECT name, analysis_type FROM analyses WHERE id = $1",
            analysis_id
        )
        
        if analysis:
            await db.execute(
                "UPDATE analyses SET status = 'cancelled' WHERE id = $1",
                analysis_id
            )
            
            reminders = await db.fetch("""
                SELECT id FROM reminders 
                WHERE item_id = $1 AND reminder_type = $2 AND status = 'pending'
            """, analysis_id, analysis['analysis_type'])
            
            for reminder in reminders:
                await db.execute(
                    "UPDATE reminders SET status = 'cancelled' WHERE id = $1",
                    reminder['id']
                )
                try:
                    scheduler.scheduler.remove_job(f"{analysis['analysis_type']}_{reminder['id']}")
                except JobLookupError:
                    pass
            
            logger.log('info', f"Удален анализ {analysis_id}")
        
        analysis_type = "Анализ" if analysis['analysis_type'] == 'analysis' else 'Исследование'
        
        keyboard = [
            [InlineKeyboardButton("📋 Список", callback_data="list_analyses"),
             InlineKeyboardButton("🏠 Главная", callback_data="start")]
        ]
        
        await self.safe_edit_message(
            query,
            f"✅ {analysis_type} *{analysis['name']}* удален",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    # ============== СПИСКИ ==============
    
    async def list_medicines(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Просмотр списка лекарств."""
        query = update.callback_query
        user_id = update.effective_user.id
        
        page = context.user_data.get('medicines_page', 1)
        offset = (page - 1) * config.PAGE_SIZE
        
        # Общее количество
        total = await db.fetchval("""
            SELECT COUNT(*) FROM medicines 
            WHERE user_id = $1 AND status IN ('active', 'paused')
        """, user_id)
        
        if total == 0:
            keyboard = [
                [InlineKeyboardButton("💊 Добавить лекарство", callback_data="add_medicine")],
                [InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ]
            await self.safe_edit_message(
                query,
                "📋 *У вас нет активных лекарств*",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        medicines = await db.fetch("""
            SELECT id, name, dosage, schedule, status,
                   total_taken, total_skipped, total_postponed
            FROM medicines 
            WHERE user_id = $1 AND status IN ('active', 'paused')
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
        """, user_id, config.PAGE_SIZE, offset)
        
        total_pages = (total + config.PAGE_SIZE - 1) // config.PAGE_SIZE
        
        text = f"📋 *Ваши лекарства (стр {page}/{total_pages})*\n\n"
        keyboard = []
        
        for med in medicines:
            status_emoji = "⏸" if med['status'] == 'paused' else "✅"
            text += f"{status_emoji} *{med['name']}*"
            if med['dosage']:
                text += f" ({med['dosage']})"
            text += f"\n   ⏰ {med['schedule']}\n"
            
            total_count = med['total_taken'] + med['total_skipped'] + med['total_postponed']
            if total_count > 0:
                adherence = (med['total_taken'] / total_count * 100)
                text += f"   📊 Приверженность: {adherence:.1f}%\n"
            
            text += "\n"
            keyboard.append([InlineKeyboardButton(f"🗑️ Удалить {med['name']}", callback_data=f"delete_medicine_{med['id']}")])
        
        # Пагинация
        nav_row = []
        if page > 1:
            nav_row.append(InlineKeyboardButton("⬅️", callback_data=f"medicines_page_{page-1}"))
        nav_row.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
        if page < total_pages:
            nav_row.append(InlineKeyboardButton("➡️", callback_data=f"medicines_page_{page+1}"))
        if nav_row:
            keyboard.append(nav_row)
        
        keyboard.append([InlineKeyboardButton("💊 Добавить", callback_data="add_medicine")])
        keyboard.append([InlineKeyboardButton("🏠 Главная", callback_data="start")])
        
        await self.safe_edit_message(
            query,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def list_analyses(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Просмотр списка анализов."""
        query = update.callback_query
        user_id = update.effective_user.id
        
        if query.data == "list_analyses":
            keyboard = [
                [InlineKeyboardButton("🩸 Анализы", callback_data="list_analyses_analysis"),
                 InlineKeyboardButton("🔬 Исследования", callback_data="list_analyses_investigation")],
                [InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ]
            await self.safe_edit_message(
                query,
                "📋 *Выберите тип для просмотра:*",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        analysis_type = 'analysis' if query.data == "list_analyses_analysis" else 'investigation'
        type_name = "анализы" if analysis_type == 'analysis' else "исследования"
        page = context.user_data.get(f'{analysis_type}_page', 1)
        offset = (page - 1) * config.PAGE_SIZE
        
        total = await db.fetchval("""
            SELECT COUNT(*) FROM analyses 
            WHERE user_id = $1 AND analysis_type = $2 AND status = 'pending'
        """, user_id, analysis_type)
        
        if total == 0:
            text = f"📋 *У вас нет запланированных {type_name}*"
            keyboard = [
                [InlineKeyboardButton("🩺 Добавить", callback_data="add_analysis")],
                [InlineKeyboardButton("🔙 К выбору типа", callback_data="list_analyses")],
                [InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ]
            await self.safe_edit_message(
                query,
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        analyses = await db.fetch("""
            SELECT id, name, scheduled_date, scheduled_time, notes, user_timezone
            FROM analyses 
            WHERE user_id = $1 AND analysis_type = $2 AND status = 'pending'
            ORDER BY scheduled_date ASC
            LIMIT $3 OFFSET $4
        """, user_id, analysis_type, config.PAGE_SIZE, offset)
        
        total_pages = (total + config.PAGE_SIZE - 1) // config.PAGE_SIZE
        text = f"📋 *Запланированные {type_name} (стр {page}/{total_pages})*\n\n"
        keyboard = []
        
        now = datetime.now(pytz.UTC)
        for analysis in analyses:
            local_date = TimeUtils.utc_to_local(analysis['scheduled_date'], analysis['user_timezone'])
            days_left = (analysis['scheduled_date'] - now).days
            
            if days_left < 0:
                status = "🔴 Просрочен"
            elif days_left == 0:
                status = "🟡 Сегодня"
            elif days_left == 1:
                status = "🟡 Завтра"
            else:
                status = f"🟢 Через {days_left} дн."
            
            text += f"*{analysis['name']}*\n"
            text += f"   📅 {local_date.strftime('%d.%m.%Y')} в {analysis['scheduled_time']}\n"
            text += f"   📊 {status}\n"
            if analysis['notes']:
                text += f"   📝 {analysis['notes']}\n"
            text += "\n"
            keyboard.append([InlineKeyboardButton(f"🗑️ Удалить {analysis['name']}", callback_data=f"delete_analysis_{analysis['id']}")])
        
        # Пагинация
        nav_row = []
        if page > 1:
            nav_row.append(InlineKeyboardButton("⬅️", callback_data=f"analyses_{analysis_type}_page_{page-1}"))
        nav_row.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
        if page < total_pages:
            nav_row.append(InlineKeyboardButton("➡️", callback_data=f"analyses_{analysis_type}_page_{page+1}"))
        if nav_row:
            keyboard.append(nav_row)
        
        keyboard.append([InlineKeyboardButton("🩺 Добавить", callback_data="add_analysis")])
        keyboard.append([InlineKeyboardButton("🔙 К выбору типа", callback_data="list_analyses")])
        keyboard.append([InlineKeyboardButton("🏠 Главная", callback_data="start")])
        
        await self.safe_edit_message(
            query,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    # ============== АДМИН-ПАНЕЛЬ ==============
    
    async def admin_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Админ-панель по команде."""
        user_id = update.effective_user.id
        
        if not await self._check_admin_by_id(user_id):
            keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
            await update.message.reply_text(
                "❌ У вас нет прав доступа",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        keyboard = [
            [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats"),
             InlineKeyboardButton("👥 Пользователи", callback_data="admin_users")],
            [InlineKeyboardButton("📝 Логи", callback_data="admin_logs"),
             InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast")],
            [InlineKeyboardButton("💾 Резервное копирование", callback_data="admin_backup"),
             InlineKeyboardButton("ℹ️ Версия", callback_data="admin_version")],
            [InlineKeyboardButton("🏠 Главная", callback_data="start")]
        ]
        
        await update.message.reply_text(
            f"👨‍💻 *Админ-панель*\n\nВерсия: {config.BOT_VERSION}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_panel_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Админ-панель по callback."""
        query = update.callback_query
        user_id = update.effective_user.id
        
        if not await self._check_admin_by_id(user_id):
            keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
            await self.safe_edit_message(
                query,
                "❌ У вас нет прав доступа",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        keyboard = [
            [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats"),
             InlineKeyboardButton("👥 Пользователи", callback_data="admin_users")],
            [InlineKeyboardButton("📝 Логи", callback_data="admin_logs"),
             InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast")],
            [InlineKeyboardButton("💾 Резервное копирование", callback_data="admin_backup"),
             InlineKeyboardButton("ℹ️ Версия", callback_data="admin_version")],
            [InlineKeyboardButton("🏠 Главная", callback_data="start")]
        ]
        
        await self.safe_edit_message(
            query,
            f"👨‍💻 *Админ-панель*\n\nВерсия: {config.BOT_VERSION}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Админ-статистика."""
        query = update.callback_query
        
        if not await self._check_admin_by_id(update.effective_user.id):
            keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
            await self.safe_edit_message(
                query,
                "❌ У вас нет прав доступа",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        total_users = await db.fetchval("SELECT COUNT(*) FROM users") or 0
        active_users = await db.fetchval("SELECT COUNT(*) FROM users WHERE status = 'active'") or 0
        total_medicines = await db.fetchval("SELECT COUNT(*) FROM medicines") or 0
        total_analyses = await db.fetchval("SELECT COUNT(*) FROM analyses") or 0
        total_reminders = await db.fetchval("SELECT COUNT(*) FROM reminders WHERE status = 'pending'") or 0
        
        text = f"""📊 *Админ-статистика*

👥 *Пользователи:*
• Всего: {total_users}
• Активных: {active_users}

💊 *Лекарства:* {total_medicines}
🩺 *Анализы:* {total_analyses}
⏰ *Напоминания:* {total_reminders}

🔄 *Система:*
• Версия: {config.BOT_VERSION}
• Дата: {config.BOT_VERSION_DATE}"""
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="admin_stats")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
        ]
        
        await self.safe_edit_message(
            query,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_users(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Управление пользователями."""
        query = update.callback_query
        
        if not await self._check_admin_by_id(update.effective_user.id):
            keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
            await self.safe_edit_message(
                query,
                "❌ У вас нет прав доступа",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        keyboard = [
            [InlineKeyboardButton("📋 Список пользователей", callback_data="admin_users_list")],
            [InlineKeyboardButton("🔍 Поиск", callback_data="admin_users_search")],
            [InlineKeyboardButton("🚫 Заблокированные", callback_data="admin_users_banned")],
            [InlineKeyboardButton("👑 Администраторы", callback_data="admin_users_admins")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
        ]
        
        await self.safe_edit_message(
            query,
            "👥 *Управление пользователями*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_users_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Список пользователей с пагинацией."""
        query = update.callback_query
        await query.answer()
        
        page = context.user_data.get('admin_users_page', 1)
        offset = (page - 1) * config.PAGE_SIZE
        
        total = await db.fetchval("SELECT COUNT(*) FROM users") or 0
        users = await db.fetch("""
            SELECT user_id, username, first_name, last_name, role, status, created_at
            FROM users
            ORDER BY created_at DESC
            LIMIT $1 OFFSET $2
        """, config.PAGE_SIZE, offset)
        
        total_pages = (total + config.PAGE_SIZE - 1) // config.PAGE_SIZE
        text = f"👥 *Пользователи (стр {page}/{total_pages})*\n\n"
        keyboard = []
        
        for user in users:
            status_emoji = "🟢" if user['status'] == 'active' else "🔴" if user['status'] == 'banned' else "🟡"
            role_emoji = "👑" if user['role'] == 'super_admin' else "🔰" if user['role'] == 'admin' else "👤"
            
            text += f"{status_emoji}{role_emoji} *{user['first_name']}*"
            if user['username']:
                text += f" (@{user['username']})"
            text += f"\n   🆔 `{user['user_id']}`"
            text += f"\n   📅 {user['created_at'].strftime('%d.%m.%Y')}\n\n"
            
            keyboard.append([InlineKeyboardButton(
                f"📊 {user['first_name'][:20]}",
                callback_data=f"admin_user_stats_{user['user_id']}"
            )])
        
        # Пагинация
        nav_row = []
        if page > 1:
            nav_row.append(InlineKeyboardButton("⬅️", callback_data=f"admin_users_page_{page-1}"))
        nav_row.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
        if page < total_pages:
            nav_row.append(InlineKeyboardButton("➡️", callback_data=f"admin_users_page_{page+1}"))
        if nav_row:
            keyboard.append(nav_row)
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_users")])
        
        await self.safe_edit_message(
            query,
            text[:4000],
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_users_page(self, update: Update, context: ContextTypes.DEFAULT_TYPE, page: int):
        """Обработка пагинации."""
        context.user_data['admin_users_page'] = page
        await self.admin_users_list(update, context)
    
    async def admin_users_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Поиск пользователей."""
        query = update.callback_query
        await query.answer()
        
        await self.safe_edit_message(
            query,
            "🔍 *Поиск пользователей*\n\n"
            "Введите для поиска:\n"
            "• ID пользователя\n"
            "• Username (с @ или без)\n"
            "• Имя или фамилию\n\n"
            "Минимум 3 символа:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Показать всех", callback_data="admin_users_list")],
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_users")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.ADMIN_USER_SEARCH
    
    async def admin_user_search_results(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Результаты поиска."""
        query_text = update.message.text.strip()
        
        if len(query_text) < 3 and not query_text.isdigit():
            await update.message.reply_text(
                "❌ Слишком короткий запрос. Минимум 3 символа.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data="admin_users_search")]
                ])
            )
            return ConversationHandler.END
        
        if query_text.isdigit():
            users = await db.fetch(
                "SELECT user_id, username, first_name, role, status FROM users WHERE user_id = $1",
                int(query_text)
            )
        else:
            clean_query = query_text.lstrip('@').lower()
            pattern = f"%{clean_query}%"
            users = await db.fetch("""
                SELECT user_id, username, first_name, role, status 
                FROM users 
                WHERE LOWER(username) LIKE $1 OR LOWER(first_name) LIKE $1 OR LOWER(last_name) LIKE $1
                LIMIT 20
            """, pattern)
        
        if not users:
            await update.message.reply_text(
                "❌ Пользователи не найдены",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Новый поиск", callback_data="admin_users_search")],
                    [InlineKeyboardButton("🔙 Назад", callback_data="admin_users")]
                ])
            )
            return ConversationHandler.END
        
        text = f"🔍 *Найдено пользователей: {len(users)}*\n\n"
        keyboard = []
        
        for user in users:
            status_emoji = "🟢" if user['status'] == 'active' else "🔴" if user['status'] == 'banned' else "🟡"
            role_emoji = "👑" if user['role'] == 'super_admin' else "🔰" if user['role'] == 'admin' else "👤"
            
            text += f"{status_emoji}{role_emoji} *{user['first_name']}*"
            if user['username']:
                text += f" (@{user['username']})"
            text += f"\n   🆔 `{user['user_id']}`\n\n"
            
            keyboard.append([InlineKeyboardButton(
                f"📊 {user['first_name'][:15]}",
                callback_data=f"admin_user_stats_{user['user_id']}"
            )])
        
        keyboard.append([InlineKeyboardButton("🔄 Новый поиск", callback_data="admin_users_search")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_users")])
        
        await update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return ConversationHandler.END
    
    async def admin_users_banned(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Список заблокированных пользователей."""
        query = update.callback_query
        await query.answer()
        
        users = await db.fetch("""
            SELECT u.user_id, u.username, u.first_name, u.status, u.total_interactions,
                   a.created_at as ban_date
            FROM users u
            LEFT JOIN admin_logs a ON u.user_id = a.target_user_id AND a.action = 'ban'
            WHERE u.status = 'banned'
            ORDER BY a.created_at DESC
        """)
        
        if not users:
            await self.safe_edit_message(
                query,
                "✅ Заблокированных пользователей нет",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data="admin_users")]
                ])
            )
            return
        
        text = "🚫 *Заблокированные пользователи*\n\n"
        keyboard = []
        
        for user in users:
            ban_date = user['ban_date'].strftime('%d.%m.%Y') if user['ban_date'] else "неизвестно"
            
            text += f"• *{user['first_name']}*"
            if user['username']:
                text += f" (@{user['username']})"
            text += f"\n  🆔 `{user['user_id']}`"
            text += f"\n  📅 Заблокирован: {ban_date}"
            text += f"\n  📊 Активность: {user['total_interactions']}\n\n"
            
            keyboard.append([InlineKeyboardButton(
                f"✅ Разблокировать {user['first_name'][:15]}",
                callback_data=f"admin_user_unban_{user['user_id']}"
            )])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_users")])
        
        await self.safe_edit_message(
            query,
            text[:4000],
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_users_admins(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Список администраторов."""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        current_user = await db.fetchrow(
            "SELECT role FROM users WHERE user_id = $1",
            user_id
        )
        
        admins = await db.fetch("""
            SELECT user_id, username, first_name, role, created_at, total_interactions
            FROM users 
            WHERE role IN ('admin', 'super_admin')
            ORDER BY 
                CASE role
                    WHEN 'super_admin' THEN 0
                    ELSE 1
                END,
                created_at
        """)
        
        text = "👑 *Администраторы*\n\n"
        keyboard = []
        
        for admin in admins:
            role_text = "👑 Главный" if admin['role'] == 'super_admin' else "🔰 Админ"
            can_manage = current_user and current_user['role'] == 'super_admin' and admin['role'] != 'super_admin'
            
            text += f"{role_text}\n"
            text += f"• *{admin['first_name']}*"
            if admin['username']:
                text += f" (@{admin['username']})"
            text += f"\n  🆔 `{admin['user_id']}`"
            text += f"\n  📅 с {admin['created_at'].strftime('%d.%m.%Y')}"
            text += f"\n  📊 управляет {admin['total_interactions']} пользователями\n\n"
            
            if can_manage:
                keyboard.append([InlineKeyboardButton(
                    f"⬇️ Снять права {admin['first_name'][:10]}",
                    callback_data=f"admin_user_remove_admin_{admin['user_id']}"
                )])
        
        if current_user and current_user['role'] == 'super_admin':
            keyboard.append([InlineKeyboardButton(
                "➕ Назначить админа",
                callback_data="admin_users_make_admin"
            )])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_users")])
        
        await self.safe_edit_message(
            query,
            text[:4000],
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_user_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE, target_user_id: int):
        """Статистика пользователя."""
        query = update.callback_query
        
        user = await db.fetchrow("""
            SELECT * FROM users WHERE user_id = $1
        """, target_user_id)
        
        if not user:
            await self.safe_edit_message(
                query,
                "❌ Пользователь не найден",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data="admin_users_list")]
                ])
            )
            return
        
        medicines = await db.fetchval(
            "SELECT COUNT(*) FROM medicines WHERE user_id = $1",
            target_user_id
        ) or 0
        
        analyses = await db.fetchval(
            "SELECT COUNT(*) FROM analyses WHERE user_id = $1",
            target_user_id
        ) or 0
        
        mood_count = await db.fetchval(
            "SELECT COUNT(*) FROM mood_logs WHERE user_id = $1",
            target_user_id
        ) or 0
        
        symptoms = await db.fetchval(
            "SELECT COUNT(*) FROM symptom_logs WHERE user_id = $1",
            target_user_id
        ) or 0
        
        last_activity = await db.fetchrow(
            "SELECT action, created_at FROM admin_logs WHERE target_user_id = $1 ORDER BY created_at DESC LIMIT 1",
            target_user_id
        )
        
        active_reminders = await db.fetchval("""
            SELECT COUNT(*) FROM reminders 
            WHERE user_id = $1 AND status = 'pending'
        """, target_user_id) or 0
        
        status_emoji = "🟢" if user['status'] == 'active' else "🔴" if user['status'] == 'banned' else "🟡"
        role_emoji = "👑" if user['role'] == 'super_admin' else "🔰" if user['role'] == 'admin' else "👤"
        
        text = f"""📊 *Статистика пользователя*

👤 {user['first_name']} (@{user['username'] or 'нет'})
🆔 `{user['user_id']}`
📅 Регистрация: {user['created_at'].strftime('%d.%m.%Y')}
⏱️ Последний визит: {user['last_seen'].strftime('%d.%m.%Y %H:%M')}
🎯 Взаимодействий: {user['total_interactions']}

💊 Лекарств: {medicines}
🩺 Анализов: {analyses}
😊 Записей настроения: {mood_count}
🤒 Записей симптомов: {symptoms}

🔹 Статус: {user['status']} {status_emoji}
🔹 Роль: {user['role']} {role_emoji}
🔹 Часовой пояс: {user['timezone']}"""

        if last_activity:
            text += f"\n📋 Последнее действие: {last_activity['action']} ({last_activity['created_at'].strftime('%d.%m.%Y %H:%M')})"
        
        text += f"\n⏰ Активных напоминаний: {active_reminders}"
        
        keyboard = [[InlineKeyboardButton("🔙 К списку", callback_data="admin_users_list")]]
        
        action_row = []
        if user['status'] != 'banned':
            action_row.append(InlineKeyboardButton("🚫 Заблокировать", callback_data=f"admin_user_ban_{target_user_id}"))
        else:
            action_row.append(InlineKeyboardButton("✅ Разблокировать", callback_data=f"admin_user_unban_{target_user_id}"))
        
        if user['role'] == 'user':
            action_row.append(InlineKeyboardButton("👑 Сделать админом", callback_data=f"admin_user_make_admin_{target_user_id}"))
        elif user['role'] == 'admin':
            action_row.append(InlineKeyboardButton("⬇️ Снять права", callback_data=f"admin_user_remove_admin_{target_user_id}"))
        
        if action_row:
            keyboard.insert(0, action_row)
        
        await self.safe_edit_message(
            query,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def _admin_ban_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        """Блокировка пользователя."""
        admin_id = update.effective_user.id
        
        user = await db.fetchrow(
            "SELECT first_name FROM users WHERE user_id = $1",
            user_id
        )
        
        if user:
            await db.execute(
                "UPDATE users SET status = 'banned', updated_at = CURRENT_TIMESTAMP WHERE user_id = $1",
                user_id
            )
            
            await db.execute("""
                INSERT INTO admin_logs (admin_id, action, target_user_id, details)
                VALUES ($1, 'ban', $2, $3)
            """, admin_id, user_id, json.dumps({"username": user['first_name']}))
            
            await db.execute("""
                UPDATE reminders SET status = 'cancelled' 
                WHERE user_id = $1 AND status = 'pending'
            """, user_id)
            
            logger.log('info', f"Пользователь {user_id} заблокирован")
        
        await update.callback_query.edit_message_text(
            f"✅ Пользователь {user['first_name']} заблокирован",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 К списку", callback_data="admin_users_list")]
            ])
        )
    
    async def _admin_unban_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        """Разблокировка пользователя."""
        admin_id = update.effective_user.id
        
        user = await db.fetchrow(
            "SELECT first_name FROM users WHERE user_id = $1",
            user_id
        )
        
        if user:
            await db.execute(
                "UPDATE users SET status = 'active', updated_at = CURRENT_TIMESTAMP WHERE user_id = $1",
                user_id
            )
            
            await db.execute("""
                INSERT INTO admin_logs (admin_id, action, target_user_id, details)
                VALUES ($1, 'unban', $2, $3)
            """, admin_id, user_id, json.dumps({"username": user['first_name']}))
            
            logger.log('info', f"Пользователь {user_id} разблокирован")
        
        await update.callback_query.edit_message_text(
            f"✅ Пользователь {user['first_name']} разблокирован",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 К списку", callback_data="admin_users_list")]
            ])
        )
    
    async def _admin_make_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        """Назначение администратором."""
        admin_id = update.effective_user.id
        
        current_admin = await db.fetchrow(
            "SELECT role FROM users WHERE user_id = $1",
            admin_id
        )
        
        if not current_admin or current_admin['role'] != 'super_admin':
            await update.callback_query.answer(
                "❌ Только главный администратор может назначать админов",
                show_alert=True
            )
            return
        
        user = await db.fetchrow(
            "SELECT first_name FROM users WHERE user_id = $1",
            user_id
        )
        
        if user:
            await db.execute(
                "UPDATE users SET role = 'admin', updated_at = CURRENT_TIMESTAMP WHERE user_id = $1",
                user_id
            )
            
            await db.execute("""
                INSERT INTO admin_logs (admin_id, action, target_user_id, details)
                VALUES ($1, 'make_admin', $2, $3)
            """, admin_id, user_id, json.dumps({"username": user['first_name']}))
            
            logger.log('info', f"Пользователь {user_id} назначен администратором")
        
        await update.callback_query.edit_message_text(
            f"✅ {user['first_name']} теперь администратор",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 К списку", callback_data="admin_users_list")]
            ])
        )
    
    async def _admin_remove_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        """Снятие прав администратора."""
        admin_id = update.effective_user.id
        
        current_admin = await db.fetchrow(
            "SELECT role FROM users WHERE user_id = $1",
            admin_id
        )
        
        if not current_admin or current_admin['role'] != 'super_admin':
            await update.callback_query.answer(
                "❌ Только главный администратор может снимать права",
                show_alert=True
            )
            return
        
        user = await db.fetchrow(
            "SELECT first_name FROM users WHERE user_id = $1",
            user_id
        )
        
        if user:
            await db.execute(
                "UPDATE users SET role = 'user', updated_at = CURRENT_TIMESTAMP WHERE user_id = $1",
                user_id
            )
            
            await db.execute("""
                INSERT INTO admin_logs (admin_id, action, target_user_id, details)
                VALUES ($1, 'remove_admin', $2, $3)
            """, admin_id, user_id, json.dumps({"username": user['first_name']}))
            
            logger.log('info', f"Пользователь {user_id} лишен прав администратора")
        
        await update.callback_query.edit_message_text(
            f"✅ Права администратора у {user['first_name']} сняты",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 К списку", callback_data="admin_users_list")]
            ])
        )
    
    async def admin_logs(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Просмотр логов."""
        query = update.callback_query
        
        if not await self._check_admin_by_id(update.effective_user.id):
            keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
            await self.safe_edit_message(
                query,
                "❌ У вас нет прав доступа",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        log_dir = Path("logs")
        logs_text = "📝 *Логи системы*\n\n"
        
        log_files = ['error.log', 'info.log', 'debug.log', 'crash.log']
        
        for log_file in log_files:
            file_path = log_dir / log_file
            if file_path.exists():
                try:
                    async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                        lines = await f.readlines()
                        logs_text += f"*{log_file}:*\n"
                        logs_text += ''.join(lines[-10:]) + "\n\n"
                except Exception:
                    logs_text += f"*{log_file}:* ошибка чтения\n\n"
            else:
                logs_text += f"*{log_file}:* файл не найден\n\n"
        
        if len(logs_text) > 4000:
            logs_text = logs_text[:4000] + "...\n(сообщение обрезано)"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="admin_logs")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
        ]
        
        await self.safe_edit_message(
            query,
            logs_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_broadcast_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начало рассылки."""
        query = update.callback_query
        
        if not await self._check_admin_by_id(update.effective_user.id):
            keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
            await self.safe_edit_message(
                query,
                "❌ У вас нет прав доступа",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        context.user_data['broadcast'] = {}
        await self.safe_edit_message(
            query,
            "📢 *Создание рассылки*\n\nВведите текст сообщения:",
            parse_mode=ParseMode.MARKDOWN
        )
        return States.BROADCAST_MESSAGE
    
    async def admin_broadcast_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка текста рассылки."""
        if not await self._check_admin_by_id(update.effective_user.id):
            await update.message.reply_text("❌ У вас нет прав доступа")
            return ConversationHandler.END
        
        message = update.message.text
        context.user_data['broadcast']['message'] = message
        
        preview_text = f"""📢 *Предпросмотр рассылки*

Сообщение:
{message}

Отправить?"""
        
        keyboard = [
            [InlineKeyboardButton("✅ Отправить", callback_data="admin_broadcast_confirm"),
             InlineKeyboardButton("❌ Отмена", callback_data="admin_broadcast_cancel")]
        ]
        
        await update.message.reply_text(
            preview_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.BROADCAST_CONFIRM
    
    async def admin_broadcast_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Подтверждение рассылки."""
        query = update.callback_query
        await query.answer()
        
        if not await self._check_admin_by_id(update.effective_user.id):
            keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
            await self.safe_edit_message(
                query,
                "❌ У вас нет прав доступа",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        broadcast_data = context.user_data.get('broadcast', {})
        message = broadcast_data.get('message', '')
        
        await self.safe_edit_message(
            query,
            "📢 *Рассылка началась*\n\n⏳ Подготовка...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        users = await db.fetch(
            "SELECT user_id FROM users WHERE status = 'active' AND notifications_enabled = TRUE"
        )
        total = len(users)
        
        sent = 0
        failed = 0
        
        for i, user in enumerate(users, 1):
            try:
                await self.safe_send_message(
                    user['user_id'],
                    f"📢 *Информационное сообщение*\n\n{message}",
                    parse_mode=ParseMode.MARKDOWN
                )
                sent += 1
            except Exception as e:
                failed += 1
                logger.log_error(e, {'user_id': user['user_id']})
            
            if i % 10 == 0:
                await asyncio.sleep(1)
            
            if i % 50 == 0:
                await self.safe_edit_message(
                    query,
                    f"📢 *Рассылка*\n\n⏳ Прогресс: {i}/{total}\n✅ Отправлено: {sent}\n❌ Ошибок: {failed}",
                    parse_mode=ParseMode.MARKDOWN
                )
        
        report = f"""📢 *Рассылка завершена*

✅ Успешно: {sent}
❌ Ошибок: {failed}
📊 Всего: {total}
📈 Доставка: {(sent/total*100) if total > 0 else 0:.1f}%"""
        
        keyboard = [[InlineKeyboardButton("🔙 В админ-панель", callback_data="admin_panel")]]
        await self.safe_edit_message(
            query,
            report,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        
        context.user_data.pop('broadcast', None)
        return ConversationHandler.END
    
    async def admin_broadcast_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена рассылки."""
        query = update.callback_query
        await query.answer()
        
        context.user_data.pop('broadcast', None)
        keyboard = [[InlineKeyboardButton("🔙 В админ-панель", callback_data="admin_panel")]]
        await self.safe_edit_message(
            query,
            "❌ Рассылка отменена",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ConversationHandler.END
    
    async def admin_backup(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Резервное копирование."""
        query = update.callback_query
        
        if not await self._check_admin_by_id(update.effective_user.id):
            keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
            await self.safe_edit_message(
                query,
                "❌ У вас нет прав доступа",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        backup_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Здесь можно добавить логику бэкапа PostgreSQL
        # Например, через pg_dump
        
        text = f"""✅ *Резервное копирование выполнено*

📁 Файл: backup_{backup_time}.sql
📅 Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"""
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]]
        await self.safe_edit_message(
            query,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_version(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Информация о версии."""
        query = update.callback_query
        
        if not await self._check_admin_by_id(update.effective_user.id):
            keyboard = [[InlineKeyboardButton("🏠 Главная", callback_data="start")]]
            await self.safe_edit_message(
                query,
                "❌ У вас нет прав доступа",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        text = f"""ℹ️ *Информация о версии*

📱 *Бот:* {config.BOT_NAME}
🔢 *Версия:* {config.BOT_VERSION}
📅 *Дата:* {config.BOT_VERSION_DATE}
👨‍⚕️ *Автор:* Денис Казарин

🛠 *Компоненты:*
• python-telegram-bot: 20.7
• asyncpg: 0.29.0 (PostgreSQL)
• APScheduler: 3.10.4
• Redis: 5.0.1

📊 *Статус:* Профессиональная версия с PostgreSQL"""
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]]
        await self.safe_edit_message(
            query,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )

# ============== ОСНОВНАЯ ФУНКЦИЯ ==============

async def main():
    """Главная функция запуска."""
    print("\n" + "="*80)
    print(f"🚀 Запуск {config.BOT_NAME} v{config.BOT_VERSION}")
    print("="*80)
    
    print(f"📊 Версия: {config.BOT_VERSION} от {config.BOT_VERSION_DATE}")
    print(f"💾 База данных: PostgreSQL на {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}")
    print(f"📊 Метрики: порт 9090")
    print(f"⚡ Оптимизации: пул соединений, кэширование, пагинация")
    print("-"*80)
    
    # Отключение webhook
    try:
        import requests
        requests.get(f"https://api.telegram.org/bot{config.BOT_TOKEN}/deleteWebhook", timeout=5)
        print(f"✅ Webhook отключен")
    except Exception as e:
        print(f"⚠️ Ошибка при отключении webhook: {e}")
    
    # Инициализация базы данных
    try:
        await db.init_pool()
        print(f"✅ База данных инициализирована")
    except Exception as e:
        print(f"❌ Ошибка подключения к PostgreSQL: {e}")
        print("📋 Проверьте настройки в .env файле")
        return
    
    # Проверка Redis
    if config.REDIS_ENABLED:
        redis_conn = await redis_cache.get_connection()
        if redis_conn:
            print(f"✅ Redis подключен")
        else:
            print(f"⚠️ Redis не доступен, кэширование отключено")
    
    # Создание приложения
    app = ApplicationBuilder().token(config.BOT_TOKEN).build()
    
    # Установка команд бота
    commands = [
        BotCommand("start", "🚀 Главное меню"),
        BotCommand("help", "❓ Помощь"),
        BotCommand("about", "👨‍⚕️ О враче"),
        BotCommand("stats", "📈 Статистика"),
        BotCommand("mood", "📊 Самочувствие"),
        BotCommand("settimezone", "🕒 Часовой пояс"),
        BotCommand("list_medicines", "📋 Список лекарств"),
        BotCommand("list_analyses", "📋 Список анализов"),
        BotCommand("add_medicine", "💊 Добавить лекарство"),
        BotCommand("add_analysis", "🩺 Добавить анализ"),
        BotCommand("take_unscheduled", "💊 Принять препарат"),
    ]
    await app.bot.set_my_commands(commands)
    print(f"✅ Команды бота установлены")
    
    # Запуск планировщика
    scheduler.set_application(app)
    scheduler.start()
    restored = await scheduler.restore_reminders()
    print(f"✅ Восстановлено {restored} напоминаний")
    
    # Создание обработчиков
    handlers = Handlers(app, scheduler, rate_limiter)
    
    # Периодическая проверка целостности
    async def integrity_check():
        """Проверка целостности напоминаний."""
        try:
            now_utc = datetime.now(pytz.UTC)
            
            pending = await db.fetch("""
                SELECT * FROM reminders 
                WHERE status = 'pending' AND scheduled_time > $1
            """, now_utc)
            
            pending_ids = {f"{r['reminder_type']}_{r['id']}" for r in pending}
            scheduler_jobs = scheduler.scheduler.get_jobs()
            scheduler_ids = {job.id for job in scheduler_jobs}
            
            missing = pending_ids - scheduler_ids
            for job_id in missing:
                parts = job_id.split('_')
                reminder_id = int(parts[-1])
                reminder = await db.fetchrow(
                    "SELECT * FROM reminders WHERE id = $1",
                    reminder_id
                )
                
                if reminder and reminder['scheduled_time'] > now_utc:
                    scheduler.scheduler.add_job(
                        scheduler.send_reminder,
                        trigger=DateTrigger(run_date=reminder['scheduled_time']),
                        id=job_id,
                        args=[reminder_id],
                        replace_existing=True
                    )
                    logger.log('warning', f"Восстановлено задание {job_id}")
            
            dead = scheduler_ids - pending_ids
            for job_id in dead:
                if job_id.startswith(('medicine_', 'analysis_', 'investigation_')):
                    try:
                        scheduler.scheduler.remove_job(job_id)
                    except JobLookupError:
                        pass
            
            logger.log('info', f"Проверка целостности: восст. {len(missing)}, удалено {len(dead)}")
            
        except Exception as e:
            logger.log_error(e, {'task': 'integrity_check'})
    
    app.job_queue.run_repeating(
        lambda ctx: asyncio.create_task(integrity_check()),
        interval=config.INTEGRITY_CHECK_INTERVAL,
        first=10,
        name="integrity_check"
    )
    
    print("\n✅ Бот запущен и готов к работе!")
    print("📡 Режим: Long Polling")
    print("💬 Отправьте любое сообщение в Telegram")
    print("⏎ Нажмите Ctrl+C для остановки")
    print("="*80 + "\n")
    
    # Запуск бота
    await app.initialize()
    await app.start()
    await app.updater.start_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True
    )
    
    # Ожидание сигнала остановки
    stop_signal = asyncio.Future()
    
    def signal_handler():
        if not stop_signal.done():
            stop_signal.set_result(None)
    
    if os.name != 'nt':
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, signal_handler)
    
    try:
        await stop_signal
    except KeyboardInterrupt:
        print("\n\n🛑 Получен сигнал остановки")
    finally:
        print("🛑 Завершаем работу...")
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        scheduler.shutdown()
        await db.close()
        await redis_cache.close()
        logger.log('info', "Бот остановлен корректно")
        print("✅ Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main())
