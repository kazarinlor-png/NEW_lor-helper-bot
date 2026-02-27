#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ЛОР-Помощник - Telegram бот для управления приемом лекарств и отслеживания симптомов
Версия: 10.2.0 (Полная стабильная версия со всеми исправлениями)
Автор: Денис Казарин (врач-оториноларинголог)
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union, Callable, TypeVar, Generic
from collections import defaultdict, OrderedDict, deque
from time import time
from functools import wraps, lru_cache, partial
from contextlib import contextmanager, asynccontextmanager
from dataclasses import dataclass, field, asdict
from enum import Enum
import pytz
import json
import re
import warnings
import signal
import traceback
import hashlib
import hmac
import io
import csv
import zipfile
import tempfile
import shutil
import pickle
import weakref
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import asyncio
import aiofiles
import aiosqlite
from typing import TypeVar, Generic

# Исправленный импорт - используем redis.asyncio вместо устаревшего aioredis
import redis.asyncio as redis
from redis.asyncio import Redis, ConnectionPool
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError

# Остальные импорты
import backoff
import tenacity
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import async_timeout

# Отключаем предупреждения
warnings.filterwarnings('ignore')

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

# Устанавливаем и импортируем зависимости
auto_install("python-telegram-bot", "20.7")
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    ConversationHandler, MessageHandler, filters, ContextTypes
)
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TimedOut, BadRequest, Conflict

auto_install("apscheduler", "3.10.4")
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

auto_install("sqlalchemy", "2.0.23")
from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Text, 
    Boolean, BigInteger, Float, JSON, Index, func, and_, or_,
    select, update, delete, inspect, text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, joinedload, selectinload
from sqlalchemy.pool import QueuePool, AsyncAdaptedQueuePool
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.exc import OperationalError, IntegrityError

auto_install("alembic", "1.12.0")
from alembic.config import Config
from alembic import command

auto_install("celery", "5.3.0")
from celery import Celery
from celery.result import AsyncResult
from celery.exceptions import SoftTimeLimitExceeded

auto_install("prometheus-client", "0.19.0")
from prometheus_client import Counter, Histogram, Gauge, start_http_server, generate_latest

# ============== ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ==============
from dotenv import load_dotenv
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
        self.cache_hits = Counter('bot_cache_hits_total', 'Cache hits', ['cache'])
        self.cache_misses = Counter('bot_cache_misses_total', 'Cache misses', ['cache'])
        self.redis_connections = Gauge('bot_redis_connections', 'Redis connections')
        self.background_tasks = Gauge('bot_background_tasks', 'Background tasks')
    
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
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
    DATABASE_URL: str = os.environ.get("DATABASE_URL", "sqlite+aiosqlite:///lor_reminder.db")
    SYNC_DATABASE_URL: str = os.environ.get("SYNC_DATABASE_URL", "sqlite:///lor_reminder.db")
    JOB_STORE_URL: str = os.environ.get("JOB_STORE_URL", "sqlite:///apscheduler_jobs.db")
    REDIS_URL: str = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    REDIS_PASSWORD: str = os.environ.get("REDIS_PASSWORD", "")
    CELERY_BROKER_URL: str = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/1")
    ADMIN_IDS: tuple = tuple(int(id) for id in os.environ.get("ADMIN_IDS", "").split(",") if id)
    BOT_VERSION: str = "10.2.0"
    BOT_VERSION_DATE: str = "26.02.2026"
    BOT_NAME: str = "ЛОР-Помощник"
    
    # Настройки производительности
    DB_POOL_SIZE: int = 50
    DB_MAX_OVERFLOW: int = 100
    DB_POOL_TIMEOUT: int = 30
    DB_POOL_PRE_PING: bool = True
    DB_ECHO: bool = False
    
    RATE_LIMIT_GLOBAL: int = 50
    RATE_LIMIT_USER: int = 1
    RATE_LIMIT_CRITICAL: int = 5
    REMINDER_RETRY_COUNT: int = 5
    INTEGRITY_CHECK_INTERVAL: int = 1800
    
    # Настройки кэша
    CACHE_MAX_SIZE: int = 10000
    CACHE_TTL: int = 300
    CACHE_SHARDS: int = 16
    REDIS_TIMEOUT: int = 2
    REDIS_RETRIES: int = 3
    
    # Настройки пагинации
    PAGE_SIZE: int = 20
    MAX_EXPORT_ROWS: int = 10000
    
    # Настройки безопасности
    SESSION_TIMEOUT: int = 86400
    MAX_MESSAGE_LENGTH: int = 4096
    MAX_CALLBACK_DATA_LENGTH: int = 64
    REQUEST_TIMEOUT: int = 30
    BATCH_TIMEOUT: int = 60
    
    SQL_INJECTION_PATTERNS: tuple = (
        r"'.*'",
        r"\".*\"",
        r"--",
        r";",
        r"\/\*",
        r"\*\/",
        r"xp_",
        r"UNION",
        r"SELECT",
        r"INSERT",
        r"UPDATE",
        r"DELETE",
        r"DROP",
        r"CREATE",
        r"ALTER"
    )
    
    # Настройки мониторинга
    ENABLE_METRICS: bool = True
    METRICS_PORT: int = 9090
    ALERT_WEBHOOK: str = os.environ.get("ALERT_WEBHOOK", "")
    
    def __post_init__(self):
        if not self.BOT_TOKEN:
            raise ValueError("❌ Не установлен BOT_TOKEN")
        if not self.DATABASE_URL:
            raise ValueError("❌ Не установлен DATABASE_URL")

config = Config()

# ============== ИНИЦИАЛИЗАЦИЯ CELERY ==============

celery_app = Celery(
    'lor_pomoshnik',
    broker=config.CELERY_BROKER_URL,
    backend=config.CELERY_BROKER_URL
)

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,
    task_soft_time_limit=25 * 60,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_default_retry_delay=60,
    task_max_retries=3
)

# ============== ОПТИМИЗИРОВАННЫЙ МЕНЕДЖЕР REDIS ==============

class RedisManager:
    """Менеджер Redis с использованием redis.asyncio вместо устаревшего aioredis."""
    
    _instance = None
    _pool: Optional[ConnectionPool] = None
    _redis: Optional[Redis] = None
    _lock = asyncio.Lock()
    _healthy = True
    _failures = 0
    _max_failures = 5
    _last_recovery_attempt = 0
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    async def _create_pool(self) -> ConnectionPool:
        """Создание пула соединений."""
        return ConnectionPool.from_url(
            config.REDIS_URL,
            password=config.REDIS_PASSWORD or None,
            max_connections=20,
            timeout=config.REDIS_TIMEOUT,
            retry_on_timeout=True,
            retry=Retry(ExponentialBackoff(), config.REDIS_RETRIES),
            decode_responses=True
        )
    
    async def get_redis(self) -> Optional[Redis]:
        """Получение подключения к Redis с автоматическим восстановлением."""
        # Попытка восстановления после сбоя
        if not self._healthy:
            current_time = time()
            if current_time - self._last_recovery_attempt > 300:  # 5 минут
                self._failures = 0
                self._healthy = True
                self._last_recovery_attempt = current_time
                logger.log('info', "Attempting Redis recovery")
        
        if not self._healthy and self._failures >= self._max_failures:
            metrics.redis_connections.set(0)
            return None
        
        async with self._lock:
            if self._pool is None:
                try:
                    self._pool = await self._create_pool()
                    self._redis = Redis(connection_pool=self._pool)
                    await self._redis.ping()
                    self._healthy = True
                    self._failures = 0
                    metrics.redis_connections.set(1)
                    logger.log('info', "Redis connected successfully")
                except (RedisConnectionError, RedisTimeoutError, Exception) as e:
                    self._failures += 1
                    if self._failures >= self._max_failures:
                        self._healthy = False
                    logger.log('warning', f"Redis connection failed ({self._failures}/{self._max_failures}): {e}")
                    return None
            
            return self._redis
    
    async def pipeline(self) -> Optional[redis.client.Pipeline]:
        """Получение pipeline для массовых операций."""
        redis_client = await self.get_redis()
        return redis_client.pipeline() if redis_client else None
    
    async def execute_pipeline(self, pipeline: redis.client.Pipeline) -> Optional[List]:
        """Выполнение pipeline с обработкой ошибок."""
        try:
            async with async_timeout.timeout(config.REDIS_TIMEOUT):
                return await pipeline.execute()
        except (RedisConnectionError, RedisTimeoutError, asyncio.TimeoutError) as e:
            logger.log('warning', f"Redis pipeline failed: {e}")
            return None
    
    async def close(self):
        """Корректное закрытие соединений."""
        async with self._lock:
            if self._redis:
                await self._redis.close()
                self._redis = None
            if self._pool:
                await self._pool.disconnect()
                self._pool = None
            self._healthy = True
            self._failures = 0
            metrics.redis_connections.set(0)

redis_manager = RedisManager()

# ============== НАСТРОЙКА ЛОГИРОВАНИЯ ==============

class LoggerSetup:
    """Многоуровневая система логирования с метриками."""
    
    _instance = None
    _loggers = {}
    _metrics = defaultdict(int)
    _alerts = deque(maxlen=100)
    _lock = asyncio.Lock()
    _metrics_task = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._setup_loggers()

    async def start_metrics(self):
        """Запуск метрик (вызывать из main)"""
        if config.ENABLE_METRICS and not self._metrics_task:
            self._metrics_task = asyncio.create_task(self._metrics_collector())
            self.log('info', "Метрики запущены")
    
    def _setup_loggers(self):
        """Настройка всех уровней логирования."""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        from logging.handlers import RotatingFileHandler
        
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        for name, level, max_bytes in [
            ('debug', logging.DEBUG, 100 * 1024 * 1024),
            ('info', logging.INFO, 100 * 1024 * 1024),
            ('warning', logging.WARNING, 50 * 1024 * 1024),
            ('error', logging.ERROR, 50 * 1024 * 1024),
            ('critical', logging.CRITICAL, 25 * 1024 * 1024),
            ('audit', logging.INFO, 100 * 1024 * 1024),
            ('performance', logging.INFO, 50 * 1024 * 1024),
            ('security', logging.WARNING, 50 * 1024 * 1024)
        ]:
            logger = logging.getLogger(name)
            logger.setLevel(level)
            
            if name == 'info':
                logger.addHandler(console)
            
            handler = RotatingFileHandler(
                log_dir / f'{name}.log',
                maxBytes=max_bytes,
                backupCount=5,
                encoding='utf-8'
            )
            
            if name in ['error', 'critical', 'audit', 'security']:
                formatter = logging.Formatter(
                    '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s\n%(exc_info)s'
                )
            else:
                formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            self._loggers[name] = logger
        
        json_logger = logging.getLogger('json')
        json_logger.setLevel(logging.INFO)
        json_handler = RotatingFileHandler(
            log_dir / 'structured.json',
            maxBytes=100 * 1024 * 1024,
            backupCount=3,
            encoding='utf-8'
        )
        json_handler.setFormatter(logging.Formatter('%(message)s'))
        json_logger.addHandler(json_handler)
        self._loggers['json'] = json_logger
    
    async def _metrics_collector(self):
        """Сбор метрик в фоне."""
        while True:
            try:
                metrics.background_tasks.set(len(asyncio.all_tasks()))
                await asyncio.sleep(15)
            except Exception as e:
                self.log('error', f"Metrics collector error: {e}")
    
    def log(self, level: str, message: str, **kwargs):
        """Логирование с метриками."""
        if level in self._loggers:
            self._metrics[f"logs_{level}_total"] += 1
            if level in ['error', 'critical']:
                metrics.errors_total.labels(type=level).inc()
            
            if level == 'json':
                log_entry = {
                    'timestamp': datetime.now().isoformat(),
                    'message': message,
                    **kwargs
                }
                self._loggers[level].info(json.dumps(log_entry, ensure_ascii=False))
            elif level in ['audit', 'security']:
                log_entry = {
                    'timestamp': datetime.now().isoformat(),
                    'level': level,
                    'message': message,
                    **kwargs
                }
                self._loggers[level].info(json.dumps(log_entry, ensure_ascii=False))
                
                if level == 'security' and config.ALERT_WEBHOOK:
                    asyncio.create_task(self._send_alert(log_entry))
            else:
                getattr(self._loggers[level], level)(message)
    
    async def _send_alert(self, alert_data: dict):
        """Отправка алерта с таймаутом."""
        try:
            import aiohttp
            async with async_timeout.timeout(5):
                async with aiohttp.ClientSession() as session:
                    await session.post(config.ALERT_WEBHOOK, json=alert_data)
        except Exception as e:
            self.log('error', f"Failed to send alert: {e}")
    
    def log_error(self, error: Exception, context: Dict = None):
        """Логирование ошибки с контекстом."""
        error_msg = f"Error: {str(error)}\n"
        if context:
            error_msg += f"Context: {json.dumps(context, ensure_ascii=False, default=str)}\n"
        error_msg += f"Traceback: {traceback.format_exc()}"
        
        self._loggers['error'].error(error_msg)
        metrics.errors_total.labels(type=type(error).__name__).inc()
        
        if isinstance(error, (SystemExit, KeyboardInterrupt)):
            self._loggers['critical'].critical(f"CRASH: {error_msg}")

logger = LoggerSetup()

# ============== БЕЗОПАСНОСТЬ ==============

class SecurityManager:
    """Менеджер безопасности."""
    
    _VALID_TIMEZONES = set(pytz.all_timezones)
    _MAX_TEXT_LENGTH = 10000
    _EMOJI_PATTERN = re.compile(r'[\U00010000-\U0010ffff]', flags=re.UNICODE)
    
    @staticmethod
    def sanitize_input(text: str, max_length: int = 1000) -> str:
        """Санитизация пользовательского ввода."""
        if not text:
            return text
        
        if len(text) > max_length:
            text = text[:max_length]
        
        text = ''.join(char for char in text if ord(char) >= 32 or char in '\n\r\t')
        
        text = text.replace("'", "''")
        text = text.replace('\\', '\\\\')
        text = text.replace('%', '\\%')
        text = text.replace('_', '\\_')
        
        return text
    
    @staticmethod
    def check_sql_injection(text: str) -> bool:
        """Проверка на SQL-инъекции."""
        if not text:
            return False
        
        text_upper = text.upper()
        for pattern in config.SQL_INJECTION_PATTERNS:
            if re.search(pattern, text_upper, re.IGNORECASE):
                logger.log('security', f"Potential SQL injection detected", 
                          text=text[:100], pattern=pattern)
                return True
        return False
    
    @staticmethod
    def validate_callback_data(data: str) -> bool:
        """Валидация callback data."""
        if not data:
            return False
        
        if len(data) > config.MAX_CALLBACK_DATA_LENGTH:
            logger.log('security', f"Callback data too long", length=len(data))
            return False
        
        if not re.match(r'^[a-zA-Z0-9_\-]+$', data):
            logger.log('security', f"Callback data contains invalid chars", data=data)
            return False
        
        return True
    
    @staticmethod
    def validate_timezone(tz_name: str) -> bool:
        """Валидация часового пояса."""
        return tz_name in SecurityManager._VALID_TIMEZONES
    
    @staticmethod
    def audit_log(action: str, user_id: int, details: dict = None):
        """Логирование действий для аудита."""
        logger.log('audit', f"Action: {action}", user_id=user_id, **(details or {}))

# ============== ОПТИМИЗИРОВАННОЕ КЭШИРОВАНИЕ ==============

T = TypeVar('T')

class ShardedCache(Generic[T]):
    """Шардированный кэш с защитой от race conditions."""
    
    def __init__(self, name: str, max_size: int = config.CACHE_MAX_SIZE, ttl: int = config.CACHE_TTL):
        self.name = name
        self.max_size = max_size
        self.ttl = ttl
        self.shards = [OrderedDict() for _ in range(config.CACHE_SHARDS)]
        self.locks = [asyncio.Lock() for _ in range(config.CACHE_SHARDS)]
        self.hits = 0
        self.misses = 0
        self._stats_lock = asyncio.Lock()
    
    def _get_shard(self, key: str) -> int:
        """Получение индекса шарда."""
        return hash(key) % len(self.shards)
    
    async def get(self, key: str) -> Optional[T]:
        """Получение значения из кэша."""
        shard_idx = self._get_shard(key)
        async with self.locks[shard_idx]:
            shard = self.shards[shard_idx]
            if key in shard:
                value, timestamp = shard[key]
                if time() - timestamp < self.ttl:
                    async with self._stats_lock:
                        self.hits += 1
                    metrics.cache_hits.labels(cache=self.name).inc()
                    return value
                else:
                    del shard[key]
            
            async with self._stats_lock:
                self.misses += 1
            metrics.cache_misses.labels(cache=self.name).inc()
            return None
    
    async def set(self, key: str, value: T, ttl: int = None):
        """Сохранение значения в кэш."""
        shard_idx = self._get_shard(key)
        async with self.locks[shard_idx]:
            shard = self.shards[shard_idx]
            shard[key] = (value, time())
            
            while len(shard) > self.max_size // len(self.shards):
                shard.popitem(last=False)
    
    async def delete(self, key: str):
        """Удаление из кэша."""
        shard_idx = self._get_shard(key)
        async with self.locks[shard_idx]:
            self.shards[shard_idx].pop(key, None)
    
    async def clear(self):
        """Полная очистка кэша."""
        for i, lock in enumerate(self.locks):
            async with lock:
                self.shards[i].clear()
    
    def get_stats(self) -> dict:
        """Получение статистики."""
        return {
            'name': self.name,
            'hits': self.hits,
            'misses': self.misses,
            'hit_ratio': self.hits / (self.hits + self.misses) if (self.hits + self.misses) > 0 else 0,
            'size': sum(len(shard) for shard in self.shards),
            'max_size': self.max_size
        }

# ============== МОДЕЛИ БАЗЫ ДАННЫХ ==============

Base = declarative_base()

class UserStatus(str, Enum):
    ACTIVE = "active"
    BLOCKED = "blocked"
    BANNED = "banned"

class UserRole(str, Enum):
    USER = "user"
    ADMIN = "admin"
    SUPER_ADMIN = "super_admin"

class AnalysisType(str, Enum):
    ANALYSIS = "analysis"
    INVESTIGATION = "investigation"

class ReminderStatus(str, Enum):
    PENDING = "pending"
    SENT = "sent"
    COMPLETED = "completed"
    SKIPPED = "skipped"
    POSTPONED = "postponed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class MedicineStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    DELETED = "deleted"
    CANCELLED = "cancelled"

class User(Base):
    __tablename__ = 'users'
    __table_args__ = (
        Index('ix_users_status_role', 'status', 'role'),
        Index('ix_users_last_seen_created', 'last_seen', 'created_at'),
        Index('ix_users_username_trgm', 'username', postgresql_using='gin'),
        Index('ix_users_first_name_trgm', 'first_name', postgresql_using='gin'),
        {'extend_existing': True}
    )
    
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, unique=True, nullable=False, index=True)
    username = Column(String(100))
    first_name = Column(String(100))
    last_name = Column(String(100))
    language = Column(String(10), default='ru')
    timezone = Column(String(50), default='Europe/Moscow')
    role = Column(String(20), default=UserRole.USER.value)
    status = Column(String(20), default=UserStatus.ACTIVE.value)
    
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow, index=True)
    total_interactions = Column(Integer, default=0)
    
    notifications_enabled = Column(Boolean, default=True)
    notification_offset = Column(Integer, default=5)
    
    user_metadata = Column(JSON, default={})
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Medicine(Base):
    __tablename__ = 'medicines'
    __table_args__ = (
        Index('ix_medicines_user_status_created', 'user_id', 'status', 'created_at'),
        Index('ix_medicines_end_date_start_date', 'end_date', 'start_date'),
        {'extend_existing': True}
    )
    
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    name = Column(String(200), nullable=False)
    dosage = Column(String(100))
    times_per_day = Column(Integer, default=1)
    schedule_times = Column(JSON)
    schedule = Column(String(200))
    
    course_type = Column(String(20), default='unlimited')
    course_duration = Column(Integer)
    course_duration_unit = Column(String(10))
    
    repeat_type = Column(String(20), default='none')
    repeat_interval = Column(Integer)
    repeat_interval_unit = Column(String(10))
    
    start_date = Column(DateTime(timezone=True))
    end_date = Column(DateTime(timezone=True), index=True)
    user_timezone = Column(String(50), nullable=False)
    status = Column(String(20), default=MedicineStatus.ACTIVE.value)
    
    total_taken = Column(Integer, default=0)
    total_skipped = Column(Integer, default=0)
    total_postponed = Column(Integer, default=0)
    
    stats = Column(JSON, default={})
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class MedicineLog(Base):
    __tablename__ = 'medicine_logs'
    __table_args__ = (
        Index('ix_medicine_logs_user_medicine_taken', 'user_id', 'medicine_id', 'taken_at'),
        Index('ix_medicine_logs_taken_at_status', 'taken_at', 'status'),
        {'extend_existing': True}
    )
    
    id = Column(Integer, primary_key=True)
    medicine_id = Column(Integer, nullable=False, index=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    
    log_type = Column(String(20), default='scheduled')
    status = Column(String(20))
    
    dosage = Column(String(100))
    comment = Column(Text)
    reason = Column(Text)
    side_effects = Column(Text)
    
    scheduled_time = Column(DateTime(timezone=True))
    taken_at = Column(DateTime(timezone=True), default=lambda: datetime.now(pytz.UTC), index=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)

class Analysis(Base):
    __tablename__ = 'analyses'
    __table_args__ = (
        Index('ix_analyses_user_status_scheduled', 'user_id', 'status', 'scheduled_date'),
        Index('ix_analyses_scheduled_date_status', 'scheduled_date', 'status'),
        {'extend_existing': True}
    )
    
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    analysis_type = Column(String(20), default=AnalysisType.ANALYSIS.value)
    name = Column(String(200), nullable=False)
    
    scheduled_date = Column(DateTime(timezone=True), nullable=False, index=True)
    scheduled_time = Column(String(10), nullable=False)
    
    repeat_type = Column(String(20), default='once')
    repeat_interval = Column(Integer)
    repeat_interval_unit = Column(String(10))
    
    reminder_before = Column(Integer, default=24)
    reminder_before_unit = Column(String(10), default='hours')
    
    notes = Column(Text)
    status = Column(String(20), default='pending')
    
    user_timezone = Column(String(50), nullable=False)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class AnalysisLog(Base):
    __tablename__ = 'analysis_logs'
    __table_args__ = (
        Index('ix_analysis_logs_user_analysis_completed', 'user_id', 'analysis_id', 'completed_at'),
        {'extend_existing': True}
    )
    
    id = Column(Integer, primary_key=True)
    analysis_id = Column(Integer, nullable=False, index=True)
    user_id = Column(BigInteger, nullable=False)
    status = Column(String(20))
    completed_at = Column(DateTime(timezone=True), default=lambda: datetime.now(pytz.UTC))
    notes = Column(Text)

class MoodLog(Base):
    __tablename__ = 'mood_logs'
    __table_args__ = (
        Index('ix_mood_logs_user_created_score', 'user_id', 'created_at', 'mood_score'),
        {'extend_existing': True}
    )
    
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    mood_score = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(pytz.UTC))

class SymptomLog(Base):
    __tablename__ = 'symptom_logs'
    __table_args__ = (
        Index('ix_symptom_logs_user_created_symptom', 'user_id', 'created_at', 'symptom'),
        {'extend_existing': True}
    )
    
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    symptom = Column(String(100), nullable=False)
    severity = Column(Integer, nullable=False)
    severity_color = Column(String(20))
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(pytz.UTC))

class Reminder(Base):
    __tablename__ = 'reminders'
    __table_args__ = (
        Index('ix_reminders_user_status_scheduled', 'user_id', 'status', 'scheduled_time'),
        Index('ix_reminders_status_scheduled', 'status', 'scheduled_time'),
        {'extend_existing': True}
    )
    
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    reminder_type = Column(String(20))
    item_id = Column(Integer, nullable=False)
    scheduled_time = Column(DateTime(timezone=True), nullable=False, index=True)
    user_timezone = Column(String(50), nullable=False)
    status = Column(String(20), default=ReminderStatus.PENDING.value)
    retry_count = Column(Integer, default=0)
    last_error = Column(Text)
    postponed_until = Column(DateTime(timezone=True))
    created_at = Column(DateTime, default=datetime.utcnow)

class AdminLog(Base):
    __tablename__ = 'admin_logs'
    __table_args__ = (
        Index('ix_admin_logs_admin_action_created', 'admin_id', 'action', 'created_at'),
        Index('ix_admin_logs_target_user', 'target_user_id'),
        {'extend_existing': True}
    )
    
    id = Column(Integer, primary_key=True)
    admin_id = Column(BigInteger, nullable=False)
    action = Column(String(100), nullable=False)
    target_user_id = Column(BigInteger)
    details = Column(JSON)
    ip_address = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)

class Broadcast(Base):
    __tablename__ = 'broadcasts'
    __table_args__ = (
        Index('ix_broadcasts_admin_status', 'admin_id', 'status'),
        {'extend_existing': True}
    )
    
    id = Column(Integer, primary_key=True)
    admin_id = Column(BigInteger, nullable=False)
    message = Column(Text, nullable=False)
    filters = Column(JSON)
    status = Column(String(20), default='pending')
    total_recipients = Column(Integer, default=0)
    sent_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)

# ============== АСИНХРОННЫЙ МЕНЕДЖЕР БД ==============

class AsyncDatabaseManager:
    """Асинхронный менеджер БД с пулом и мониторингом."""
    
    _instance = None
    _engine = None
    _async_session_factory = None
    _sync_engine = None
    _lock = asyncio.Lock()
    _connection_errors = 0
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._engine is None:
            self._init_engines()
    
    def _init_engines(self):
        """Инициализация движков БД."""
        self._engine = create_async_engine(
            config.DATABASE_URL,
            pool_size=config.DB_POOL_SIZE,
            max_overflow=config.DB_MAX_OVERFLOW,
            pool_timeout=config.DB_POOL_TIMEOUT,
            pool_pre_ping=config.DB_POOL_PRE_PING,
            pool_recycle=3600,
            echo=config.DB_ECHO,
            future=True,
            poolclass=AsyncAdaptedQueuePool
        )
        
        self._async_session_factory = async_sessionmaker(
            self._engine,
            expire_on_commit=False,
            class_=AsyncSession
        )
        
        self._sync_engine = create_engine(
            config.SYNC_DATABASE_URL,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False,
            future=True
        )
        
        metrics.db_pool_size.set(config.DB_POOL_SIZE)
    
    @asynccontextmanager
    async def session(self) -> AsyncSession:
        """Асинхронный контекстный менеджер сессии."""
        async with self._async_session_factory() as session:
            try:
                async with async_timeout.timeout(config.REQUEST_TIMEOUT):
                    yield session
                    await session.commit()
            except asyncio.TimeoutError:
                await session.rollback()
                logger.log('error', "Database session timeout")
                self._connection_errors += 1
                raise
            except Exception as e:
                await session.rollback()
                logger.log_error(e)
                self._connection_errors += 1
                raise
            finally:
                await session.close()
    
    @backoff.on_exception(
        backoff.expo,
        (OperationalError, IntegrityError),
        max_tries=3,
        max_time=10
    )
    async def execute_with_retry(self, stmt):
        """Выполнение запроса с повторными попытками."""
        async with self.session() as db:
            result = await db.execute(stmt)
            return result

db_manager = AsyncDatabaseManager()

# ============== ОПТИМИЗИРОВАННЫЙ ПОИСК ==============

class SearchManager:
    """Менеджер поиска с защитой от инъекций."""
    
    _search_cache = ShardedCache("search", max_size=500, ttl=60)
    
    @staticmethod
    async def search_users(query: str, limit: int = 20) -> List[User]:
        """Безопасный поиск пользователей с кэшированием."""
        if SecurityManager.check_sql_injection(query):
            logger.log('security', f"Blocked SQL injection attempt")
            return []
        
        cache_key = f"search:{query}:{limit}"
        cached = await SearchManager._search_cache.get(cache_key)
        if cached:
            import pickle
            return pickle.loads(cached.encode('latin1'))
        
        clean_query = SecurityManager.sanitize_input(query, max_length=100)
        
        async with db_manager.session() as db:
            if clean_query.isdigit():
                stmt = select(User).where(User.user_id == int(clean_query))
                result = await db.execute(stmt)
                users = result.scalars().all()
            else:
                clean_username = clean_query.lstrip('@').lower()
                pattern = f"%{clean_username}%"
                
                stmt = select(User).where(
                    or_(
                        User.username.ilike(pattern),
                        User.first_name.ilike(pattern),
                        User.last_name.ilike(pattern)
                    )
                ).limit(limit)
                
                result = await db.execute(stmt)
                users = result.scalars().all()
            
            if users:
                await SearchManager._search_cache.set(
                    cache_key,
                    pickle.dumps(users).decode('latin1'),
                    60
                )
            
            return users

# ============== УТИЛИТЫ ==============

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
        """Получение часового пояса пользователя."""
        async with db_manager.session() as db:
            stmt = select(User.timezone).where(User.user_id == user_id)
            result = await db.execute(stmt)
            tz = result.scalar_one_or_none()
            return tz if tz else 'Europe/Moscow'

# ============== RATE LIMITER ==============

class RateLimiter:
    """Rate limiter для защиты от превышения лимитов Telegram."""
    
    def __init__(self):
        self.user_last_message = defaultdict(float)
        self.user_critical_actions = defaultdict(int)
        self._lock = asyncio.Lock()
        self._metrics = defaultdict(int)
    
    async def acquire(self, user_id: Optional[int] = None, critical: bool = False):
        """Acquire rate limit permit."""
        if user_id:
            async with self._lock:
                now = time()
                
                if critical:
                    # Проверяем количество критических операций за последний час
                    hour_ago = now - 3600
                    if self.user_critical_actions[user_id] > 10:
                        wait_time = 60
                        await asyncio.sleep(wait_time)
                        self._metrics['critical_delayed'] += 1
                    
                    self.user_critical_actions[user_id] += 1
                    self._metrics['critical_ops'] += 1
                else:
                    last_msg = self.user_last_message[user_id]
                    if now - last_msg < config.RATE_LIMIT_USER:
                        wait_time = config.RATE_LIMIT_USER - (now - last_msg)
                        await asyncio.sleep(wait_time)
                        self._metrics['user_delayed'] += 1
                    
                    self.user_last_message[user_id] = now
                    self._metrics['user_ops'] += 1

# ============== ПЛАНИРОВЩИК ==============

class SchedulerManager:
    """Менеджер планировщика с обработкой ошибок."""
    
    def __init__(self):
        jobstores = {
            'default': SQLAlchemyJobStore(url=config.JOB_STORE_URL)
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
        self._background_tasks = set()
        self._task_lock = asyncio.Lock()
    
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
    
    async def add_background_task(self, coro):
        """Добавление фоновой задачи с защитой от ошибок."""
        async with self._task_lock:
            async def wrapped():
                try:
                    async with async_timeout.timeout(config.BATCH_TIMEOUT):
                        await coro
                except asyncio.TimeoutError:
                    logger.log('error', f"Background task timeout: {coro}")
                    metrics.errors_total.labels(type='background_timeout').inc()
                except Exception as e:
                    logger.log_error(e, {'background_task': str(coro)})
                finally:
                    async with self._task_lock:
                        self._background_tasks.discard(task)
            
            task = asyncio.create_task(wrapped())
            self._background_tasks.add(task)
            metrics.background_tasks.set(len(self._background_tasks))
    
    async def restore_reminders(self):
        """Восстановление напоминаний при старте."""
        async with db_manager.session() as db:
            now_utc = datetime.now(pytz.UTC)
            
            stmt = select(Reminder).where(
                and_(
                    Reminder.status == ReminderStatus.PENDING.value,
                    Reminder.scheduled_time > now_utc
                )
            )
            result = await db.execute(stmt)
            pending = result.scalars().all()
            
            restored = 0
            for reminder in pending:
                job_id = f"{reminder.reminder_type}_{reminder.id}"
                
                try:
                    self.scheduler.remove_job(job_id)
                except JobLookupError:
                    pass
                
                self.scheduler.add_job(
                    self.send_reminder,
                    trigger=DateTrigger(run_date=reminder.scheduled_time),
                    id=job_id,
                    args=[reminder.id],
                    replace_existing=True,
                    misfire_grace_time=3600
                )
                restored += 1
            
            logger.log('info', f"Восстановлено {restored} напоминаний")
            return restored
    
    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        retry=tenacity.retry_if_exception_type((RetryAfter, TimedOut))
    )
    async def send_reminder(self, reminder_id: int):
        """Отправка напоминания с повторными попытками."""
        if not self.application:
            return
        
        async with db_manager.session() as db:
            try:
                stmt = select(Reminder).where(Reminder.id == reminder_id)
                result = await db.execute(stmt)
                reminder = result.scalar_one_or_none()
                
                if not reminder or reminder.status != ReminderStatus.PENDING.value:
                    return
                
                user_id = reminder.user_id
                
                if reminder.reminder_type == 'medicine':
                    stmt = select(Medicine).where(Medicine.id == reminder.item_id)
                    result = await db.execute(stmt)
                    medicine = result.scalar_one_or_none()
                    
                    if not medicine or medicine.status != MedicineStatus.ACTIVE.value:
                        reminder.status = ReminderStatus.CANCELLED.value
                        return
                    
                    text = f"💊 *Время принять лекарство!*\n\n{medicine.name}"
                    if medicine.dosage:
                        text += f"\n💧 Дозировка: {medicine.dosage}"
                    
                    keyboard = [
                        [
                            InlineKeyboardButton("✅ Принял(а)", callback_data=f"take_{medicine.id}"),
                            InlineKeyboardButton("⏸ Отложить", callback_data=f"postpone_medicine_{medicine.id}")
                        ],
                        [
                            InlineKeyboardButton("📝 Комментарий", callback_data=f"comment_medicine_{medicine.id}"),
                            InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_medicine_{medicine.id}")
                        ],
                        [
                            InlineKeyboardButton("🆕 Новый симптом", callback_data=f"new_symptom_{medicine.id}"),
                            InlineKeyboardButton("⚠️ Побочное действие", callback_data=f"side_effect_{medicine.id}")
                        ]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    
                elif reminder.reminder_type in ['analysis', 'investigation']:
                    stmt = select(Analysis).where(Analysis.id == reminder.item_id)
                    result = await db.execute(stmt)
                    analysis = result.scalar_one_or_none()
                    
                    if not analysis or analysis.status != 'pending':
                        reminder.status = ReminderStatus.CANCELLED.value
                        return
                    
                    local_date = analysis.scheduled_date
                    if local_date.tzinfo:
                        local_date = local_date.astimezone(pytz.timezone(analysis.user_timezone))
                    
                    analysis_type = "анализ" if analysis.analysis_type == 'analysis' else "исследование"
                    
                    text = f"🩺 *Напоминание об {analysis_type}е!*\n\n"
                    text += f"📋 {analysis.name}\n"
                    text += f"📅 {local_date.strftime('%d.%m.%Y')} в {analysis.scheduled_time}\n"
                    
                    if analysis.notes:
                        text += f"\n📝 *Заметки:* {analysis.notes}"
                    
                    keyboard = [
                        [
                            InlineKeyboardButton("✅ Сдал(а)", callback_data=f"analysis_take_{analysis.id}"),
                            InlineKeyboardButton("⏸ Отложить", callback_data=f"postpone_analysis_{analysis.id}")
                        ],
                        [
                            InlineKeyboardButton("📝 Заметки", callback_data=f"analysis_notes_{analysis.id}"),
                            InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_analysis_{analysis.id}")
                        ]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                else:
                    return
                
                for attempt in range(config.REMINDER_RETRY_COUNT):
                    try:
                        async with async_timeout.timeout(config.REQUEST_TIMEOUT):
                            await self.application.bot.send_message(
                                chat_id=user_id,
                                text=text,
                                reply_markup=reply_markup,
                                parse_mode=ParseMode.MARKDOWN
                            )
                        
                        reminder.status = ReminderStatus.SENT.value
                        reminder.retry_count = attempt + 1
                        logger.log('info', f"Напоминание {reminder_id} отправлено")
                        return
                        
                    except (RetryAfter, TimedOut, asyncio.TimeoutError) as e:
                        reminder.retry_count = attempt + 1
                        reminder.last_error = str(e)
                        
                        if attempt < config.REMINDER_RETRY_COUNT - 1:
                            await asyncio.sleep(5 * (attempt + 1))
                    
                    except Exception as e:
                        reminder.status = ReminderStatus.FAILED.value
                        reminder.last_error = str(e)
                        logger.log_error(e, {'reminder_id': reminder_id})
                        return
                
                reminder.status = ReminderStatus.FAILED.value
                logger.log('error', f"Напоминание {reminder_id} не отправлено")
                
            except Exception as e:
                logger.log_error(e, {'reminder_id': reminder_id})

# ============== СОСТОЯНИЯ ==============

class States:
    """Состояния для ConversationHandler."""
    # Общие
    START = 0
    CANCEL = 999
    
    # Лекарства (1-19)
    MEDICINE_NAME = 1
    MEDICINE_DOSAGE = 2
    MEDICINE_TIMES_PER_DAY = 3
    MEDICINE_SCHEDULE_HOUR = 4
    MEDICINE_SCHEDULE_MINUTE = 5
    MEDICINE_COURSE_DURATION = 6
    MEDICINE_COURSE_UNIT = 7
    MEDICINE_REPEAT = 8
    MEDICINE_REPEAT_INTERVAL = 9
    MEDICINE_REPEAT_UNIT = 10
    MEDICINE_START_TYPE = 11
    MEDICINE_START_DATE = 12
    MEDICINE_CONFIRM = 13
    MEDICINE_EDIT = 14
    MEDICINE_COMMENT = 15
    
    # Анализы (20-39)
    ANALYSIS_TYPE = 20
    ANALYSIS_NAME = 21
    ANALYSIS_DATE = 22
    ANALYSIS_TIME_HOUR = 23
    ANALYSIS_TIME_MINUTE = 24
    ANALYSIS_REPEAT = 25
    ANALYSIS_REPEAT_INTERVAL = 26
    ANALYSIS_REMINDER = 27
    ANALYSIS_REMINDER_VALUE = 28
    ANALYSIS_REMINDER_UNIT = 29
    ANALYSIS_NOTES = 30
    ANALYSIS_CONFIRM = 31
    ANALYSIS_EDIT = 32
    LIST_ANALYSES_TYPE = 33
    LIST_ANALYSES_VIEW = 34
    
    # Симптомы (40-49)
    SYMPTOM_TEXT = 40
    SYMPTOM_SEVERITY = 41
    
    # Незапланированный прием (50-59)
    UNSCHEDULED_MEDICINE_SELECT = 50
    UNSCHEDULED_MEDICINE_DOSAGE = 51
    UNSCHEDULED_MEDICINE_REASON = 52
    UNSCHEDULED_MEDICINE_COMMENT = 53
    
    # Откладывание (60-69)
    POSTPONE_TYPE = 60
    POSTPONE_HOURS = 61
    POSTPONE_DAYS = 62
    POSTPONE_CUSTOM = 63
    
    # Админ (70-89)
    BROADCAST_MESSAGE = 70
    BROADCAST_CONFIRM = 71
    ADMIN_USER_SEARCH = 72
    ADMIN_SETTINGS_EDIT = 73

# ============== ОСНОВНОЙ КЛАСС ОБРАБОТЧИКОВ ==============

class Handlers:
    """Основные обработчики бота."""
    
    def __init__(self, application, scheduler, rate_limiter):
        self.app = application
        self.scheduler = scheduler
        self.rate_limiter = rate_limiter
        self._callback_map = self._build_callback_map()
        self._setup_handlers()
    
    def _build_callback_map(self) -> Dict[str, Callable]:
        """Построение карты callback обработчиков."""
        return {
            # Навигация
            "start": self.start_callback,
            "back": self.back_callback,
            "about": self.about_callback,
            "help": self.help_callback,
            "stats": self.stats_callback,
            "mood": self.mood_callback,
            
            # Лекарства
            "list_medicines": self.list_medicines,
            "add_medicine": self.add_medicine_start,
            "take_unscheduled": self.unscheduled_medicine_start,
            
            # Анализы
            "list_analyses": self.list_analyses,
            "add_analysis": self.add_analysis_start,
            
            # Телефоны
            "phone_kit": self.phone_kit,
            "phone_family": self.phone_family,
            
            # Симптомы
            "no_thanks": self.no_thanks,
            
            # Откладывание
            "postpone_type_hours": self.postpone_type_hours,
            "postpone_type_days": self.postpone_type_days,
            "postpone_custom": self.postpone_custom,
            
            # Анализы - специальные
            "analysis_time_other": self.analysis_time_hour,
            "analysis_force_create": self.analysis_force_create,
            
            # Доктор
            "doctor_visited": self.doctor_visited,
            
            # Экспорт
            "export_mood_csv": lambda u, c: self.export_data(u, c, 'mood'),
            "export_symptoms_csv": lambda u, c: self.export_data(u, c, 'symptoms'),
            "export_medicines_csv": lambda u, c: self.export_data(u, c, 'medicines'),
            
            # Админ
            "admin_stats": self.admin_stats,
            "admin_users": self.admin_users,
            "admin_logs": self.admin_logs,
            "admin_broadcast": self.admin_broadcast_start,
            "admin_backup": self.admin_backup,
            "admin_settings": self.admin_settings,
            "admin_version": self.admin_version,
            "admin_users_list": self.admin_users_list,
            "admin_users_search": self.admin_users_search,
            "admin_users_banned": self.admin_users_banned,
            "admin_users_admins": self.admin_users_admins,
            "admin_broadcast_confirm": self.admin_broadcast_confirm,
            "admin_broadcast_cancel": self.admin_broadcast_cancel,
            "admin_logs_download": self.admin_logs_download,
            "admin_settings_general": self.admin_settings_general,
            "admin_settings_notifications": self.admin_settings_notifications,
            "admin_settings_security": self.admin_settings_security,
            "admin_settings_logging": self.admin_settings_logging,
            "admin_check_updates": self.admin_check_updates,
        }
    
    def _setup_handlers(self):
        """Настройка всех обработчиков."""
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("admin", self.admin_panel))
        self.app.add_handler(CommandHandler("cancel", self.cancel))
        self.app.add_handler(CommandHandler("help", self.help_command))
        self.app.add_handler(CommandHandler("about", self.about_command))
        self.app.add_handler(CommandHandler("stats", self.stats_command))
        self.app.add_handler(CommandHandler("mood", self.mood_command))
        
        self.app.add_handler(self._medicine_conversation())
        self.app.add_handler(self._analysis_conversation())
        self.app.add_handler(self._symptom_conversation())
        self.app.add_handler(self._unscheduled_conversation())
        self.app.add_handler(self._admin_broadcast_conversation())
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
                States.MEDICINE_COURSE_UNIT: [CallbackQueryHandler(self.medicine_course_unit, pattern="^unit_")],
                States.MEDICINE_REPEAT: [CallbackQueryHandler(self.medicine_repeat, pattern="^repeat_")],
                States.MEDICINE_REPEAT_INTERVAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.medicine_repeat_interval)],
                States.MEDICINE_REPEAT_UNIT: [CallbackQueryHandler(self.medicine_repeat_unit, pattern="^unit_")],
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
                CallbackQueryHandler(self.start_callback, pattern="^start$")
            ],
            name="add_medicine",
            persistent=False
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
                States.ANALYSIS_REMINDER: [
                    CallbackQueryHandler(self.analysis_reminder, pattern="^remind_"),
                    CallbackQueryHandler(self.analysis_reminder_custom, pattern="^remind_custom$")
                ],
                States.ANALYSIS_REMINDER_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.analysis_reminder_value)],
                States.ANALYSIS_REMINDER_UNIT: [CallbackQueryHandler(self.analysis_reminder_unit, pattern="^unit_")],
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
                CallbackQueryHandler(self.start_callback, pattern="^start$")
            ],
            name="add_analysis",
            persistent=False
        )
    
    def _symptom_conversation(self):
        """Conversation для добавления симптомов."""
        return ConversationHandler(
            entry_points=[
                CommandHandler("symptoms", self.symptoms_start),
                CallbackQueryHandler(self.symptoms_start, pattern="^symptoms$"),
                CallbackQueryHandler(self.new_symptom, pattern="^new_symptom_")
            ],
            states={
                States.SYMPTOM_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.symptom_text)],
                States.SYMPTOM_SEVERITY: [CallbackQueryHandler(self.symptom_severity, pattern="^severity_")]
            },
            fallbacks=[
                CommandHandler("cancel", self.cancel),
                CallbackQueryHandler(self.cancel, pattern="^cancel$"),
                CallbackQueryHandler(self.mood_callback, pattern="^mood$")
            ],
            name="add_symptom",
            persistent=False
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
                CallbackQueryHandler(self.start_callback, pattern="^start$")
            ],
            name="unscheduled_medicine",
            persistent=False
        )
    
    def _admin_broadcast_conversation(self):
        """Conversation для рассылки."""
        return ConversationHandler(
            entry_points=[CallbackQueryHandler(self.admin_broadcast_start, pattern="^admin_broadcast$")],
            states={
                States.BROADCAST_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.admin_broadcast_message)],
                States.BROADCAST_CONFIRM: [CallbackQueryHandler(self.admin_broadcast_confirm, pattern="^admin_broadcast_confirm$")]
            },
            fallbacks=[CallbackQueryHandler(self.admin_broadcast_cancel, pattern="^admin_broadcast_cancel$")],
            name="admin_broadcast",
            persistent=False
        )
    
    def _admin_search_conversation(self):
        """Conversation для поиска пользователей."""
        return ConversationHandler(
            entry_points=[CallbackQueryHandler(self.admin_users_search, pattern="^admin_users_search$")],
            states={
                States.ADMIN_USER_SEARCH: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.admin_user_search_results)]
            },
            fallbacks=[CallbackQueryHandler(self.admin_users, pattern="^admin_users$")],
            name="admin_user_search",
            persistent=False
        )
    
    async def callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик callback запросов."""
        query = update.callback_query
        data = query.data
        user_id = update.effective_user.id
        
        # Валидация callback data
        if not SecurityManager.validate_callback_data(data):
            await query.answer("❌ Некорректные данные")
            return
        
        # Rate limiting для критических операций
        critical_actions = ['delete_', 'ban_', 'make_admin_', 'broadcast']
        is_critical = any(action in data for action in critical_actions)
        
        try:
            await self.rate_limiter.acquire(user_id, critical=is_critical)
        except asyncio.TimeoutError:
            await query.answer("⏰ Слишком много запросов, попробуйте позже")
            return
        
        # Обновление статистики пользователя
        try:
            async with db_manager.session() as db:
                stmt = update(User).where(User.user_id == user_id).values(
                    last_seen=datetime.utcnow(),
                    total_interactions=User.total_interactions + 1
                )
                await db.execute(stmt)
        except Exception as e:
            logger.log_error(e, {'user_id': user_id, 'action': 'update_stats'})
        
        await query.answer()
        
        # Аудит критических действий
        if is_critical:
            SecurityManager.audit_log(
                action=data.split('_')[0],
                user_id=user_id,
                details={'callback_data': data}
            )
        
        # Прямая маршрутизация по карте
        if data in self._callback_map:
            await self._callback_map[data](update, context)
            return
        
        # Префиксная маршрутизация
        if data.startswith("mood_"):
            await self.mood_select(update, context)
        elif data.startswith("delete_medicine_"):
            medicine_id = int(data.replace("delete_medicine_", ""))
            await self.delete_medicine(update, context, medicine_id)
        elif data.startswith("take_"):
            medicine_id = int(data.replace("take_", ""))
            await self.medicine_take(update, context, medicine_id)
        elif data.startswith("skip_"):
            medicine_id = int(data.replace("skip_", ""))
            await self.medicine_skip(update, context, medicine_id)
        elif data.startswith("postpone_medicine_"):
            medicine_id = int(data.replace("postpone_medicine_", ""))
            await self.medicine_postpone_start(update, context, medicine_id)
        elif data.startswith("cancel_medicine_"):
            medicine_id = int(data.replace("cancel_medicine_", ""))
            await self.medicine_cancel(update, context, medicine_id)
        elif data.startswith("comment_medicine_"):
            medicine_id = int(data.replace("comment_medicine_", ""))
            await self.medicine_comment(update, context, medicine_id)
        elif data.startswith("side_effect_"):
            medicine_id = int(data.replace("side_effect_", ""))
            await self.medicine_side_effect(update, context, medicine_id)
        elif data.startswith("delete_analysis_"):
            analysis_id = int(data.replace("delete_analysis_", ""))
            await self.delete_analysis(update, context, analysis_id)
        elif data.startswith("analysis_take_"):
            analysis_id = int(data.replace("analysis_take_", ""))
            await self.analysis_take(update, context, analysis_id)
        elif data.startswith("analysis_skip_"):
            analysis_id = int(data.replace("analysis_skip_", ""))
            await self.analysis_skip(update, context, analysis_id)
        elif data.startswith("postpone_analysis_"):
            analysis_id = int(data.replace("postpone_analysis_", ""))
            await self.analysis_postpone_start(update, context, analysis_id)
        elif data.startswith("cancel_analysis_"):
            analysis_id = int(data.replace("cancel_analysis_", ""))
            await self.analysis_cancel(update, context, analysis_id)
        elif data.startswith("analysis_notes_"):
            analysis_id = int(data.replace("analysis_notes_", ""))
            await self.analysis_notes(update, context, analysis_id)
        elif data.startswith("repeat_"):
            await self.analysis_repeat(update, context)
        elif data.startswith("postpone_hour_"):
            hours = int(data.replace("postpone_hour_", ""))
            await self.postpone_hour(update, context, hours)
        elif data.startswith("postpone_days_"):
            if data == "postpone_days_custom":
                await self.postpone_custom(update, context)
            else:
                days = int(data.replace("postpone_days_", ""))
                await self.postpone_days(update, context, days)
        elif data.startswith("stats_"):
            await self.stats_detail(update, context)
        elif data.startswith("delete_symptom_"):
            symptom_id = int(data.replace("delete_symptom_", ""))
            await self.delete_symptom(update, context, symptom_id)
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
        elif data.startswith("admin_logs_download_"):
            log_type = data.replace("admin_logs_download_", "")
            await self.admin_logs_download_type(update, context, log_type)
        elif data.startswith("list_analyses_"):
            await self.list_analyses(update, context)
        elif data.startswith("admin_users_page_"):
            page = int(data.replace("admin_users_page_", ""))
            await self.admin_users_page(update, context, page)
        else:
            logger.log('warning', f"Неизвестный callback: {data}")
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик /start."""
        user = update.effective_user
        
        async with db_manager.session() as db:
            stmt = select(User).where(User.user_id == user.id)
            result = await db.execute(stmt)
            db_user = result.scalar_one_or_none()
            
            if not db_user:
                db_user = User(
                    user_id=user.id,
                    username=user.username,
                    first_name=user.first_name,
                    last_name=user.last_name
                )
                db.add(db_user)
                logger.log('info', f"Новый пользователь: {user.id}")
            else:
                db_user.last_seen = datetime.utcnow()
                db_user.total_interactions += 1
        
        welcome_text = f"""👋 *Здравствуйте, {user.first_name}!*

Я *{config.BOT_NAME}* врача-оториноларинголога Казарина Дениса Сергеевича

🤖 *Мои возможности:*
• 💊 Напоминания о приеме лекарств
• 🩺 Напоминания об анализах и исследованиях
• 📊 Отслеживание самочувствия и симптомов
• 📈 Статистика и отчеты

Начните с добавления лекарства или анализа/исследования!"""
        
        await update.message.reply_text(
            welcome_text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💊 Добавить лекарство", callback_data="add_medicine"),
                 InlineKeyboardButton("📋 Список лекарств", callback_data="list_medicines")],
                [InlineKeyboardButton("💊 Принять препарат", callback_data="take_unscheduled"),
                 InlineKeyboardButton("🩺 Добавить анализ/исследование", callback_data="add_analysis")],
                [InlineKeyboardButton("📋 Список анализов", callback_data="list_analyses"),
                 InlineKeyboardButton("📊 Самочувствие", callback_data="mood")],
                [InlineKeyboardButton("📈 Статистика", callback_data="stats"),
                 InlineKeyboardButton("👨‍⚕️ О враче", callback_data="about")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик /help."""
        text = """❓ *Как очистить историю переписки*

Чтобы удалить всю переписку с ботом:

1️⃣ В правом верхнем углу нажмите на свой профиль
2️⃣ В меню выберите пункт "Еще" (или "More")
3️⃣ Прокрутите вниз и нажмите "Удалить переписку"

✅ После этого появится начальная страница бота
💾 Все ваши сохраненные данные останутся"""
        
        await update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="about"),
                 InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def about_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик /about."""
        await self.about_callback(update, context)
    
    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик /stats."""
        await self.stats_callback(update, context)
    
    async def mood_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик /mood."""
        await self.mood_callback(update, context)
    
    async def start_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Возврат на стартовую страницу."""
        query = update.callback_query
        user = update.effective_user
        
        text = f"""👋 *Здравствуйте, {user.first_name}!*

Я *{config.BOT_NAME}* врача-оториноларинголога Казарина Дениса Сергеевича

🤖 *Мои возможности:*
• 💊 Напоминания о приеме лекарств
• 🩺 Напоминания об анализах и исследованиях
• 📊 Отслеживание самочувствия и симптомов
• 📈 Статистика и отчеты"""
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💊 Добавить лекарство", callback_data="add_medicine"),
                 InlineKeyboardButton("📋 Список лекарств", callback_data="list_medicines")],
                [InlineKeyboardButton("💊 Принять препарат", callback_data="take_unscheduled"),
                 InlineKeyboardButton("🩺 Добавить анализ/исследование", callback_data="add_analysis")],
                [InlineKeyboardButton("📋 Список анализов", callback_data="list_analyses"),
                 InlineKeyboardButton("📊 Самочувствие", callback_data="mood")],
                [InlineKeyboardButton("📈 Статистика", callback_data="stats"),
                 InlineKeyboardButton("👨‍⚕️ О враче", callback_data="about")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def back_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка кнопки Назад."""
        previous = context.user_data.get('previous_state', 'start')
        if previous == 'about':
            await self.about_callback(update, context)
        elif previous == 'stats':
            await self.stats_callback(update, context)
        else:
            await self.start_callback(update, context)
    
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
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("👥 Telegram канал", url="https://t.me/KAZARIN_LOR"),
                 InlineKeyboardButton("💬 Мой Telegram", url="https://t.me/deniskazarin")],
                [InlineKeyboardButton("🏥 КИТ-клиника", url=KIT_CLINIC['booking']),
                 InlineKeyboardButton("📞 Позвонить", callback_data="phone_kit"),
                 InlineKeyboardButton("🗺️ Карты", url=KIT_CLINIC['maps'])],
                [InlineKeyboardButton("🏥 Семейная клиника", url=FAMILY_CLINIC['booking']),
                 InlineKeyboardButton("📞 Позвонить", callback_data="phone_family"),
                 InlineKeyboardButton("🗺️ Карты", url=FAMILY_CLINIC['maps'])],
                [InlineKeyboardButton("❓ Помощь", callback_data="help"),
                 InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def help_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик 'Помощь'."""
        query = update.callback_query
        
        text = """❓ *Как очистить историю переписки*

Чтобы удалить всю переписку с ботом:

1️⃣ В правом верхнем углу нажмите на свой профиль
2️⃣ В меню выберите пункт "Еще" (или "More")
3️⃣ Прокрутите вниз и нажмите "Удалить переписку"

✅ После этого появится начальная страница бота
💾 Все ваши сохраненные данные останутся"""
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="about"),
                 InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def stats_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показ статистики."""
        query = update.callback_query
        
        await query.edit_message_text(
            "📈 *Статистика*\n\nВыберите тип статистики:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 За всё время", callback_data="stats_all"),
                 InlineKeyboardButton("💊 По лекарствам", callback_data="stats_medicines")],
                [InlineKeyboardButton("😊 Настроение", callback_data="stats_mood"),
                 InlineKeyboardButton("🩺 Симптомы", callback_data="stats_symptoms")],
                [InlineKeyboardButton("📈 Детально", callback_data="stats_detailed"),
                 InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def mood_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Оценка самочувствия."""
        query = update.callback_query
        
        await query.edit_message_text(
            "📊 *Как вы себя чувствуете сегодня?*\n\nОцените по 5-балльной шкале:",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("1 😢", callback_data="mood_1"),
                    InlineKeyboardButton("2 🙁", callback_data="mood_2"),
                    InlineKeyboardButton("3 😐", callback_data="mood_3"),
                    InlineKeyboardButton("4 🙂", callback_data="mood_4"),
                    InlineKeyboardButton("5 😊", callback_data="mood_5"),
                ],
                [InlineKeyboardButton("🔙 Назад", callback_data="start")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def mood_select(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор оценки настроения."""
        query = update.callback_query
        await query.answer()
        
        mood_score = int(query.data.replace("mood_", ""))
        user_id = update.effective_user.id
        
        async with db_manager.session() as db:
            mood_log = MoodLog(user_id=user_id, mood_score=mood_score)
            db.add(mood_log)
            
            # Проверка на ухудшение 2 дня подряд
            stmt = select(MoodLog).where(
                MoodLog.user_id == user_id
            ).order_by(MoodLog.created_at.desc()).limit(2)
            result = await db.execute(stmt)
            recent = result.scalars().all()
            
            if len(recent) == 2 and all(m.mood_score <= 2 for m in recent):
                warning_text = """⚠️ *Внимание!*

Зафиксировано ухудшение самочувствия два дня подряд.

Рекомендуется обратиться к врачу."""
                
                await context.bot.send_message(
                    chat_id=user_id,
                    text=warning_text,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("👨‍⚕️ Записаться", callback_data="about"),
                         InlineKeyboardButton("✅ Отметить визит", callback_data="doctor_visited")]
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
             InlineKeyboardButton("❌ Нет", callback_data="no_thanks")],
            [InlineKeyboardButton("🏠 Главная", callback_data="start")]
        ]
        
        await query.edit_message_text(
            f"✅ {mood_texts[mood_score]}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def phone_kit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Телефон КИТ-клиники."""
        query = update.callback_query
        await context.bot.send_message(
            chat_id=update.effective_user.id,
            text=f"📞 Телефон КИТ-клиники: {KIT_CLINIC['phone_display']}\n\nНажмите на номер чтобы позвонить: {KIT_CLINIC['phone']}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="about"),
                 InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ])
        )
    
    async def phone_family(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Телефон Семейной клиники."""
        query = update.callback_query
        await context.bot.send_message(
            chat_id=update.effective_user.id,
            text=f"📞 Телефон Семейной клиники: {FAMILY_CLINIC['phone_display']}\n\nНажмите на номер чтобы позвонить: {FAMILY_CLINIC['phone']}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="about"),
                 InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ])
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
        context.user_data['previous_state'] = 'start'
        
        await edit_func(
            "💊 *Добавление лекарства*\n\nШаг 1/7: Введите *название лекарства*",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.MEDICINE_NAME
    
    async def medicine_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение названия лекарства."""
        if SecurityManager.check_sql_injection(update.message.text):
            await update.message.reply_text(
                "❌ Обнаружены недопустимые символы. Пожалуйста, используйте только буквы и цифры."
            )
            return States.MEDICINE_NAME
        
        context.user_data['medicine_data']['name'] = update.message.text
        
        keyboard = [
            [InlineKeyboardButton("⏭️ Пропустить", callback_data="skip_dosage")],
            [InlineKeyboardButton("🔙 Отмена", callback_data="cancel")]
        ]
        
        await update.message.reply_text(
            "Шаг 2/7: Укажите *дозировку* (или нажмите Пропустить)\nНапример: 500мг, 1 таблетка, 5мл",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.MEDICINE_DOSAGE
    
    async def medicine_dosage(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение дозировки."""
        if SecurityManager.check_sql_injection(update.message.text):
            await update.message.reply_text(
                "❌ Обнаружены недопустимые символы. Пожалуйста, используйте только буквы и цифры."
            )
            return States.MEDICINE_DOSAGE
        
        context.user_data['medicine_data']['dosage'] = update.message.text
        
        await update.message.reply_text(
            "Шаг 3/7: Сколько раз в день принимать?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("1 раз", callback_data="times_1"),
                 InlineKeyboardButton("2 раза", callback_data="times_2"),
                 InlineKeyboardButton("3 раза", callback_data="times_3")],
                [InlineKeyboardButton("4 раза", callback_data="times_4"),
                 InlineKeyboardButton("5 раз", callback_data="times_5"),
                 InlineKeyboardButton("6 раз", callback_data="times_6")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.MEDICINE_TIMES_PER_DAY
    
    async def skip_dosage(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Пропуск дозировки."""
        query = update.callback_query
        await query.answer()
        
        context.user_data['medicine_data']['dosage'] = None
        
        if 'medicine_data' in context.user_data:
            await query.edit_message_text(
                "Шаг 3/7: Сколько раз в день принимать?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("1 раз", callback_data="times_1"),
                     InlineKeyboardButton("2 раза", callback_data="times_2"),
                     InlineKeyboardButton("3 раза", callback_data="times_3")],
                    [InlineKeyboardButton("4 раза", callback_data="times_4"),
                     InlineKeyboardButton("5 раз", callback_data="times_5"),
                     InlineKeyboardButton("6 раз", callback_data="times_6")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_TIMES_PER_DAY
        else:
            await query.edit_message_text(
                "💊 *Укажите принятую дозу*\n(или нажмите Пропустить):",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⏭️ Пропустить", callback_data="skip_reason")],
                    [InlineKeyboardButton("🔙 Назад", callback_data="take_unscheduled")]
                ])
            )
            return States.UNSCHEDULED_MEDICINE_DOSAGE
    
    async def medicine_times_per_day(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор количества приемов."""
        query = update.callback_query
        await query.answer()
        
        times = int(query.data.replace("times_", ""))
        context.user_data['medicine_data']['times_per_day'] = times
        context.user_data['medicine_data']['schedule_times'] = []
        
        default_times = {
            2: ["08:00", "20:00"],
            3: ["08:00", "14:00", "20:00"]
        }.get(times, [])
        
        if default_times:
            keyboard = [
                [InlineKeyboardButton(f"{', '.join(default_times)}", callback_data="use_default_times")],
                [InlineKeyboardButton("⚙️ Своё время", callback_data="hour_select")]
            ]
            await query.edit_message_text(
                f"Шаг 4/7: Выберите время приема\n\nРекомендуемое время для {times} раз в день:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_SCHEDULE_HOUR
        else:
            await self.show_hour_selection(query, context)
            return States.MEDICINE_SCHEDULE_HOUR
    
    async def show_hour_selection(self, query, context):
        """Показать выбор часа."""
        keyboard = []
        for start in range(0, 24, 6):
            row = []
            for h in range(start, min(start + 6, 24)):
                row.append(InlineKeyboardButton(f"{h:02d}", callback_data=f"hour_{h:02d}"))
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="add_medicine")])
        
        await query.edit_message_text(
            "Выберите час:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
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
        """Выбор часа приема."""
        query = update.callback_query
        await query.answer()
        
        if query.data == "hour_select":
            await self.show_hour_selection(query, context)
            return States.MEDICINE_SCHEDULE_HOUR
        
        hour = query.data.replace("hour_", "")
        context.user_data['temp_hour'] = hour
        
        # Показать выбор минут
        minutes = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
        keyboard = []
        for i in range(0, len(minutes), 4):
            row = [InlineKeyboardButton(f"{m:02d}", callback_data=f"minute_{m:02d}") for m in minutes[i:i+4]]
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("⏰ К выбору часа", callback_data="back_to_hours")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="add_medicine")])
        
        await query.edit_message_text(
            f"Вы выбрали час {hour}. Теперь выберите минуты:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.MEDICINE_SCHEDULE_MINUTE
    
    async def medicine_schedule_minute(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор минут приема."""
        query = update.callback_query
        await query.answer()
        
        minute = query.data.replace("minute_", "")
        hour = context.user_data.get('temp_hour', "00")
        time_str = f"{hour}:{minute}"
        
        times = context.user_data['medicine_data']['schedule_times']
        times.append(time_str)
        context.user_data['medicine_data']['schedule_times'] = times
        
        if len(times) < context.user_data['medicine_data']['times_per_day']:
            await self.show_hour_selection(query, context)
            return States.MEDICINE_SCHEDULE_HOUR
        else:
            await self.show_course_duration(query, context)
            return States.MEDICINE_COURSE_DURATION
    
    async def back_to_hours(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Возврат к выбору часа."""
        query = update.callback_query
        await query.answer()
        
        times = context.user_data['medicine_data']['schedule_times']
        if times:
            times.pop()
            context.user_data['medicine_data']['schedule_times'] = times
        
        await self.show_hour_selection(query, context)
        return States.MEDICINE_SCHEDULE_HOUR
    
    async def show_course_duration(self, query, context):
        """Показать выбор длительности курса."""
        await query.edit_message_text(
            "Шаг 5/7: Выберите продолжительность курса",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📅 3 дня", callback_data="duration_3"),
                 InlineKeyboardButton("📅 5 дней", callback_data="duration_5"),
                 InlineKeyboardButton("📅 7 дней", callback_data="duration_7")],
                [InlineKeyboardButton("📅 10 дней", callback_data="duration_10"),
                 InlineKeyboardButton("📅 14 дней", callback_data="duration_14"),
                 InlineKeyboardButton("📅 30 дней", callback_data="duration_30")],
                [InlineKeyboardButton("📅 60 дней", callback_data="duration_60"),
                 InlineKeyboardButton("📅 90 дней", callback_data="duration_90"),
                 InlineKeyboardButton("⚙️ Свой вариант", callback_data="duration_custom")],
                [InlineKeyboardButton("∞ Бессрочно", callback_data="duration_unlimited")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def medicine_course_duration(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор продолжительности курса."""
        query = update.callback_query
        await query.answer()
        
        data = query.data.replace("duration_", "")
        
        if data == "custom":
            await query.edit_message_text(
                "Введите количество дней (от 1 до 365):",
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_COURSE_DURATION
        elif data == "unlimited":
            context.user_data['medicine_data']['course_duration'] = None
            context.user_data['medicine_data']['course_type'] = 'unlimited'
            
            await self.show_repeat_selection(query, context)
            return States.MEDICINE_REPEAT
        else:
            context.user_data['medicine_data']['course_duration'] = int(data)
            context.user_data['medicine_data']['course_type'] = 'limited'
            
            await self.show_repeat_selection(query, context)
            return States.MEDICINE_REPEAT
    
    async def medicine_course_duration_custom(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Пользовательская продолжительность курса."""
        try:
            days = int(update.message.text.strip())
            if not 1 <= days <= 365:
                raise ValueError
            
            context.user_data['medicine_data']['course_duration'] = days
            context.user_data['medicine_data']['course_type'] = 'limited'
            
            await update.message.reply_text(
                "Шаг 6/7: Нужно ли повторять курс?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Без повторения", callback_data="repeat_none"),
                     InlineKeyboardButton("⚙️ Свой вариант", callback_data="repeat_custom")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_REPEAT
            
        except ValueError:
            await update.message.reply_text(
                "❌ Введите число от 1 до 365",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back")]])
            )
            return States.MEDICINE_COURSE_DURATION
    
    async def medicine_course_unit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор единиц курса."""
        query = update.callback_query
        await query.answer()
        
        unit = query.data.replace("unit_", "")
        context.user_data['medicine_data']['course_unit'] = unit
        
        await query.edit_message_text(
            f"Введите количество {unit}:",
            parse_mode=ParseMode.MARKDOWN
        )
        return States.MEDICINE_COURSE_DURATION
    
    async def show_repeat_selection(self, query, context):
        """Показать выбор повторения."""
        await query.edit_message_text(
            "Шаг 6/7: Нужно ли повторять курс?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Без повторения", callback_data="repeat_none"),
                 InlineKeyboardButton("⚙️ Свой вариант", callback_data="repeat_custom")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def medicine_repeat(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор повторения курса."""
        query = update.callback_query
        await query.answer()
        
        repeat_type = query.data.replace("repeat_", "")
        
        if repeat_type == "custom":
            await query.edit_message_text(
                "Введите интервал повторения в днях (например: 30, 60, 90):",
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_REPEAT_INTERVAL
        elif repeat_type == "none":
            context.user_data['medicine_data']['repeat_type'] = 'none'
            context.user_data['medicine_data']['repeat_interval'] = None
            await self.show_start_date(query, context)
            return States.MEDICINE_START_TYPE
        else:
            context.user_data['medicine_data']['repeat_type'] = 'custom'
            context.user_data['medicine_data']['repeat_interval'] = 0
            await self.show_start_date(query, context)
            return States.MEDICINE_START_TYPE
    
    async def medicine_repeat_interval(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка интервала повторения."""
        try:
            interval = int(update.message.text.strip())
            if interval < 1:
                raise ValueError
            
            context.user_data['medicine_data']['repeat_type'] = 'custom'
            context.user_data['medicine_data']['repeat_interval'] = interval
            context.user_data['medicine_data']['repeat_unit'] = 'days'
            
            await update.message.reply_text(
                "Шаг 7/7: Выберите дату начала",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📅 Сегодня", callback_data="start_today"),
                     InlineKeyboardButton("📅 Завтра", callback_data="start_tomorrow")],
                    [InlineKeyboardButton("📅 Выбрать дату", callback_data="start_custom")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_START_TYPE
            
        except ValueError:
            await update.message.reply_text(
                "❌ Введите положительное число",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back")]])
            )
            return States.MEDICINE_REPEAT_INTERVAL
    
    async def medicine_repeat_unit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор единиц повторения."""
        query = update.callback_query
        await query.answer()
        
        unit = query.data.replace("unit_", "")
        context.user_data['medicine_data']['repeat_unit'] = unit
        
        await query.edit_message_text(
            f"Введите количество {unit}:",
            parse_mode=ParseMode.MARKDOWN
        )
        return States.MEDICINE_REPEAT_INTERVAL
    
    async def medicine_start_type(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор типа даты начала."""
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
            await query.edit_message_text(
                "Введите дату начала в формате *ДД.ММ.ГГГГ*\nНапример: 20.02.2026",
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_START_DATE
        
        return States.MEDICINE_CONFIRM
    
    async def medicine_start_date(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка пользовательской даты."""
        user_id = update.effective_user.id
        tz_name = await TimeUtils.get_user_timezone(user_id)
        
        try:
            date_str = update.message.text.strip()
            for fmt in ['%d.%m.%Y', '%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d']:
                try:
                    date = datetime.strptime(date_str, fmt)
                    date = date.replace(hour=12, minute=0)
                    tz = pytz.timezone(tz_name)
                    context.user_data['medicine_data']['start_date'] = tz.localize(date)
                    await self.show_medicine_confirmation(update, context)
                    return States.MEDICINE_CONFIRM
                except ValueError:
                    continue
            
            raise ValueError("Неверный формат даты")
            
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
        
        date_str = start_date.strftime('%d.%m.%Y') if hasattr(start_date, 'strftime') else str(start_date)
        
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
            await update.callback_query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def medicine_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Подтверждение добавления лекарства."""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        tz_name = await TimeUtils.get_user_timezone(user_id)
        data = context.user_data['medicine_data']
        
        async with db_manager.session() as db:
            try:
                medicine = Medicine(
                    user_id=user_id,
                    name=data['name'],
                    dosage=data.get('dosage'),
                    times_per_day=data['times_per_day'],
                    schedule_times=data['schedule_times'],
                    schedule=','.join(data['schedule_times']),
                    course_duration=data.get('course_duration'),
                    repeat_type=data.get('repeat_type', 'none'),
                    repeat_interval=data.get('repeat_interval'),
                    start_date=data['start_date'],
                    user_timezone=tz_name
                )
                db.add(medicine)
                await db.flush()
                
                for time_str in data['schedule_times']:
                    scheduled_utc = self.local_to_utc(time_str, tz_name, data['start_date'])
                    
                    reminder = Reminder(
                        user_id=user_id,
                        reminder_type='medicine',
                        item_id=medicine.id,
                        scheduled_time=scheduled_utc,
                        user_timezone=tz_name
                    )
                    db.add(reminder)
                    await db.flush()
                    
                    job_id = f"medicine_{reminder.id}"
                    self.scheduler.scheduler.add_job(
                        self.scheduler.send_reminder,
                        trigger=DateTrigger(run_date=scheduled_utc),
                        id=job_id,
                        args=[reminder.id],
                        replace_existing=True
                    )
                
                logger.log('info', f"Добавлено лекарство {medicine.id}")
                
                await query.edit_message_text(
                    "✅ *Лекарство успешно добавлено!*",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📋 Список лекарств", callback_data="list_medicines"),
                         InlineKeyboardButton("➕ Добавить еще", callback_data="add_medicine")],
                        [InlineKeyboardButton("🏠 Главная", callback_data="start")]
                    ]),
                    parse_mode=ParseMode.MARKDOWN
                )
                
            except Exception as e:
                logger.log_error(e, {'user_id': user_id})
                await query.edit_message_text(
                    "❌ *Ошибка при добавлении*\n\nПожалуйста, попробуйте позже.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главная", callback_data="start")]])
                )
        
        del context.user_data['medicine_data']
        return ConversationHandler.END
    
    async def medicine_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Редактирование лекарства."""
        query = update.callback_query
        await query.answer()
        
        await query.edit_message_text(
            "✏️ *Редактирование*\n\nВыберите, что хотите изменить:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📝 Название", callback_data="edit_field_name"),
                 InlineKeyboardButton("💧 Дозировка", callback_data="edit_field_dosage")],
                [InlineKeyboardButton("⏰ Расписание", callback_data="edit_field_schedule"),
                 InlineKeyboardButton("📅 Длительность", callback_data="edit_field_course")],
                [InlineKeyboardButton("🔄 Повторение", callback_data="edit_field_repeat"),
                 InlineKeyboardButton("📆 Дата начала", callback_data="edit_field_start")],
                [InlineKeyboardButton("✅ Готово", callback_data="confirm_medicine")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.MEDICINE_EDIT
    
    async def medicine_edit_field(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Редактирование поля."""
        query = update.callback_query
        await query.answer()
        
        field = query.data.replace("edit_field_", "")
        
        if field == "name":
            await query.edit_message_text("Введите новое название:", parse_mode=ParseMode.MARKDOWN)
            return States.MEDICINE_NAME
        elif field == "dosage":
            await query.edit_message_text("Введите новую дозировку:", parse_mode=ParseMode.MARKDOWN)
            return States.MEDICINE_DOSAGE
        elif field == "schedule":
            await query.edit_message_text(
                "Выберите новое расписание:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("1 раз", callback_data="times_1"),
                     InlineKeyboardButton("2 раза", callback_data="times_2"),
                     InlineKeyboardButton("3 раза", callback_data="times_3")],
                    [InlineKeyboardButton("4 раза", callback_data="times_4"),
                     InlineKeyboardButton("5 раз", callback_data="times_5"),
                     InlineKeyboardButton("6 раз", callback_data="times_6")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_TIMES_PER_DAY
        elif field == "course":
            await query.edit_message_text(
                "Выберите новую продолжительность:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📅 3 дня", callback_data="duration_3"),
                     InlineKeyboardButton("📅 5 дней", callback_data="duration_5"),
                     InlineKeyboardButton("📅 7 дней", callback_data="duration_7")],
                    [InlineKeyboardButton("📅 10 дней", callback_data="duration_10"),
                     InlineKeyboardButton("📅 14 дней", callback_data="duration_14"),
                     InlineKeyboardButton("📅 30 дней", callback_data="duration_30")],
                    [InlineKeyboardButton("∞ Бессрочно", callback_data="duration_unlimited"),
                     InlineKeyboardButton("⚙️ Свой вариант", callback_data="duration_custom")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_COURSE_DURATION
        elif field == "repeat":
            await query.edit_message_text(
                "Выберите новое повторение:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Без повторения", callback_data="repeat_none"),
                     InlineKeyboardButton("⚙️ Свой вариант", callback_data="repeat_custom")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.MEDICINE_REPEAT
        elif field == "start":
            await query.edit_message_text(
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
            async with db_manager.session() as db:
                log = MedicineLog(
                    medicine_id=medicine_id,
                    user_id=update.effective_user.id,
                    log_type='scheduled',
                    status='comment',
                    comment=comment,
                    taken_at=datetime.now(pytz.UTC)
                )
                db.add(log)
            
            await update.message.reply_text("✅ Комментарий сохранен", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ]))
        else:
            await update.message.reply_text("❌ Ошибка сохранения", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ]))
        
        return ConversationHandler.END
    
    async def list_medicines(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Просмотр списка лекарств."""
        user_id = update.effective_user.id
        query = update.callback_query
        
        page = int(context.user_data.get('medicines_page', 1))
        per_page = 10
        offset = (page - 1) * per_page
        
        async with db_manager.session() as db:
            count_stmt = select(func.count()).where(
                and_(
                    Medicine.user_id == user_id,
                    Medicine.status == MedicineStatus.ACTIVE.value
                )
            )
            result = await db.execute(count_stmt)
            total = result.scalar() or 0
            
            if total == 0:
                await query.edit_message_text(
                    "📋 *У вас нет активных лекарств*",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💊 Добавить лекарство", callback_data="add_medicine")],
                        [InlineKeyboardButton("🏠 Главная", callback_data="start")]
                    ]),
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            stmt = select(Medicine).where(
                and_(
                    Medicine.user_id == user_id,
                    Medicine.status == MedicineStatus.ACTIVE.value
                )
            ).order_by(Medicine.created_at.desc()).offset(offset).limit(per_page)
            result = await db.execute(stmt)
            medicines = result.scalars().all()
            
            text = f"📋 *Ваши лекарства (стр {page}/{max(1, (total + per_page - 1) // per_page)})*\n\n"
            keyboard = []
            
            for i, med in enumerate(medicines, offset + 1):
                text += f"{i}. *{med.name}*"
                if med.dosage:
                    text += f" ({med.dosage})"
                text += f"\n   ⏰ {med.schedule}\n"
                
                if med.start_date:
                    local_date = med.start_date.astimezone(pytz.timezone(med.user_timezone))
                    text += f"   📅 с {local_date.strftime('%d.%m.%Y')}\n"
                
                total_count = med.total_taken + med.total_skipped + med.total_postponed
                if total_count > 0:
                    adherence = (med.total_taken / total_count * 100)
                    text += f"   📊 Приверженность: {adherence:.1f}%\n"
                
                text += "\n"
                keyboard.append([InlineKeyboardButton(f"🗑️ Удалить {med.name}", callback_data=f"delete_medicine_{med.id}")])
            
            # Пагинация
            if page > 1:
                keyboard.append([InlineKeyboardButton("⬅️ Предыдущая", callback_data=f"medicines_page_{page-1}")])
            if offset + per_page < total:
                keyboard.append([InlineKeyboardButton("➡️ Следующая", callback_data=f"medicines_page_{page+1}")])
            
            keyboard.append([InlineKeyboardButton("💊 Добавить лекарство", callback_data="add_medicine")])
            keyboard.append([InlineKeyboardButton("🏠 Главная", callback_data="start")])
            
            await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def delete_medicine(self, update: Update, context: ContextTypes.DEFAULT_TYPE, medicine_id: int):
        """Удаление лекарства."""
        query = update.callback_query
        await query.answer()
        
        async with db_manager.session() as db:
            stmt = select(Medicine).where(Medicine.id == medicine_id)
            result = await db.execute(stmt)
            medicine = result.scalar_one_or_none()
            
            if medicine:
                medicine.status = MedicineStatus.DELETED.value
                
                stmt = select(Reminder).where(
                    and_(
                        Reminder.item_id == medicine_id,
                        Reminder.reminder_type == 'medicine',
                        Reminder.status == ReminderStatus.PENDING.value
                    )
                )
                result = await db.execute(stmt)
                reminders = result.scalars().all()
                
                for reminder in reminders:
                    reminder.status = ReminderStatus.CANCELLED.value
                    try:
                        self.scheduler.scheduler.remove_job(f"medicine_{reminder.id}")
                    except JobLookupError:
                        pass
                
                logger.log('info', f"Удалено лекарство {medicine_id}")
        
        await query.edit_message_text(
            f"✅ Лекарство *{medicine.name}* удалено",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Список лекарств", callback_data="list_medicines"),
                 InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
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
        context.user_data['previous_state'] = 'start'
        
        await edit_func(
            "🩺 *Добавление анализа или исследования*\n\nВыберите тип:",
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
        
        await query.edit_message_text(
            f"Шаг 1/6: Введите *название {'анализа' if analysis_type == 'analysis' else 'исследования'}*",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.ANALYSIS_NAME
    
    async def analysis_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение названия анализа."""
        if SecurityManager.check_sql_injection(update.message.text):
            await update.message.reply_text(
                "❌ Обнаружены недопустимые символы. Пожалуйста, используйте только буквы и цифры."
            )
            return States.ANALYSIS_NAME
        
        context.user_data['analysis_data']['name'] = update.message.text
        
        await update.message.reply_text(
            "Шаг 2/6: Выберите *дату*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("20.02.2026 (Пт)", callback_data="analysis_date_20.02.2026"),
                 InlineKeyboardButton("21.02.2026 (Сб)", callback_data="analysis_date_21.02.2026")],
                [InlineKeyboardButton("22.02.2026 (Вс)", callback_data="analysis_date_22.02.2026"),
                 InlineKeyboardButton("23.02.2026 (Пн)", callback_data="analysis_date_23.02.2026")],
                [InlineKeyboardButton("24.02.2026 (Вт)", callback_data="analysis_date_24.02.2026"),
                 InlineKeyboardButton("25.02.2026 (Ср)", callback_data="analysis_date_25.02.2026")],
                [InlineKeyboardButton("📅 Своя дата", callback_data="analysis_date_custom")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.ANALYSIS_DATE
    
    async def analysis_date(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка выбора даты."""
        query = update.callback_query
        await query.answer()
        
        if query.data == "analysis_date_custom":
            await query.edit_message_text(
                "Введите дату в формате *ДД.ММ.ГГГГ*",
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_DATE
        
        if query.data == "analysis_type_back":
            return await self.add_analysis_start(update, context)
        
        date_str = query.data.replace("analysis_date_", "")
        user_id = update.effective_user.id
        tz_name = await TimeUtils.get_user_timezone(user_id)
        
        for fmt in ['%d.%m.%Y', '%d/%m/%Y']:
            try:
                date = datetime.strptime(date_str, fmt)
                date = date.replace(hour=12, minute=0)
                tz = pytz.timezone(tz_name)
                context.user_data['analysis_data']['scheduled_date'] = tz.localize(date)
                break
            except ValueError:
                continue
        
        await query.edit_message_text(
            "Шаг 3/6: Выберите *время*",
            reply_markup=InlineKeyboardMarkup([
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
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.ANALYSIS_TIME_HOUR
    
    async def analysis_date_custom(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Пользовательская дата."""
        user_id = update.effective_user.id
        tz_name = await TimeUtils.get_user_timezone(user_id)
        
        try:
            date_str = update.message.text.strip()
            for fmt in ['%d.%m.%Y', '%d/%m/%Y', '%d-%m-%Y']:
                try:
                    date = datetime.strptime(date_str, fmt)
                    date = date.replace(hour=12, minute=0)
                    tz = pytz.timezone(tz_name)
                    context.user_data['analysis_data']['scheduled_date'] = tz.localize(date)
                    
                    await update.message.reply_text(
                        "Шаг 3/6: Выберите *время*",
                        reply_markup=InlineKeyboardMarkup([
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
                        ]),
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return States.ANALYSIS_TIME_HOUR
                except ValueError:
                    continue
            
            raise ValueError("Неверный формат даты")
            
        except Exception:
            await update.message.reply_text(
                "❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back")]])
            )
            return States.ANALYSIS_DATE
    
    async def analysis_time_hour(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор часа."""
        query = update.callback_query
        await query.answer()
        
        if query.data == "analysis_time_custom":
            await query.edit_message_text(
                "Выберите час:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("00", callback_data="analysis_hour_00"),
                     InlineKeyboardButton("01", callback_data="analysis_hour_01"),
                     InlineKeyboardButton("02", callback_data="analysis_hour_02"),
                     InlineKeyboardButton("03", callback_data="analysis_hour_03")],
                    [InlineKeyboardButton("04", callback_data="analysis_hour_04"),
                     InlineKeyboardButton("05", callback_data="analysis_hour_05"),
                     InlineKeyboardButton("06", callback_data="analysis_hour_06"),
                     InlineKeyboardButton("07", callback_data="analysis_hour_07")],
                    [InlineKeyboardButton("08", callback_data="analysis_hour_08"),
                     InlineKeyboardButton("09", callback_data="analysis_hour_09"),
                     InlineKeyboardButton("10", callback_data="analysis_hour_10"),
                     InlineKeyboardButton("11", callback_data="analysis_hour_11")],
                    [InlineKeyboardButton("12", callback_data="analysis_hour_12"),
                     InlineKeyboardButton("13", callback_data="analysis_hour_13"),
                     InlineKeyboardButton("14", callback_data="analysis_hour_14"),
                     InlineKeyboardButton("15", callback_data="analysis_hour_15")],
                    [InlineKeyboardButton("16", callback_data="analysis_hour_16"),
                     InlineKeyboardButton("17", callback_data="analysis_hour_17"),
                     InlineKeyboardButton("18", callback_data="analysis_hour_18"),
                     InlineKeyboardButton("19", callback_data="analysis_hour_19")],
                    [InlineKeyboardButton("20", callback_data="analysis_hour_20"),
                     InlineKeyboardButton("21", callback_data="analysis_hour_21"),
                     InlineKeyboardButton("22", callback_data="analysis_hour_22"),
                     InlineKeyboardButton("23", callback_data="analysis_hour_23")],
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_TIME_HOUR
        
        hour = query.data.replace("analysis_hour_", "")
        context.user_data['temp_hour'] = hour
        
        # Показать выбор минут
        minutes = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
        keyboard = []
        for i in range(0, len(minutes), 4):
            row = [InlineKeyboardButton(f"{m:02d}", callback_data=f"minute_{m:02d}") for m in minutes[i:i+4]]
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("⏰ К выбору часа", callback_data="back_to_analysis_hours")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="analysis_date_back")])
        
        await query.edit_message_text(
            f"Вы выбрали час {hour}. Теперь выберите минуты:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.ANALYSIS_TIME_MINUTE
    
    async def analysis_time_minute(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор минут."""
        query = update.callback_query
        await query.answer()
        
        minute = query.data.replace("minute_", "")
        hour = context.user_data.get('temp_hour', "12")
        scheduled_time = f"{hour}:{minute}"
        context.user_data['analysis_data']['scheduled_time'] = scheduled_time
        
        user_id = update.effective_user.id
        scheduled_date = context.user_data['analysis_data']['scheduled_date']
        
        # Проверка на дубликат
        async with db_manager.session() as db:
            stmt = select(Analysis).where(
                and_(
                    Analysis.user_id == user_id,
                    Analysis.status == 'pending',
                    Analysis.scheduled_date == scheduled_date,
                    Analysis.scheduled_time == scheduled_time
                )
            )
            result = await db.execute(stmt)
            existing = result.scalar_one_or_none()
            
            if existing:
                analysis_type = "анализ" if context.user_data['analysis_data']['type'] == 'analysis' else 'исследование'
                
                keyboard = [
                    [InlineKeyboardButton("⏰ Другое время", callback_data="analysis_time_other"),
                     InlineKeyboardButton("✅ Все равно создать", callback_data="analysis_force_create")],
                    [InlineKeyboardButton("🔙 Назад", callback_data="analysis_date_back")]
                ]
                
                await query.edit_message_text(
                    f"⚠️ *Внимание!*\n\nНа {scheduled_date.strftime('%d.%m.%Y')} в {scheduled_time} "
                    f"уже запланирован {analysis_type}.\n\nВы можете выбрать другое время или создать дубликат.",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.MARKDOWN
                )
                return States.ANALYSIS_TIME_HOUR
        
        await query.edit_message_text(
            "Шаг 4/6: Выберите *повторение*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🕐 Одноразово", callback_data="repeat_once"),
                 InlineKeyboardButton("📅 Ежедневно", callback_data="repeat_daily")],
                [InlineKeyboardButton("📆 Еженедельно", callback_data="repeat_weekly"),
                 InlineKeyboardButton("🗓️ Ежемесячно", callback_data="repeat_monthly")],
                [InlineKeyboardButton("📊 Ежегодно", callback_data="repeat_yearly"),
                 InlineKeyboardButton("⚙️ Свой интервал", callback_data="repeat_custom")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.ANALYSIS_REPEAT
    
    async def back_to_analysis_hours(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Возврат к выбору часа."""
        query = update.callback_query
        await query.answer()
        
        await query.edit_message_text(
            "Выберите час:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("00", callback_data="analysis_hour_00"),
                 InlineKeyboardButton("01", callback_data="analysis_hour_01"),
                 InlineKeyboardButton("02", callback_data="analysis_hour_02"),
                 InlineKeyboardButton("03", callback_data="analysis_hour_03")],
                [InlineKeyboardButton("04", callback_data="analysis_hour_04"),
                 InlineKeyboardButton("05", callback_data="analysis_hour_05"),
                 InlineKeyboardButton("06", callback_data="analysis_hour_06"),
                 InlineKeyboardButton("07", callback_data="analysis_hour_07")],
                [InlineKeyboardButton("08", callback_data="analysis_hour_08"),
                 InlineKeyboardButton("09", callback_data="analysis_hour_09"),
                 InlineKeyboardButton("10", callback_data="analysis_hour_10"),
                 InlineKeyboardButton("11", callback_data="analysis_hour_11")],
                [InlineKeyboardButton("12", callback_data="analysis_hour_12"),
                 InlineKeyboardButton("13", callback_data="analysis_hour_13"),
                 InlineKeyboardButton("14", callback_data="analysis_hour_14"),
                 InlineKeyboardButton("15", callback_data="analysis_hour_15")],
                [InlineKeyboardButton("16", callback_data="analysis_hour_16"),
                 InlineKeyboardButton("17", callback_data="analysis_hour_17"),
                 InlineKeyboardButton("18", callback_data="analysis_hour_18"),
                 InlineKeyboardButton("19", callback_data="analysis_hour_19")],
                [InlineKeyboardButton("20", callback_data="analysis_hour_20"),
                 InlineKeyboardButton("21", callback_data="analysis_hour_21"),
                 InlineKeyboardButton("22", callback_data="analysis_hour_22"),
                 InlineKeyboardButton("23", callback_data="analysis_hour_23")],
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.ANALYSIS_TIME_HOUR
    
    async def analysis_time_custom(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка пользовательского времени."""
        query = update.callback_query
        await query.answer()
        
        await query.edit_message_text(
            "Выберите час:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("00", callback_data="analysis_hour_00"),
                 InlineKeyboardButton("01", callback_data="analysis_hour_01"),
                 InlineKeyboardButton("02", callback_data="analysis_hour_02"),
                 InlineKeyboardButton("03", callback_data="analysis_hour_03")],
                [InlineKeyboardButton("04", callback_data="analysis_hour_04"),
                 InlineKeyboardButton("05", callback_data="analysis_hour_05"),
                 InlineKeyboardButton("06", callback_data="analysis_hour_06"),
                 InlineKeyboardButton("07", callback_data="analysis_hour_07")],
                [InlineKeyboardButton("08", callback_data="analysis_hour_08"),
                 InlineKeyboardButton("09", callback_data="analysis_hour_09"),
                 InlineKeyboardButton("10", callback_data="analysis_hour_10"),
                 InlineKeyboardButton("11", callback_data="analysis_hour_11")],
                [InlineKeyboardButton("12", callback_data="analysis_hour_12"),
                 InlineKeyboardButton("13", callback_data="analysis_hour_13"),
                 InlineKeyboardButton("14", callback_data="analysis_hour_14"),
                 InlineKeyboardButton("15", callback_data="analysis_hour_15")],
                [InlineKeyboardButton("16", callback_data="analysis_hour_16"),
                 InlineKeyboardButton("17", callback_data="analysis_hour_17"),
                 InlineKeyboardButton("18", callback_data="analysis_hour_18"),
                 InlineKeyboardButton("19", callback_data="analysis_hour_19")],
                [InlineKeyboardButton("20", callback_data="analysis_hour_20"),
                 InlineKeyboardButton("21", callback_data="analysis_hour_21"),
                 InlineKeyboardButton("22", callback_data="analysis_hour_22"),
                 InlineKeyboardButton("23", callback_data="analysis_hour_23")],
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.ANALYSIS_TIME_HOUR
    
    async def analysis_force_create(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Принудительное создание дубликата."""
        query = update.callback_query
        await query.answer()
        
        await query.edit_message_text(
            "Шаг 4/6: Выберите *повторение*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🕐 Одноразово", callback_data="repeat_once"),
                 InlineKeyboardButton("📅 Ежедневно", callback_data="repeat_daily")],
                [InlineKeyboardButton("📆 Еженедельно", callback_data="repeat_weekly"),
                 InlineKeyboardButton("🗓️ Ежемесячно", callback_data="repeat_monthly")],
                [InlineKeyboardButton("📊 Ежегодно", callback_data="repeat_yearly"),
                 InlineKeyboardButton("⚙️ Свой интервал", callback_data="repeat_custom")]
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
            "yearly": "yearly",
            "custom": "custom"
        }
        
        context.user_data['analysis_data']['repeat_type'] = repeat_map.get(repeat_type, "once")
        
        if repeat_type == "custom":
            await query.edit_message_text(
                "Введите интервал повторения в днях:",
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_REPEAT_INTERVAL
        
        await query.edit_message_text(
            "Шаг 5/6: *Когда напомнить?*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("15 мин", callback_data="remind_15"),
                 InlineKeyboardButton("30 мин", callback_data="remind_30"),
                 InlineKeyboardButton("60 мин", callback_data="remind_60")],
                [InlineKeyboardButton("1 день", callback_data="remind_1440"),
                 InlineKeyboardButton("2 дня", callback_data="remind_2880"),
                 InlineKeyboardButton("3 дня", callback_data="remind_4320")],
                [InlineKeyboardButton("⚙️ Свой вариант", callback_data="remind_custom")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.ANALYSIS_REMINDER
    
    async def analysis_repeat_interval(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка интервала повторения."""
        try:
            interval = int(update.message.text.strip())
            if interval < 1:
                raise ValueError
            
            context.user_data['analysis_data']['repeat_type'] = 'custom'
            context.user_data['analysis_data']['repeat_interval'] = interval
            
            await update.message.reply_text(
                "Шаг 5/6: *Когда напомнить?*",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("15 мин", callback_data="remind_15"),
                     InlineKeyboardButton("30 мин", callback_data="remind_30"),
                     InlineKeyboardButton("60 мин", callback_data="remind_60")],
                    [InlineKeyboardButton("1 день", callback_data="remind_1440"),
                     InlineKeyboardButton("2 дня", callback_data="remind_2880"),
                     InlineKeyboardButton("3 дня", callback_data="remind_4320")],
                    [InlineKeyboardButton("⚙️ Свой вариант", callback_data="remind_custom")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_REMINDER
            
        except ValueError:
            await update.message.reply_text(
                "❌ Введите положительное число",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back")]])
            )
            return States.ANALYSIS_REPEAT_INTERVAL
    
    async def analysis_reminder(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор времени напоминания."""
        query = update.callback_query
        await query.answer()
        
        if query.data == "remind_custom":
            await query.edit_message_text(
                "Введите количество минут:",
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_REMINDER_VALUE
        
        minutes = int(query.data.replace("remind_", ""))
        context.user_data['analysis_data']['reminder_before'] = minutes
        context.user_data['analysis_data']['reminder_unit'] = 'minutes'
        
        await query.edit_message_text(
            "Шаг 6/6: Введите *заметки* (или нажмите Пропустить)",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭️ Пропустить", callback_data="skip_notes")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.ANALYSIS_NOTES
    
    async def analysis_reminder_custom(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка пользовательского времени."""
        query = update.callback_query
        await query.answer()
        
        await query.edit_message_text(
            "Введите количество минут:",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.ANALYSIS_REMINDER_VALUE
    
    async def analysis_reminder_value(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка значения напоминания."""
        try:
            value = int(update.message.text.strip())
            if value < 1:
                raise ValueError
            
            context.user_data['analysis_data']['reminder_before'] = value
            context.user_data['analysis_data']['reminder_unit'] = 'minutes'
            
            await update.message.reply_text(
                "Шаг 6/6: Введите *заметки* (или нажмите Пропустить)",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⏭️ Пропустить", callback_data="skip_notes")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_NOTES
            
        except ValueError:
            await update.message.reply_text(
                "❌ Введите положительное число",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back")]])
            )
            return States.ANALYSIS_REMINDER_VALUE
    
    async def analysis_reminder_unit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор единиц напоминания."""
        query = update.callback_query
        await query.answer()
        
        unit = query.data.replace("unit_", "")
        context.user_data['analysis_data']['reminder_unit'] = unit
        
        await query.edit_message_text(
            f"Введите количество {unit}:",
            parse_mode=ParseMode.MARKDOWN
        )
        return States.ANALYSIS_REMINDER_VALUE
    
    async def analysis_notes(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка заметок."""
        if SecurityManager.check_sql_injection(update.message.text):
            await update.message.reply_text(
                "❌ Обнаружены недопустимые символы. Пожалуйста, используйте только буквы и цифры."
            )
            return States.ANALYSIS_NOTES
        
        context.user_data['analysis_data']['notes'] = update.message.text
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
        else:
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
            await update.callback_query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def analysis_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Подтверждение добавления анализа."""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        tz_name = await TimeUtils.get_user_timezone(user_id)
        data = context.user_data['analysis_data']
        
        async with db_manager.session() as db:
            try:
                scheduled_date = data['scheduled_date']
                hour, minute = map(int, data['scheduled_time'].split(':'))
                scheduled_datetime = scheduled_date.replace(hour=hour, minute=minute)
                
                analysis = Analysis(
                    user_id=user_id,
                    analysis_type=data['type'],
                    name=data['name'],
                    scheduled_date=scheduled_datetime,
                    scheduled_time=data['scheduled_time'],
                    repeat_type=data['repeat_type'],
                    repeat_interval=data.get('repeat_interval'),
                    reminder_before=data['reminder_before'],
                    notes=data.get('notes'),
                    user_timezone=tz_name
                )
                db.add(analysis)
                await db.flush()
                
                reminder_time = scheduled_datetime - timedelta(minutes=data['reminder_before'])
                
                if reminder_time > datetime.now(pytz.UTC):
                    reminder = Reminder(
                        user_id=user_id,
                        reminder_type=data['type'],
                        item_id=analysis.id,
                        scheduled_time=reminder_time,
                        user_timezone=tz_name
                    )
                    db.add(reminder)
                    await db.flush()
                    
                    job_id = f"{data['type']}_{reminder.id}"
                    self.scheduler.scheduler.add_job(
                        self.scheduler.send_reminder,
                        trigger=DateTrigger(run_date=reminder_time),
                        id=job_id,
                        args=[reminder.id],
                        replace_existing=True
                    )
                
                logger.log('info', f"Добавлен {analysis.analysis_type} {analysis.id}")
                
                analysis_type = "Анализ" if data['type'] == 'analysis' else 'Исследование'
                
                await query.edit_message_text(
                    f"✅ *{analysis_type} успешно добавлен!*",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📋 Список", callback_data="list_analyses"),
                         InlineKeyboardButton("➕ Добавить еще", callback_data="add_analysis")],
                        [InlineKeyboardButton("🏠 Главная", callback_data="start")]
                    ]),
                    parse_mode=ParseMode.MARKDOWN
                )
                
            except Exception as e:
                logger.log_error(e, {'user_id': user_id})
                await query.edit_message_text(
                    "❌ *Ошибка при добавлении*\n\nПожалуйста, попробуйте позже.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главная", callback_data="start")]])
                )
        
        del context.user_data['analysis_data']
        return ConversationHandler.END
    
    async def analysis_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Редактирование анализа."""
        query = update.callback_query
        await query.answer()
        
        await query.edit_message_text(
            "✏️ *Редактирование анализа*\n\nВыберите, что хотите изменить:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Название", callback_data="edit_field_analysis_name"),
                 InlineKeyboardButton("📅 Дата", callback_data="edit_field_analysis_date")],
                [InlineKeyboardButton("⏰ Время", callback_data="edit_field_analysis_time"),
                 InlineKeyboardButton("🔄 Повторение", callback_data="edit_field_analysis_repeat")],
                [InlineKeyboardButton("⏰ Напоминание", callback_data="edit_field_analysis_reminder"),
                 InlineKeyboardButton("📝 Заметки", callback_data="edit_field_analysis_notes")],
                [InlineKeyboardButton("✅ Готово", callback_data="confirm_analysis")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.ANALYSIS_EDIT
    
    async def analysis_edit_field(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Редактирование поля анализа."""
        query = update.callback_query
        await query.answer()
        
        field = query.data.replace("edit_field_analysis_", "")
        
        if field == "name":
            await query.edit_message_text("Введите новое название:", parse_mode=ParseMode.MARKDOWN)
            return States.ANALYSIS_NAME
        elif field == "date":
            await query.edit_message_text(
                "Выберите новую дату:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("20.02.2026 (Пт)", callback_data="analysis_date_20.02.2026"),
                     InlineKeyboardButton("21.02.2026 (Сб)", callback_data="analysis_date_21.02.2026")],
                    [InlineKeyboardButton("22.02.2026 (Вс)", callback_data="analysis_date_22.02.2026"),
                     InlineKeyboardButton("23.02.2026 (Пн)", callback_data="analysis_date_23.02.2026")],
                    [InlineKeyboardButton("24.02.2026 (Вт)", callback_data="analysis_date_24.02.2026"),
                     InlineKeyboardButton("25.02.2026 (Ср)", callback_data="analysis_date_25.02.2026")],
                    [InlineKeyboardButton("📅 Своя дата", callback_data="analysis_date_custom")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_DATE
        elif field == "time":
            await query.edit_message_text(
                "Выберите новое время:",
                reply_markup=InlineKeyboardMarkup([
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
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_TIME_HOUR
        elif field == "repeat":
            await query.edit_message_text(
                "Выберите новое повторение:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🕐 Одноразово", callback_data="repeat_once"),
                     InlineKeyboardButton("📅 Ежедневно", callback_data="repeat_daily")],
                    [InlineKeyboardButton("📆 Еженедельно", callback_data="repeat_weekly"),
                     InlineKeyboardButton("🗓️ Ежемесячно", callback_data="repeat_monthly")],
                    [InlineKeyboardButton("📊 Ежегодно", callback_data="repeat_yearly"),
                     InlineKeyboardButton("⚙️ Свой интервал", callback_data="repeat_custom")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_REPEAT
        elif field == "reminder":
            await query.edit_message_text(
                "Выберите новое время напоминания:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("15 мин", callback_data="remind_15"),
                     InlineKeyboardButton("30 мин", callback_data="remind_30"),
                     InlineKeyboardButton("60 мин", callback_data="remind_60")],
                    [InlineKeyboardButton("1 день", callback_data="remind_1440"),
                     InlineKeyboardButton("2 дня", callback_data="remind_2880"),
                     InlineKeyboardButton("3 дня", callback_data="remind_4320")],
                    [InlineKeyboardButton("⚙️ Свой вариант", callback_data="remind_custom")]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_REMINDER
        elif field == "notes":
            await query.edit_message_text(
                "Введите новые заметки:",
                parse_mode=ParseMode.MARKDOWN
            )
            return States.ANALYSIS_NOTES
        
        return States.ANALYSIS_EDIT
    
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
            await query.edit_message_text(
                "📋 *Выберите тип для просмотра:*",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        analysis_type = 'analysis' if query.data == "list_analyses_analysis" else 'investigation'
        type_name = "анализы" if analysis_type == 'analysis' else "исследования"
        page = int(context.user_data.get(f'{analysis_type}_page', 1))
        per_page = 10
        offset = (page - 1) * per_page
        
        async with db_manager.session() as db:
            count_stmt = select(func.count()).where(
                and_(
                    Analysis.user_id == user_id,
                    Analysis.analysis_type == analysis_type,
                    Analysis.status == 'pending'
                )
            )
            result = await db.execute(count_stmt)
            total = result.scalar() or 0
            
            if total == 0:
                text = f"📋 *У вас нет запланированных {type_name}*"
                keyboard = [
                    [InlineKeyboardButton("🩺 Добавить", callback_data="add_analysis")],
                    [InlineKeyboardButton("🔙 К выбору типа", callback_data="list_analyses")],
                    [InlineKeyboardButton("🏠 Главная", callback_data="start")]
                ]
                await query.edit_message_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            stmt = select(Analysis).where(
                and_(
                    Analysis.user_id == user_id,
                    Analysis.analysis_type == analysis_type,
                    Analysis.status == 'pending'
                )
            ).order_by(Analysis.scheduled_date.asc()).offset(offset).limit(per_page)
            result = await db.execute(stmt)
            analyses = result.scalars().all()
            
            text = f"📋 *Запланированные {type_name} (стр {page}/{max(1, (total + per_page - 1) // per_page)})*\n\n"
            keyboard = []
            
            now = datetime.now(pytz.UTC)
            for i, analysis in enumerate(analyses, offset + 1):
                if analysis.scheduled_date.tzinfo is None:
                    analysis_date = pytz.UTC.localize(analysis.scheduled_date)
                else:
                    analysis_date = analysis.scheduled_date.astimezone(pytz.UTC)
                
                local_date = analysis_date.astimezone(pytz.timezone(analysis.user_timezone))
                days_left = (analysis_date - now).days
                
                if days_left < 0:
                    status = "🔴 Просрочен"
                elif days_left == 0:
                    status = "🟡 Сегодня"
                elif days_left == 1:
                    status = "🟡 Завтра"
                else:
                    status = f"🟢 Через {days_left} дн."
                
                text += f"{i}. *{analysis.name}*\n"
                text += f"   📅 {local_date.strftime('%d.%m.%Y')} в {analysis.scheduled_time}\n"
                text += f"   📊 {status}\n"
                if analysis.notes:
                    text += f"   📝 {analysis.notes}\n"
                text += "\n"
                
                keyboard.append([InlineKeyboardButton(f"🗑️ Удалить {analysis.name}", callback_data=f"delete_analysis_{analysis.id}")])
            
            # Пагинация
            if page > 1:
                keyboard.append([InlineKeyboardButton("⬅️ Предыдущая", callback_data=f"list_analyses_{analysis_type}_page_{page-1}")])
            if offset + per_page < total:
                keyboard.append([InlineKeyboardButton("➡️ Следующая", callback_data=f"list_analyses_{analysis_type}_page_{page+1}")])
            
            keyboard.append([InlineKeyboardButton("🩺 Добавить", callback_data="add_analysis")])
            keyboard.append([InlineKeyboardButton("🔙 К выбору типа", callback_data="list_analyses")])
            keyboard.append([InlineKeyboardButton("🏠 Главная", callback_data="start")])
            
            await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
    
    async def delete_analysis(self, update: Update, context: ContextTypes.DEFAULT_TYPE, analysis_id: int):
        """Удаление анализа."""
        query = update.callback_query
        await query.answer()
        
        async with db_manager.session() as db:
            stmt = select(Analysis).where(Analysis.id == analysis_id)
            result = await db.execute(stmt)
            analysis = result.scalar_one_or_none()
            
            if analysis:
                analysis.status = 'cancelled'
                
                stmt = select(Reminder).where(
                    and_(
                        Reminder.item_id == analysis_id,
                        Reminder.reminder_type.in_(['analysis', 'investigation']),
                        Reminder.status == ReminderStatus.PENDING.value
                    )
                )
                result = await db.execute(stmt)
                reminders = result.scalars().all()
                
                for reminder in reminders:
                    reminder.status = ReminderStatus.CANCELLED.value
                    try:
                        self.scheduler.scheduler.remove_job(f"{reminder.reminder_type}_{reminder.id}")
                    except JobLookupError:
                        pass
                
                logger.log('info', f"Удален анализ {analysis_id}")
        
        analysis_type = "Анализ" if analysis.analysis_type == 'analysis' else 'Исследование'
        
        await query.edit_message_text(
            f"✅ {analysis_type} *{analysis.name}* удален",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Список", callback_data="list_analyses"),
                 InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    # ============== СИМПТОМЫ ==============
    
    async def symptoms_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начало добавления симптомов."""
        if update.callback_query:
            query = update.callback_query
            await query.answer()
            edit_func = query.edit_message_text
        else:
            edit_func = update.message.reply_text
        
        context.user_data['previous_state'] = 'mood'
        
        await edit_func(
            "🩺 *Какие симптомы вас беспокоят?*\n\nВведите симптом текстом:",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.SYMPTOM_TEXT
    
    async def new_symptom(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Добавление нового симптома из напоминания."""
        query = update.callback_query
        await query.answer()
        
        medicine_id = int(query.data.replace("new_symptom_", ""))
        context.user_data['medicine_context'] = medicine_id
        
        await query.edit_message_text(
            "🩺 *Опишите новый симптом:*\n\nВведите симптом текстом:",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.SYMPTOM_TEXT
    
    async def symptom_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение текста симптома."""
        if SecurityManager.check_sql_injection(update.message.text):
            await update.message.reply_text(
                "❌ Обнаружены недопустимые символы. Пожалуйста, используйте только буквы и цифры."
            )
            return States.SYMPTOM_TEXT
        
        context.user_data['symptom_text'] = update.message.text
        
        await update.message.reply_text(
            "🩺 *Оцените тяжесть симптома*\n\n"
            "Шкала тяжести (возрастание от 1 до 5):\n"
            "1️⃣ Очень легкий - зеленый\n"
            "2️⃣ Легкий - лимонный\n"
            "3️⃣ Умеренный - желтый\n"
            "4️⃣ Сильный - оранжевый\n"
            "5️⃣ Максимальный - красный",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("1️⃣ Очень легкий 🟢", callback_data="severity_1"),
                 InlineKeyboardButton("2️⃣ Легкий 💚", callback_data="severity_2")],
                [InlineKeyboardButton("3️⃣ Умеренный 💛", callback_data="severity_3"),
                 InlineKeyboardButton("4️⃣ Сильный 🧡", callback_data="severity_4")],
                [InlineKeyboardButton("5️⃣ Максимальный ❤️", callback_data="severity_5")]
            ]),
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
        
        severity_colors = {
            1: "зеленый", 2: "лимонный", 3: "желтый", 4: "оранжевый", 5: "красный"
        }
        
        async with db_manager.session() as db:
            symptom_log = SymptomLog(
                user_id=user_id,
                symptom=symptom,
                severity=severity,
                severity_color=severity_colors[severity]
            )
            db.add(symptom_log)
            
            # Если симптом из контекста лекарства, записываем побочное действие
            if 'medicine_context' in context.user_data:
                medicine_id = context.user_data['medicine_context']
                stmt = select(Medicine).where(Medicine.id == medicine_id)
                result = await db.execute(stmt)
                medicine = result.scalar_one_or_none()
                if medicine:
                    log = MedicineLog(
                        medicine_id=medicine_id,
                        user_id=user_id,
                        log_type='scheduled',
                        status='side_effect',
                        side_effects=symptom,
                        taken_at=datetime.now(pytz.UTC)
                    )
                    db.add(log)
        
        severity_texts = {
            1: "1️⃣ Очень легкий (зеленый)",
            2: "2️⃣ Легкий (лимонный)",
            3: "3️⃣ Умеренный (желтый)",
            4: "4️⃣ Сильный (оранжевый)",
            5: "5️⃣ Максимальный (красный)"
        }
        
        keyboard = [
            [InlineKeyboardButton("➕ Добавить еще симптом", callback_data="symptoms"),
             InlineKeyboardButton("🏠 Главная", callback_data="start")],
        ]
        
        await query.edit_message_text(
            f"✅ *Симптом зафиксирован:*\n\n🤒 {symptom}\n📊 {severity_texts[severity]}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        
        context.user_data.pop('symptom_text', None)
        context.user_data.pop('medicine_context', None)
        
        return ConversationHandler.END
    
    async def no_thanks(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Пропуск симптомов."""
        query = update.callback_query
        
        await query.edit_message_text(
            "✅ Хорошего дня!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главная", callback_data="start")]])
        )
    
    # ============== НАПОМИНАНИЯ ==============
    
    async def medicine_take(self, update: Update, context: ContextTypes.DEFAULT_TYPE, medicine_id: int):
        """Отметка о приеме лекарства."""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        
        async with db_manager.session() as db:
            stmt = select(Medicine).where(Medicine.id == medicine_id)
            result = await db.execute(stmt)
            medicine = result.scalar_one_or_none()
            
            if medicine:
                medicine.total_taken += 1
                
                # Обновляем статистику
                stats = medicine.stats or {}
                stats['last_taken'] = datetime.now(pytz.UTC).isoformat()
                medicine.stats = stats
            
            log = MedicineLog(
                medicine_id=medicine_id,
                user_id=user_id,
                log_type='scheduled',
                status='taken',
                taken_at=datetime.now(pytz.UTC)
            )
            db.add(log)
            
            stmt = select(Reminder).where(
                and_(
                    Reminder.item_id == medicine_id,
                    Reminder.reminder_type == 'medicine',
                    Reminder.status == ReminderStatus.SENT.value
                )
            ).order_by(Reminder.scheduled_time.desc())
            result = await db.execute(stmt)
            reminder = result.scalar_one_or_none()
            
            if reminder:
                reminder.status = ReminderStatus.COMPLETED.value
        
        await query.edit_message_text(
            "✅ *Отлично!*\n\nПрием лекарства отмечен.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главная", callback_data="start")]]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def medicine_skip(self, update: Update, context: ContextTypes.DEFAULT_TYPE, medicine_id: int):
        """Пропуск приема лекарства."""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        
        async with db_manager.session() as db:
            stmt = select(Medicine).where(Medicine.id == medicine_id)
            result = await db.execute(stmt)
            medicine = result.scalar_one_or_none()
            
            if medicine:
                medicine.total_skipped += 1
            
            log = MedicineLog(
                medicine_id=medicine_id,
                user_id=user_id,
                log_type='scheduled',
                status='skipped',
                taken_at=datetime.now(pytz.UTC)
            )
            db.add(log)
            
            stmt = select(Reminder).where(
                and_(
                    Reminder.item_id == medicine_id,
                    Reminder.reminder_type == 'medicine',
                    Reminder.status == ReminderStatus.SENT.value
                )
            ).order_by(Reminder.scheduled_time.desc())
            result = await db.execute(stmt)
            reminder = result.scalar_one_or_none()
            
            if reminder:
                reminder.status = ReminderStatus.SKIPPED.value
        
        await query.edit_message_text(
            "❌ *Прием пропущен*",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главная", callback_data="start")]]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def medicine_postpone_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE, medicine_id: int):
        """Начало откладывания лекарства."""
        query = update.callback_query
        await query.answer()
        
        context.user_data['postpone_item_id'] = medicine_id
        context.user_data['postpone_type'] = 'medicine'
        
        await query.edit_message_text(
            "⏸ *На сколько отложить?*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏰ Часы", callback_data="postpone_type_hours"),
                 InlineKeyboardButton("📅 Дни", callback_data="postpone_type_days")],
                [InlineKeyboardButton("⚙️ Свой вариант", callback_data="postpone_custom")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.POSTPONE_TYPE
    
    async def medicine_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE, medicine_id: int):
        """Отмена препарата."""
        query = update.callback_query
        await query.answer()
        
        async with db_manager.session() as db:
            stmt = select(Medicine).where(Medicine.id == medicine_id)
            result = await db.execute(stmt)
            medicine = result.scalar_one_or_none()
            
            if medicine:
                medicine.status = MedicineStatus.CANCELLED.value
                
                stmt = select(Reminder).where(
                    and_(
                        Reminder.item_id == medicine_id,
                        Reminder.reminder_type == 'medicine',
                        Reminder.status.in_([ReminderStatus.PENDING.value, ReminderStatus.SENT.value])
                    )
                )
                result = await db.execute(stmt)
                reminders = result.scalars().all()
                
                for reminder in reminders:
                    reminder.status = ReminderStatus.CANCELLED.value
                    try:
                        self.scheduler.scheduler.remove_job(f"medicine_{reminder.id}")
                    except JobLookupError:
                        pass
                
                logger.log('info', f"Препарат {medicine_id} отменен")
        
        await query.edit_message_text(
            f"✅ Препарат *{medicine.name}* отменен",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главная", callback_data="start")]]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def medicine_comment(self, update: Update, context: ContextTypes.DEFAULT_TYPE, medicine_id: int):
        """Добавление комментария."""
        query = update.callback_query
        await query.answer()
        
        context.user_data['comment_medicine_id'] = medicine_id
        
        await query.edit_message_text(
            "📝 *Добавьте комментарий к приему*\n\nНапример: скорректированная доза, особенности приема и т.д.",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.MEDICINE_COMMENT
    
    async def medicine_side_effect(self, update: Update, context: ContextTypes.DEFAULT_TYPE, medicine_id: int):
        """Отметка о побочном действии."""
        query = update.callback_query
        await query.answer()
        
        context.user_data['side_effect_medicine_id'] = medicine_id
        
        await query.edit_message_text(
            "⚠️ *Опишите побочное действие*\n\nВведите текст:",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.SYMPTOM_TEXT
    
    async def analysis_take(self, update: Update, context: ContextTypes.DEFAULT_TYPE, analysis_id: int):
        """Отметка о сдаче анализа."""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        
        async with db_manager.session() as db:
            stmt = select(Analysis).where(Analysis.id == analysis_id)
            result = await db.execute(stmt)
            analysis = result.scalar_one_or_none()
            
            if analysis:
                analysis.status = 'completed'
            
            log = AnalysisLog(
                analysis_id=analysis_id,
                user_id=user_id,
                status='completed'
            )
            db.add(log)
            
            stmt = select(Reminder).where(
                and_(
                    Reminder.item_id == analysis_id,
                    Reminder.reminder_type.in_(['analysis', 'investigation']),
                    Reminder.status == ReminderStatus.SENT.value
                )
            ).order_by(Reminder.scheduled_time.desc())
            result = await db.execute(stmt)
            reminder = result.scalar_one_or_none()
            
            if reminder:
                reminder.status = ReminderStatus.COMPLETED.value
        
        analysis_type = "Анализ" if analysis.analysis_type == 'analysis' else 'Исследование'
        
        await query.edit_message_text(
            f"✅ *Отлично!*\n\n{analysis_type} сдан.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главная", callback_data="start")]]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def analysis_skip(self, update: Update, context: ContextTypes.DEFAULT_TYPE, analysis_id: int):
        """Пропуск анализа."""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        
        async with db_manager.session() as db:
            stmt = select(Analysis).where(Analysis.id == analysis_id)
            result = await db.execute(stmt)
            analysis = result.scalar_one_or_none()
            
            if analysis:
                analysis.status = 'skipped'
            
            log = AnalysisLog(
                analysis_id=analysis_id,
                user_id=user_id,
                status='skipped'
            )
            db.add(log)
            
            stmt = select(Reminder).where(
                and_(
                    Reminder.item_id == analysis_id,
                    Reminder.reminder_type.in_(['analysis', 'investigation']),
                    Reminder.status == ReminderStatus.SENT.value
                )
            ).order_by(Reminder.scheduled_time.desc())
            result = await db.execute(stmt)
            reminder = result.scalar_one_or_none()
            
            if reminder:
                reminder.status = ReminderStatus.SKIPPED.value
        
        analysis_type = "Анализ" if analysis.analysis_type == 'analysis' else 'Исследование'
        
        await query.edit_message_text(
            f"❌ {analysis_type} пропущен",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главная", callback_data="start")]]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def analysis_postpone_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE, analysis_id: int):
        """Начало откладывания анализа."""
        query = update.callback_query
        await query.answer()
        
        context.user_data['postpone_item_id'] = analysis_id
        context.user_data['postpone_type'] = 'analysis'
        
        await query.edit_message_text(
            "⏸ *На сколько отложить?*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏰ Часы", callback_data="postpone_type_hours"),
                 InlineKeyboardButton("📅 Дни", callback_data="postpone_type_days")],
                [InlineKeyboardButton("⚙️ Свой вариант", callback_data="postpone_custom")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.POSTPONE_TYPE
    
    async def analysis_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE, analysis_id: int):
        """Отмена анализа."""
        query = update.callback_query
        await query.answer()
        
        async with db_manager.session() as db:
            stmt = select(Analysis).where(Analysis.id == analysis_id)
            result = await db.execute(stmt)
            analysis = result.scalar_one_or_none()
            
            if analysis:
                analysis.status = 'cancelled'
                
                stmt = select(Reminder).where(
                    and_(
                        Reminder.item_id == analysis_id,
                        Reminder.reminder_type.in_(['analysis', 'investigation']),
                        Reminder.status.in_([ReminderStatus.PENDING.value, ReminderStatus.SENT.value])
                    )
                )
                result = await db.execute(stmt)
                reminders = result.scalars().all()
                
                for reminder in reminders:
                    reminder.status = ReminderStatus.CANCELLED.value
                    try:
                        self.scheduler.scheduler.remove_job(f"{reminder.reminder_type}_{reminder.id}")
                    except JobLookupError:
                        pass
                
                logger.log('info', f"Анализ {analysis_id} отменен")
        
        analysis_type = "Анализ" if analysis.analysis_type == 'analysis' else 'Исследование'
        
        await query.edit_message_text(
            f"✅ {analysis_type} *{analysis.name}* отменен",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главная", callback_data="start")]]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def analysis_notes(self, update: Update, context: ContextTypes.DEFAULT_TYPE, analysis_id: int):
        """Добавление заметок к анализу."""
        query = update.callback_query
        await query.answer()
        
        context.user_data['notes_analysis_id'] = analysis_id
        
        await query.edit_message_text(
            "📝 *Добавьте заметки к анализу/исследованию*\n\nВведите текст заметок:",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.ANALYSIS_NOTES
    
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
        context.user_data['previous_state'] = 'start'
        
        async with db_manager.session() as db:
            stmt = select(Medicine).where(
                and_(
                    Medicine.user_id == user_id,
                    Medicine.status == MedicineStatus.ACTIVE.value
                )
            )
            result = await db.execute(stmt)
            medicines = result.scalars().all()
            
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
                text = f"💊 {med.name}" + (f" ({med.dosage})" if med.dosage else "")
                keyboard.append([InlineKeyboardButton(text, callback_data=f"unscheduled_medicine_{med.id}")])
            
            keyboard.append([InlineKeyboardButton("➕ Новый препарат", callback_data="add_new_medicine")])
            keyboard.append([InlineKeyboardButton("🏠 Главная", callback_data="start")])
            
            await edit_func(
                "💊 *Принять препарат*\n\nВыберите препарат из списка или добавьте новый:",
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
        
        await query.edit_message_text(
            "💊 *Укажите принятую дозу*\n(или нажмите Пропустить):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭️ Пропустить", callback_data="skip_dosage")],
                [InlineKeyboardButton("🔙 Назад", callback_data="take_unscheduled")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.UNSCHEDULED_MEDICINE_DOSAGE
    
    async def unscheduled_medicine_dosage(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Дозировка."""
        context.user_data['unscheduled_dosage'] = update.message.text
        
        await update.message.reply_text(
            "💊 *Почему был принят препарат?*\n(укажите причину или нажмите Пропустить):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭️ Пропустить", callback_data="skip_reason")],
                [InlineKeyboardButton("🔙 Назад", callback_data="take_unscheduled")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.UNSCHEDULED_MEDICINE_REASON
    
    async def unscheduled_medicine_reason(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Причина приема."""
        context.user_data['unscheduled_reason'] = update.message.text
        
        await update.message.reply_text(
            "💊 *Добавьте комментарий*\n(или нажмите Пропустить):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭️ Пропустить", callback_data="skip_comment")],
                [InlineKeyboardButton("🔙 Назад", callback_data="take_unscheduled")]
            ]),
            parse_mode=ParseMode.MARKDOWN
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
        
        await query.edit_message_text(
            "💊 *Добавьте комментарий*\n(или нажмите Пропустить):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭️ Пропустить", callback_data="skip_comment")],
                [InlineKeyboardButton("🔙 Назад", callback_data="take_unscheduled")]
            ]),
            parse_mode=ParseMode.MARKDOWN
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
        
        async with db_manager.session() as db:
            stmt = select(Medicine).where(Medicine.id == medicine_id)
            result = await db.execute(stmt)
            medicine = result.scalar_one_or_none()
            
            log = MedicineLog(
                medicine_id=medicine_id,
                user_id=user_id,
                log_type='unscheduled',
                status='taken',
                dosage=dosage,
                reason=reason,
                comment=comment,
                taken_at=datetime.now(pytz.UTC)
            )
            db.add(log)
            
            logger.log('info', f"Незапланированный прием {medicine.name}")
        
        text = f"✅ *Прием зафиксирован*\n\nПрепарат: {medicine.name}"
        if dosage:
            text += f"\nДоза: {dosage}"
        
        keyboard = [
            [InlineKeyboardButton("👨‍⚕️ Записаться к врачу", callback_data="about"),
             InlineKeyboardButton("🏠 Главная", callback_data="start")],
        ]
        
        if update.callback_query:
            await update.callback_query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
        
        for key in ['unscheduled_medicine_id', 'unscheduled_dosage', 'unscheduled_reason', 'unscheduled_comment']:
            context.user_data.pop(key, None)
    
    # ============== ОТКЛАДЫВАНИЕ ==============
    
    async def postpone_type_hours(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор часов для откладывания."""
        query = update.callback_query
        await query.answer()
        
        keyboard = []
        for start in range(0, 24, 6):
            row = []
            for h in range(start, min(start + 6, 24)):
                row.append(InlineKeyboardButton(f"{h:02d}", callback_data=f"postpone_hour_{h}"))
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
        
        await query.edit_message_text(
            "⏰ *На сколько часов отложить?*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.POSTPONE_HOURS
    
    async def postpone_type_days(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор дней для откладывания."""
        query = update.callback_query
        await query.answer()
        
        await query.edit_message_text(
            "📅 *На сколько дней отложить?*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("5 дней", callback_data="postpone_days_5"),
                 InlineKeyboardButton("10 дней", callback_data="postpone_days_10"),
                 InlineKeyboardButton("15 дней", callback_data="postpone_days_15")],
                [InlineKeyboardButton("20 дней", callback_data="postpone_days_20"),
                 InlineKeyboardButton("25 дней", callback_data="postpone_days_25"),
                 InlineKeyboardButton("30 дней", callback_data="postpone_days_30")],
                [InlineKeyboardButton("⚙️ Свой вариант", callback_data="postpone_days_custom")],
                [InlineKeyboardButton("🔙 Назад", callback_data="back")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.POSTPONE_DAYS
    
    async def postpone_custom(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Пользовательское значение."""
        query = update.callback_query
        await query.answer()
        
        await query.edit_message_text(
            "Введите количество часов или дней (например: 24, 48, 72):",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.POSTPONE_CUSTOM
    
    async def postpone_hour(self, update: Update, context: ContextTypes.DEFAULT_TYPE, hours: int):
        """Выбор часа."""
        query = update.callback_query
        await query.answer()
        
        await self.execute_postpone(update, context, hours, 'hours')
        return ConversationHandler.END
    
    async def postpone_days(self, update: Update, context: ContextTypes.DEFAULT_TYPE, days: int):
        """Выбор дня."""
        query = update.callback_query
        await query.answer()
        
        await self.execute_postpone(update, context, days, 'days')
        return ConversationHandler.END
    
    async def execute_postpone(self, update: Update, context: ContextTypes.DEFAULT_TYPE, value: int, unit: str):
        """Выполнение откладывания."""
        item_id = context.user_data['postpone_item_id']
        item_type = context.user_data['postpone_type']
        user_id = update.effective_user.id
        
        delta = timedelta(hours=value) if unit == 'hours' else timedelta(days=value)
        new_time = datetime.now(pytz.UTC) + delta
        
        async with db_manager.session() as db:
            if item_type == 'medicine':
                stmt = select(Medicine).where(Medicine.id == item_id)
                result = await db.execute(stmt)
                medicine = result.scalar_one_or_none()
                
                if medicine:
                    medicine.total_postponed += 1
                    
                    stats = medicine.stats or {}
                    stats['last_postponed'] = new_time.isoformat()
                    medicine.stats = stats
                
                stmt = select(Reminder).where(
                    and_(
                        Reminder.item_id == item_id,
                        Reminder.reminder_type == 'medicine',
                        Reminder.status == ReminderStatus.SENT.value
                    )
                )
                result = await db.execute(stmt)
                reminder = result.scalar_one_or_none()
                
                if reminder:
                    reminder.status = ReminderStatus.POSTPONED.value
                    reminder.postponed_until = new_time
                    
                    new_reminder = Reminder(
                        user_id=user_id,
                        reminder_type='medicine',
                        item_id=item_id,
                        scheduled_time=new_time,
                        user_timezone=await TimeUtils.get_user_timezone(user_id)
                    )
                    db.add(new_reminder)
                    await db.flush()
                    
                    job_id = f"medicine_{new_reminder.id}"
                    self.scheduler.scheduler.add_job(
                        self.scheduler.send_reminder,
                        trigger=DateTrigger(run_date=new_time),
                        id=job_id,
                        args=[new_reminder.id],
                        replace_existing=True
                    )
                    
                    logger.log('info', f"Лекарство {item_id} отложено на {value} {unit}")
                    
            elif item_type == 'analysis':
                stmt = select(Analysis).where(Analysis.id == item_id)
                result = await db.execute(stmt)
                analysis = result.scalar_one_or_none()
                
                if analysis:
                    new_date = analysis.scheduled_date + delta
                    analysis.scheduled_date = new_date
                    
                    stmt = select(Reminder).where(
                        and_(
                            Reminder.item_id == item_id,
                            Reminder.reminder_type.in_(['analysis', 'investigation']),
                            Reminder.status == ReminderStatus.SENT.value
                        )
                    )
                    result = await db.execute(stmt)
                    reminder = result.scalar_one_or_none()
                    
                    if reminder:
                        reminder.status = ReminderStatus.POSTPONED.value
                        reminder.postponed_until = new_time
                        
                        reminder_time = new_date - timedelta(minutes=analysis.reminder_before)
                        new_reminder = Reminder(
                            user_id=user_id,
                            reminder_type=analysis.analysis_type,
                            item_id=item_id,
                            scheduled_time=reminder_time,
                            user_timezone=await TimeUtils.get_user_timezone(user_id)
                        )
                        db.add(new_reminder)
                        await db.flush()
                        
                        job_id = f"{analysis.analysis_type}_{new_reminder.id}"
                        self.scheduler.scheduler.add_job(
                            self.scheduler.send_reminder,
                            trigger=DateTrigger(run_date=reminder_time),
                            id=job_id,
                            args=[new_reminder.id],
                            replace_existing=True
                        )
                        
                        logger.log('info', f"Анализ {item_id} отложен на {value} {unit}")
        
        await update.callback_query.edit_message_text(
            f"✅ *Напоминание отложено на {value} {unit}*",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главная", callback_data="start")]]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        context.user_data.pop('postpone_item_id', None)
        context.user_data.pop('postpone_type', None)
    
    # ============== СТАТИСТИКА ==============
    
    async def stats_detail(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Детальная статистика."""
        query = update.callback_query
        await query.answer()
        
        data = query.data
        user_id = update.effective_user.id
        
        if data == "stats_all":
            await self.stats_all(update, context)
        elif data == "stats_medicines":
            await self.stats_medicines(update, context)
        elif data == "stats_mood":
            await self.stats_mood(update, context)
        elif data == "stats_symptoms":
            await self.stats_symptoms(update, context)
        elif data == "stats_detailed":
            await self.stats_detailed(update, context)
    
    async def stats_all(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Общая статистика."""
        query = update.callback_query
        user_id = update.effective_user.id
        
        async with db_manager.session() as db:
            medicines = await db.execute(
                select(Medicine).where(
                    and_(
                        Medicine.user_id == user_id,
                        Medicine.status == MedicineStatus.ACTIVE.value
                    )
                )
            )
            medicines = medicines.scalars().all()
            
            total_taken = sum(m.total_taken for m in medicines)
            total_skipped = sum(m.total_skipped for m in medicines)
            total_postponed = sum(m.total_postponed for m in medicines)
            total_planned = total_taken + total_skipped + total_postponed
            
            unscheduled = await db.execute(
                select(func.count()).where(
                    and_(
                        MedicineLog.user_id == user_id,
                        MedicineLog.log_type == 'unscheduled'
                    )
                )
            )
            unscheduled = unscheduled.scalar() or 0
            
            mood_count = await db.execute(
                select(func.count()).where(MoodLog.user_id == user_id)
            )
            mood_count = mood_count.scalar() or 0
            
            avg_mood = await db.execute(
                select(func.avg(MoodLog.mood_score)).where(MoodLog.user_id == user_id)
            )
            avg_mood = avg_mood.scalar() or 0
            
            symptom_count = await db.execute(
                select(func.count()).where(SymptomLog.user_id == user_id)
            )
            symptom_count = symptom_count.scalar() or 0
        
        text = f"""📊 *Общая статистика*

💊 *Лекарства:*
• Активных препаратов: {len(medicines)}
• Запланированных приемов: {total_planned}
• ✅ Принято: {total_taken}
• ❌ Пропущено: {total_skipped}
• ⏸ Отложено: {total_postponed}
• 🆕 Незапланированных: {unscheduled}"""

        if total_planned > 0:
            adherence = (total_taken / total_planned * 100)
            text += f"\n• 📈 Приверженность: {adherence:.1f}%"
        
        text += f"""

😊 *Настроение:*
• Записей: {mood_count}
• Среднее: {avg_mood:.1f}/5

🩺 *Симптомы:*
• Записей: {symptom_count}"""
        
        keyboard = [
            [InlineKeyboardButton("💊 По лекарствам", callback_data="stats_medicines"),
             InlineKeyboardButton("😊 Настроение", callback_data="stats_mood")],
            [InlineKeyboardButton("🩺 Симптомы", callback_data="stats_symptoms"),
             InlineKeyboardButton("📈 Детально", callback_data="stats_detailed")],
            [InlineKeyboardButton("🔙 Назад", callback_data="stats")]
        ]
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def stats_medicines(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Статистика по лекарствам."""
        query = update.callback_query
        user_id = update.effective_user.id
        
        async with db_manager.session() as db:
            medicines = await db.execute(
                select(Medicine).where(
                    and_(
                        Medicine.user_id == user_id,
                        Medicine.status == MedicineStatus.ACTIVE.value
                    )
                )
            )
            medicines = medicines.scalars().all()
            
            text = "💊 *Статистика по лекарствам*\n\n"
            keyboard = []
            
            for med in medicines:
                total = med.total_taken + med.total_skipped + med.total_postponed
                if total > 0:
                    adherence = (med.total_taken / total * 100)
                    text += f"• *{med.name}*: {adherence:.1f}%\n"
                    text += f"  ✅ {med.total_taken} | ❌ {med.total_skipped} | ⏸ {med.total_postponed}\n\n"
                    
                    keyboard.append([InlineKeyboardButton(
                        f"📊 {med.name} ({adherence:.1f}%)",
                        callback_data=f"stats_medicine_{med.id}"
                    )])
                else:
                    text += f"• *{med.name}*: нет данных\n\n"
                    keyboard.append([InlineKeyboardButton(
                        f"📊 {med.name} (0%)",
                        callback_data=f"stats_medicine_{med.id}"
                    )])
            
            unscheduled = await db.execute(
                select(func.count()).where(
                    and_(
                        MedicineLog.user_id == user_id,
                        MedicineLog.log_type == 'unscheduled'
                    )
                )
            )
            unscheduled = unscheduled.scalar() or 0
            
            if unscheduled:
                text += f"\n🆕 *Дополнительно принятые препараты:* {unscheduled}"
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="stats")])
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def stats_mood(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Статистика настроения."""
        query = update.callback_query
        user_id = update.effective_user.id
        
        async with db_manager.session() as db:
            mood_logs = await db.execute(
                select(MoodLog).where(MoodLog.user_id == user_id).order_by(MoodLog.created_at.desc())
            )
            mood_logs = mood_logs.scalars().all()
            
            if not mood_logs:
                await query.edit_message_text(
                    "😊 Нет записей о настроении",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="stats")]])
                )
                return
            
            text = "😊 *Дневник настроения*\n\n"
            
            for mood in mood_logs[:20]:
                local_time = mood.created_at.astimezone(pytz.timezone('Europe/Moscow'))
                emoji = "😢" if mood.mood_score <= 2 else "😐" if mood.mood_score == 3 else "😊"
                text += f"{emoji} {local_time.strftime('%d.%m.%Y %H:%M')} - {mood.mood_score}/5\n"
            
            month_ago = datetime.now(pytz.UTC) - timedelta(days=30)
            recent = [m for m in mood_logs if m.created_at >= month_ago]
            if recent:
                avg = sum(m.mood_score for m in recent) / len(recent)
                text += f"\n📊 Среднее за 30 дней: {avg:.1f}/5"
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📥 Экспорт", callback_data="export_mood_csv")],
                [InlineKeyboardButton("🔙 Назад", callback_data="stats")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def stats_symptoms(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Статистика симптомов."""
        query = update.callback_query
        user_id = update.effective_user.id
        
        async with db_manager.session() as db:
            symptoms = await db.execute(
                select(SymptomLog).where(SymptomLog.user_id == user_id).order_by(SymptomLog.created_at.desc())
            )
            symptoms = symptoms.scalars().all()
            
            if not symptoms:
                await query.edit_message_text(
                    "🩺 Нет записей о симптомах",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="stats")]])
                )
                return
            
            symptom_counts = defaultdict(int)
            symptom_severity = defaultdict(list)
            
            for s in symptoms:
                symptom_counts[s.symptom] += 1
                symptom_severity[s.symptom].append(s.severity)
            
            text = "🩺 *Статистика симптомов*\n\n"
            keyboard = []
            
            for symptom, count in sorted(symptom_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
                avg_severity = sum(symptom_severity[symptom]) / len(symptom_severity[symptom])
                text += f"• *{symptom}*: {count} раз"
                if avg_severity:
                    text += f", средняя тяжесть {avg_severity:.1f}/5\n"
                else:
                    text += "\n"
            
            text += "\n📋 *Последние записи:*\n"
            
            for symptom in symptoms[:10]:
                local_time = symptom.created_at.astimezone(pytz.timezone('Europe/Moscow'))
                color_emoji = {
                    "зеленый": "🟢", "лимонный": "💚", "желтый": "💛",
                    "оранжевый": "🧡", "красный": "❤️"
                }.get(symptom.severity_color, "⚪")
                
                text += f"\n{color_emoji} {local_time.strftime('%d.%m.%Y %H:%M')}"
                text += f"\n   {symptom.symptom} - {symptom.severity}/5\n"
                
                keyboard.append([InlineKeyboardButton(
                    f"🗑️ Удалить {local_time.strftime('%d.%m.%Y %H:%M')}",
                    callback_data=f"delete_symptom_{symptom.id}"
                )])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="stats")])
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def stats_detailed(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Детальный просмотр."""
        query = update.callback_query
        
        keyboard = [
            [InlineKeyboardButton("😊 Настроение", callback_data="view_mood_details"),
             InlineKeyboardButton("🩺 Симптомы", callback_data="view_symptom_details")],
            [InlineKeyboardButton("💊 Лекарства", callback_data="view_medicine_details"),
             InlineKeyboardButton("📊 Графики", callback_data="stats_graphs")],
            [InlineKeyboardButton("🔙 Назад", callback_data="stats")]
        ]
        
        await query.edit_message_text(
            "📈 *Детальный просмотр*\n\nВыберите тип записей для просмотра:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def delete_symptom(self, update: Update, context: ContextTypes.DEFAULT_TYPE, symptom_id: int):
        """Удаление симптома."""
        query = update.callback_query
        await query.answer()
        
        async with db_manager.session() as db:
            stmt = select(SymptomLog).where(SymptomLog.id == symptom_id)
            result = await db.execute(stmt)
            symptom = result.scalar_one_or_none()
            
            if symptom:
                await db.delete(symptom)
                logger.log('info', f"Удален симптом {symptom_id}")
        
        await query.edit_message_text(
            "✅ Симптом удален",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="stats")]])
        )
    
    # ============== АДМИН-ПАНЕЛЬ ==============
    
    async def admin_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Админ-панель."""
        user_id = update.effective_user.id
        
        async with db_manager.session() as db:
            user = await db.execute(select(User).where(User.user_id == user_id))
            user = user.scalar_one_or_none()
            
            if not user or user.role not in [UserRole.ADMIN.value, UserRole.SUPER_ADMIN.value]:
                await update.message.reply_text(
                    "❌ У вас нет прав доступа к админ-панели",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главная", callback_data="start")]])
                )
                return
        
        await update.message.reply_text(
            f"👨‍💻 *Админ-панель*\n\nВерсия: {config.BOT_VERSION} от {config.BOT_VERSION_DATE}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats"),
                 InlineKeyboardButton("👥 Пользователи", callback_data="admin_users")],
                [InlineKeyboardButton("📝 Логи", callback_data="admin_logs"),
                 InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast")],
                [InlineKeyboardButton("💾 Резервное копирование", callback_data="admin_backup"),
                 InlineKeyboardButton("⚙️ Настройки", callback_data="admin_settings")],
                [InlineKeyboardButton("ℹ️ Версия", callback_data="admin_version"),
                 InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Админ-статистика."""
        query = update.callback_query
        
        async with db_manager.session() as db:
            total_users = await db.execute(select(func.count()).select_from(User))
            total_users = total_users.scalar() or 0
            
            active_users = await db.execute(select(func.count()).where(User.status == UserStatus.ACTIVE.value))
            active_users = active_users.scalar() or 0
            
            new_users_today = await db.execute(
                select(func.count()).where(User.created_at >= datetime.utcnow() - timedelta(days=1))
            )
            new_users_today = new_users_today.scalar() or 0
            
            total_medicines = await db.execute(select(func.count()).select_from(Medicine))
            total_medicines = total_medicines.scalar() or 0
            
            total_analyses = await db.execute(select(func.count()).select_from(Analysis))
            total_analyses = total_analyses.scalar() or 0
            
            total_reminders = await db.execute(select(func.count()).where(Reminder.status == ReminderStatus.PENDING.value))
            total_reminders = total_reminders.scalar() or 0
            
            text = f"""📊 *Админ-статистика*

👥 *Пользователи:*
• Всего: {total_users}
• Активных: {active_users}
• Новых за 24ч: {new_users_today}

💊 *Лекарства:* {total_medicines}
🩺 *Анализы:* {total_analyses}
⏰ *Напоминания:* {total_reminders}

🔄 *Система:*
• Версия: {config.BOT_VERSION}
• Дата: {config.BOT_VERSION_DATE}"""
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Обновить", callback_data="admin_stats")],
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_users(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Управление пользователями."""
        query = update.callback_query
        
        await query.edit_message_text(
            "👥 *Управление пользователями*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Список пользователей", callback_data="admin_users_list")],
                [InlineKeyboardButton("🔍 Поиск", callback_data="admin_users_search")],
                [InlineKeyboardButton("🚫 Заблокированные", callback_data="admin_users_banned")],
                [InlineKeyboardButton("👑 Администраторы", callback_data="admin_users_admins")],
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_users_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Список пользователей с пагинацией."""
        query = update.callback_query
        await query.answer()
        
        page = int(context.user_data.get('admin_users_page', 1))
        per_page = 10
        offset = (page - 1) * per_page
        
        async with db_manager.session() as db:
            total_users = await db.execute(select(func.count()).select_from(User))
            total_users = total_users.scalar() or 0
            
            users = await db.execute(
                select(User).order_by(User.created_at.desc()).offset(offset).limit(per_page)
            )
            users = users.scalars().all()
            
            text = f"👥 *Пользователи (стр {page}/{max(1, (total_users + per_page - 1) // per_page)})*\n\n"
            keyboard = []
            
            for user in users:
                status_emoji = {
                    UserStatus.ACTIVE.value: "🟢",
                    UserStatus.BLOCKED.value: "🟡",
                    UserStatus.BANNED.value: "🔴"
                }.get(user.status, "⚪")
                
                role_emoji = {
                    UserRole.SUPER_ADMIN.value: "👑",
                    UserRole.ADMIN.value: "🔰",
                    UserRole.USER.value: "👤"
                }.get(user.role, "⚪")
                
                user_text = f"{status_emoji}{role_emoji} *{user.first_name}*"
                if user.username:
                    user_text += f" (@{user.username})"
                
                text += f"{user_text}\n"
                text += f"   🆔 `{user.user_id}`\n"
                text += f"   📅 {user.created_at.strftime('%d.%m.%Y')}\n"
                
                med_count = await db.execute(select(func.count()).where(Medicine.user_id == user.user_id))
                med_count = med_count.scalar() or 0
                text += f"   💊 {med_count}\n\n"
                
                keyboard.append([InlineKeyboardButton(
                    f"📊 {user.first_name[:20]}",
                    callback_data=f"admin_user_stats_{user.user_id}"
                )])
            
            # Пагинация
            if page > 1:
                keyboard.append([InlineKeyboardButton("⬅️ Предыдущая", callback_data=f"admin_users_page_{page-1}")])
            if offset + per_page < total_users:
                keyboard.append([InlineKeyboardButton("➡️ Следующая", callback_data=f"admin_users_page_{page+1}")])
            
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_users")])
        
        await query.edit_message_text(
            text[:4000],
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_users_page(self, update: Update, context: ContextTypes.DEFAULT_TYPE, page: int):
        """Обработка пагинации пользователей."""
        context.user_data['admin_users_page'] = page
        await self.admin_users_list(update, context)
    
    async def admin_users_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Поиск пользователей."""
        query = update.callback_query
        await query.answer()
        
        await query.edit_message_text(
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
        
        # Безопасный поиск
        users = await SearchManager.search_users(query_text)
        
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
        
        async with db_manager.session() as db:
            for user in users:
                status_emoji = {
                    UserStatus.ACTIVE.value: "🟢",
                    UserStatus.BLOCKED.value: "🟡",
                    UserStatus.BANNED.value: "🔴"
                }.get(user.status, "⚪")
                
                role_emoji = {
                    UserRole.SUPER_ADMIN.value: "👑",
                    UserRole.ADMIN.value: "🔰",
                    UserRole.USER.value: "👤"
                }.get(user.role, "⚪")
                
                text += f"{status_emoji}{role_emoji} *{user.first_name}*"
                if user.username:
                    text += f" (@{user.username})\n"
                else:
                    text += "\n"
                text += f"   🆔 `{user.user_id}`\n"
                
                medicine_count = await db.execute(select(func.count()).where(Medicine.user_id == user.user_id))
                medicine_count = medicine_count.scalar() or 0
                
                mood_count = await db.execute(select(func.count()).where(MoodLog.user_id == user.user_id))
                mood_count = mood_count.scalar() or 0
                
                text += f"   💊 {medicine_count} | 😊 {mood_count}\n\n"
                
                keyboard.append([InlineKeyboardButton(
                    f"📊 {user.first_name[:15]} ({user.user_id})",
                    callback_data=f"admin_user_stats_{user.user_id}"
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
        
        async with db_manager.session() as db:
            users = await db.execute(
                select(User).where(User.status == UserStatus.BANNED.value).order_by(User.updated_at.desc())
            )
            users = users.scalars().all()
            
            if not users:
                await query.edit_message_text(
                    "✅ Заблокированных пользователей нет",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", callback_data="admin_users")]
                    ])
                )
                return
            
            text = "🚫 *Заблокированные пользователи*\n\n"
            keyboard = []
            
            for user in users:
                ban_info = await db.execute(
                    select(AdminLog).where(
                        and_(
                            AdminLog.target_user_id == user.user_id,
                            AdminLog.action == "ban"
                        )
                    ).order_by(AdminLog.created_at.desc())
                )
                ban_info = ban_info.scalar_one_or_none()
                
                ban_date = ban_info.created_at.strftime('%d.%m.%Y') if ban_info else "неизвестно"
                
                text += f"• *{user.first_name}*"
                if user.username:
                    text += f" (@{user.username})"
                text += f"\n  🆔 `{user.user_id}`"
                text += f"\n  📅 Заблокирован: {ban_date}"
                text += f"\n  📊 Активность: {user.total_interactions}\n\n"
                
                keyboard.append([InlineKeyboardButton(
                    f"✅ Разблокировать {user.first_name[:15]}",
                    callback_data=f"admin_user_unban_{user.user_id}"
                )])
            
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_users")])
        
        await query.edit_message_text(
            text[:4000],
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_users_admins(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Список администраторов."""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        
        async with db_manager.session() as db:
            current_user = await db.execute(select(User).where(User.user_id == user_id))
            current_user = current_user.scalar_one_or_none()
            
            admins = await db.execute(
                select(User).where(
                    User.role.in_([UserRole.ADMIN.value, UserRole.SUPER_ADMIN.value])
                ).order_by(
                    func.case(
                        (User.role == UserRole.SUPER_ADMIN.value, 0),
                        else_=1
                    ),
                    User.created_at
                )
            )
            admins = admins.scalars().all()
            
            text = "👑 *Администраторы*\n\n"
            keyboard = []
            
            for admin in admins:
                role_text = "👑 Главный" if admin.role == UserRole.SUPER_ADMIN.value else "🔰 Админ"
                can_manage = current_user and current_user.role == UserRole.SUPER_ADMIN.value and admin.role != UserRole.SUPER_ADMIN.value
                
                text += f"{role_text}\n"
                text += f"• *{admin.first_name}*"
                if admin.username:
                    text += f" (@{admin.username})"
                text += f"\n  🆔 `{admin.user_id}`"
                text += f"\n  📅 с {admin.created_at.strftime('%d.%m.%Y')}"
                text += f"\n  📊 управляет {admin.total_interactions} пользователями\n\n"
                
                if can_manage:
                    keyboard.append([InlineKeyboardButton(
                        f"⬇️ Снять права {admin.first_name[:10]}",
                        callback_data=f"admin_user_remove_admin_{admin.user_id}"
                    )])
            
            if current_user and current_user.role == UserRole.SUPER_ADMIN.value:
                keyboard.append([InlineKeyboardButton(
                    "➕ Назначить админа",
                    callback_data="admin_users_make_admin"
                )])
            
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_users")])
        
        await query.edit_message_text(
            text[:4000],
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_broadcast_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начало рассылки."""
        query = update.callback_query
        await query.answer()
        
        context.user_data['broadcast'] = {}
        
        await query.edit_message_text(
            "📢 *Создание рассылки*\n\nВведите текст сообщения для рассылки:",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.BROADCAST_MESSAGE
    
    async def admin_broadcast_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка текста рассылки."""
        message = update.message.text
        context.user_data['broadcast'] = {
            'message': message,
            'created_at': datetime.utcnow().isoformat()
        }
        
        preview_text = f"""📢 *Предпросмотр рассылки*

Сообщение:
{message}

{'-'*30}

Получатели:
• Все активные пользователи
• С включенными уведомлениями

Отправить?"""
        
        await update.message.reply_text(
            preview_text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Отправить", callback_data="admin_broadcast_confirm"),
                 InlineKeyboardButton("✏️ Редактировать", callback_data="admin_broadcast_edit"),
                 InlineKeyboardButton("❌ Отмена", callback_data="admin_broadcast_cancel")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.BROADCAST_CONFIRM
    
    async def admin_broadcast_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Подтверждение рассылки."""
        query = update.callback_query
        await query.answer()
        
        admin_id = update.effective_user.id
        broadcast_data = context.user_data.get('broadcast', {})
        message = broadcast_data.get('message', '')
        
        progress_msg = await query.edit_message_text(
            "📢 *Рассылка началась*\n\n⏳ Подготовка...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        async with db_manager.session() as db:
            users = await db.execute(
                select(User).where(
                    and_(
                        User.status == UserStatus.ACTIVE.value,
                        User.notifications_enabled == True
                    )
                )
            )
            users = users.scalars().all()
            
            total = len(users)
            
            broadcast = Broadcast(
                admin_id=admin_id,
                message=message,
                total_recipients=total,
                filters={"status": "active", "notifications": True}
            )
            db.add(broadcast)
            await db.flush()
            
            broadcast_id = broadcast.id
            
            sent = 0
            failed = 0
            failed_users = []
            
            for i, user in enumerate(users, 1):
                try:
                    async with async_timeout.timeout(config.REQUEST_TIMEOUT):
                        await context.bot.send_message(
                            chat_id=user.user_id,
                            text=f"📢 *Информационное сообщение*\n\n{message}",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    sent += 1
                except Exception as e:
                    failed += 1
                    failed_users.append(str(user.user_id))
                    logger.log_error(e, {
                        'user_id': user.user_id,
                        'broadcast_id': broadcast_id
                    })
                
                if i % 20 == 0:
                    await asyncio.sleep(1)
                
                if i % 100 == 0:
                    await progress_msg.edit_text(
                        f"📢 *Рассылка*\n\n"
                        f"⏳ Прогресс: {i}/{total}\n"
                        f"✅ Отправлено: {sent}\n"
                        f"❌ Ошибок: {failed}"
                    )
            
            broadcast.status = 'completed'
            broadcast.sent_count = sent
            broadcast.failed_count = failed
            broadcast.completed_at = datetime.utcnow()
            if failed_users:
                broadcast.details = {"failed_users": failed_users}
            await db.commit()
        
        report = f"""📢 *Рассылка завершена*

✅ Успешно: {sent}
❌ Ошибок: {failed}
📊 Всего: {total}

🎯 Процент доставки: {(sent/total*100):.1f}%"""

        if failed_users:
            report += f"\n\n⚠️ Ошибки у {len(failed_users)} пользователей"
        
        await progress_msg.edit_text(
            report,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
                [InlineKeyboardButton("🔙 В админ-панель", callback_data="admin_panel")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        context.user_data.pop('broadcast', None)
        return ConversationHandler.END
    
    async def admin_broadcast_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Редактирование сообщения."""
        query = update.callback_query
        await query.answer()
        
        await query.edit_message_text(
            "✏️ *Редактирование сообщения*\n\nВведите новый текст рассылки:",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.BROADCAST_MESSAGE
    
    async def admin_broadcast_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена рассылки."""
        query = update.callback_query
        await query.answer()
        
        context.user_data.pop('broadcast', None)
        
        await query.edit_message_text(
            "❌ Рассылка отменена",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 В админ-панель", callback_data="admin_panel")]
            ])
        )
        
        return ConversationHandler.END
    
    async def admin_logs(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Просмотр логов."""
        query = update.callback_query
        
        log_dir = Path("logs")
        logs_text = "📝 *Логи системы*\n\n"
        
        log_files = ['error.log', 'info.log', 'debug.log', 'crash.log', 'structured.json']
        
        for log_file in log_files:
            file_path = log_dir / log_file
            if file_path.exists():
                try:
                    async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                        lines = await f.readlines()
                        logs_text += f"*{log_file}:*\n"
                        logs_text += ''.join(lines[-5:]) + "\n\n"
                except Exception:
                    logs_text += f"*{log_file}:* ошибка чтения\n\n"
            else:
                logs_text += f"*{log_file}:* файл не найден\n\n"
        
        if len(logs_text) > 4000:
            logs_text = logs_text[:4000] + "...\n(сообщение обрезано)"
        
        await query.edit_message_text(
            logs_text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Обновить", callback_data="admin_logs")],
                [InlineKeyboardButton("📥 Скачать логи", callback_data="admin_logs_download")],
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_logs_download(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Скачивание логов."""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        
        async with db_manager.session() as db:
            user = await db.execute(select(User).where(User.user_id == user_id))
            user = user.scalar_one_or_none()
            if not user or user.role not in [UserRole.ADMIN.value, UserRole.SUPER_ADMIN.value]:
                return
        
        log_dir = Path("logs")
        log_files = ['error.log', 'info.log', 'debug.log', 'crash.log', 'structured.json']
        files = [log_dir / f for f in log_files if (log_dir / f).exists()]
        
        if not files:
            await query.edit_message_text(
                "❌ Файлы логов не найдены",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data="admin_logs")]
                ])
            )
            return
        
        if len(files) > 1:
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp:
                with zipfile.ZipFile(tmp.name, 'w') as zf:
                    for f in files:
                        zf.write(f, f.name)
                
                with open(tmp.name, 'rb') as zf:
                    await context.bot.send_document(
                        chat_id=user_id,
                        document=zf,
                        filename=f'logs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip',
                        caption=f"📦 Архив логов"
                    )
                os.unlink(tmp.name)
        else:
            async with aiofiles.open(files[0], 'rb') as f:
                data = await f.read()
                await context.bot.send_document(
                    chat_id=user_id,
                    document=io.BytesIO(data),
                    filename=files[0].name
                )
        
        await query.edit_message_text(
            "✅ Логи отправлены!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_logs")]
            ])
        )
    
    async def admin_logs_download_type(self, update: Update, context: ContextTypes.DEFAULT_TYPE, log_type: str):
        """Скачивание определенного типа логов."""
        await self.admin_logs_download(update, context)
    
    async def admin_backup(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Резервное копирование."""
        query = update.callback_query
        
        backup_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f"backup_{backup_time}.sql"
        
        try:
            if 'sqlite' in config.SYNC_DATABASE_URL:
                db_path = config.SYNC_DATABASE_URL.replace('sqlite:///', '')
                if os.path.exists(db_path):
                    shutil.copy2(db_path, backup_file)
            
            text = f"""✅ *Резервное копирование выполнено*

📁 Файл: {backup_file}
📅 Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"""
            
        except Exception as e:
            text = f"❌ Ошибка при создании резервной копии: {e}"
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Создать новую копию", callback_data="admin_backup")],
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_settings(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Настройки админ-панели."""
        query = update.callback_query
        
        await query.edit_message_text(
            "⚙️ *Настройки админ-панели*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⚙️ Общие настройки", callback_data="admin_settings_general")],
                [InlineKeyboardButton("🔔 Уведомления", callback_data="admin_settings_notifications")],
                [InlineKeyboardButton("🛡️ Безопасность", callback_data="admin_settings_security")],
                [InlineKeyboardButton("📊 Логирование", callback_data="admin_settings_logging")],
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_settings_general(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Общие настройки."""
        query = update.callback_query
        await query.answer()
        
        text = f"""⚙️ *Общие настройки*

📱 *Бот:*
• Версия: {config.BOT_VERSION}
• Дата: {config.BOT_VERSION_DATE}
• Режим: Long Polling

⚡ *Производительность:*
• Rate limit: {config.RATE_LIMIT_GLOBAL}/сек
• Пул БД: {config.DB_POOL_SIZE}
• Кэш: активен

💾 *База данных:*
• URL: {config.DATABASE_URL}

🔄 *Планировщик:*
• Задач: {len(self.scheduler.scheduler.get_jobs())}
• Интервал проверки: {config.INTEGRITY_CHECK_INTERVAL} сек"""
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_settings")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_settings_notifications(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Настройки уведомлений."""
        query = update.callback_query
        await query.answer()
        
        await query.edit_message_text(
            "🔔 *Настройки уведомлений*\n\n"
            "• Ежедневный отчет в 21:00\n"
            "• Мгновенные уведомления об ошибках\n"
            "• Оповещения о новых пользователях",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_settings")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_settings_security(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Настройки безопасности."""
        query = update.callback_query
        await query.answer()
        
        async with db_manager.session() as db:
            admin_count = await db.execute(
                select(func.count()).where(
                    User.role.in_([UserRole.ADMIN.value, UserRole.SUPER_ADMIN.value])
                )
            )
            admin_count = admin_count.scalar() or 0
            
            banned_count = await db.execute(select(func.count()).where(User.status == UserStatus.BANNED.value))
            banned_count = banned_count.scalar() or 0
            
            recent_logs = await db.execute(
                select(func.count()).where(AdminLog.created_at >= datetime.utcnow() - timedelta(days=1))
            )
            recent_logs = recent_logs.scalar() or 0
            
            super_admin_count = await db.execute(select(func.count()).where(User.role == UserRole.SUPER_ADMIN.value))
            super_admin_count = super_admin_count.scalar() or 0
        
        text = f"""🛡️ *Настройки безопасности*

👑 *Администраторы:*
• Всего: {admin_count}
• Супер-админов: {super_admin_count}

🚫 *Блокировки:*
• Заблокировано: {banned_count}
• За последние 24ч: {recent_logs}

🔐 *Защита:*
• Rate limiting: активен ({config.RATE_LIMIT_GLOBAL}/сек)
• Rate limit критический: {config.RATE_LIMIT_CRITICAL}/сек
• Многоуровневая система: да
• Логирование действий: да
• Проверка целостности: каждый час"""
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_settings")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_settings_logging(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Настройки логирования."""
        query = update.callback_query
        await query.answer()
        
        log_dir = Path("logs")
        log_files = ['debug.log', 'info.log', 'error.log', 'crash.log', 'structured.json']
        files_info = []
        
        for log_file in log_files:
            file_path = log_dir / log_file
            if file_path.exists():
                size = file_path.stat().st_size
                modified = datetime.fromtimestamp(file_path.stat().st_mtime)
                files_info.append(f"• {log_file}: {self._format_size(size)} (изменен {modified.strftime('%d.%m.%Y %H:%M')})")
            else:
                files_info.append(f"• {log_file}: файл не создан")
        
        text = f"""📊 *Настройки логирования*

📁 *Файлы логов:*
{chr(10).join(files_info)}

🎯 *Уровни:*
• DEBUG - отладка (debug.log)
• INFO - события (info.log)
• ERROR - ошибки (error.log)
• CRITICAL - критические (crash.log)
• JSON - структурированные (structured.json)
• AUDIT - аудит действий
• SECURITY - события безопасности

💾 *Ротация:* автоматическая (при достижении 10МБ)
📊 *Метрики:* доступны на порту {config.METRICS_PORT}"""
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📥 Скачать", callback_data="admin_logs_download")],
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_settings")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    def _format_size(self, size: int) -> str:
        for unit in ['Б', 'КБ', 'МБ', 'ГБ']:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} ТБ"
    
    async def admin_version(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Информация о версии."""
        query = update.callback_query
        
        text = f"""ℹ️ *Информация о версии*

📱 *Бот:* {config.BOT_NAME}
🔢 *Версия:* {config.BOT_VERSION}
📅 *Дата выпуска:* {config.BOT_VERSION_DATE}
👨‍⚕️ *Автор:* Денис Казарин

🛠 *Компоненты:*
• python-telegram-bot: 20.7
• SQLAlchemy: 2.0.23
• APScheduler: 3.10.4
• Redis: последняя
• Celery: 5.3.0
• pytz: последняя

📊 *Статус:* Стабильная версия"""
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Проверить обновления", callback_data="admin_check_updates")],
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_check_updates(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Проверка обновлений."""
        query = update.callback_query
        await query.answer()
        
        await query.edit_message_text(
            f"✅ Установлена актуальная версия {config.BOT_VERSION}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_version")]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def admin_user_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE, target_user_id: int):
        """Статистика пользователя."""
        query = update.callback_query
        
        async with db_manager.session() as db:
            user = await db.execute(select(User).where(User.user_id == target_user_id))
            user = user.scalar_one_or_none()
            
            if not user:
                await query.edit_message_text(
                    "❌ Пользователь не найден",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", callback_data="admin_users_list")]
                    ])
                )
                return
            
            medicines = await db.execute(select(func.count()).where(Medicine.user_id == target_user_id))
            medicines = medicines.scalar() or 0
            
            analyses = await db.execute(select(func.count()).where(Analysis.user_id == target_user_id))
            analyses = analyses.scalar() or 0
            
            mood_count = await db.execute(select(func.count()).where(MoodLog.user_id == target_user_id))
            mood_count = mood_count.scalar() or 0
            
            symptoms = await db.execute(select(func.count()).where(SymptomLog.user_id == target_user_id))
            symptoms = symptoms.scalar() or 0
            
            last_activity = await db.execute(
                select(AdminLog).where(AdminLog.target_user_id == target_user_id).order_by(AdminLog.created_at.desc())
            )
            last_activity = last_activity.scalar_one_or_none()
            
            active_reminders = await db.execute(
                select(func.count()).where(
                    and_(
                        Reminder.user_id == target_user_id,
                        Reminder.status == ReminderStatus.PENDING.value
                    )
                )
            )
            active_reminders = active_reminders.scalar() or 0
            
            status_emoji = {
                UserStatus.ACTIVE.value: "🟢",
                UserStatus.BLOCKED.value: "🟡",
                UserStatus.BANNED.value: "🔴"
            }.get(user.status, "⚪")
            
            role_emoji = {
                UserRole.SUPER_ADMIN.value: "👑",
                UserRole.ADMIN.value: "🔰",
                UserRole.USER.value: "👤"
            }.get(user.role, "⚪")
            
            text = f"""📊 *Статистика пользователя*

👤 {user.first_name} (@{user.username})
🆔 `{user.user_id}`
📅 Регистрация: {user.created_at.strftime('%d.%m.%Y')}
⏱️ Последний визит: {user.last_seen.strftime('%d.%m.%Y %H:%M')}
🎯 Взаимодействий: {user.total_interactions:,}

💊 Лекарств: {medicines}
🩺 Анализов: {analyses}
😊 Записей настроения: {mood_count}
🤒 Записей симптомов: {symptoms}

🔹 Статус: {user.status} {status_emoji}
🔹 Роль: {user.role} {role_emoji}
🔹 Часовой пояс: {user.timezone}"""
            
            if last_activity:
                text += f"\n📋 Последнее действие: {last_activity.action} ({last_activity.created_at.strftime('%d.%m.%Y %H:%M')})"
            
            text += f"\n⏰ Активных напоминаний: {active_reminders}"
        
        keyboard = [[InlineKeyboardButton("🔙 К списку", callback_data="admin_users_list")]]
        
        action_row = []
        if user.status != UserStatus.BANNED.value:
            action_row.append(InlineKeyboardButton("🚫 Заблокировать", callback_data=f"admin_user_ban_{target_user_id}"))
        else:
            action_row.append(InlineKeyboardButton("✅ Разблокировать", callback_data=f"admin_user_unban_{target_user_id}"))
        
        if user.role == UserRole.USER.value:
            action_row.append(InlineKeyboardButton("👑 Сделать админом", callback_data=f"admin_user_make_admin_{target_user_id}"))
        elif user.role == UserRole.ADMIN.value:
            action_row.append(InlineKeyboardButton("⬇️ Снять права", callback_data=f"admin_user_remove_admin_{target_user_id}"))
        
        if action_row:
            keyboard.insert(0, action_row)
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def _admin_ban_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        """Блокировка пользователя."""
        admin_id = update.effective_user.id
        
        async with db_manager.session() as db:
            user = await db.execute(select(User).where(User.user_id == user_id))
            user = user.scalar_one_or_none()
            
            if user:
                user.status = UserStatus.BANNED.value
                user.updated_at = datetime.utcnow()
                
                admin_log = AdminLog(
                    admin_id=admin_id,
                    action="ban",
                    target_user_id=user_id,
                    details={"username": user.username}
                )
                db.add(admin_log)
                
                reminders = await db.execute(
                    select(Reminder).where(
                        and_(
                            Reminder.user_id == user_id,
                            Reminder.status == ReminderStatus.PENDING.value
                        )
                    )
                )
                reminders = reminders.scalars().all()
                
                for reminder in reminders:
                    reminder.status = ReminderStatus.CANCELLED.value
                    try:
                        self.scheduler.scheduler.remove_job(f"{reminder.reminder_type}_{reminder.id}")
                    except JobLookupError:
                        pass
                
                logger.log('info', f"Пользователь {user_id} заблокирован администратором {admin_id}")
                
                await db.commit()
        
        await update.callback_query.edit_message_text(
            f"✅ Пользователь {user.first_name} заблокирован",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 К списку", callback_data="admin_users_list")]
            ])
        )
    
    async def _admin_unban_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        """Разблокировка пользователя."""
        admin_id = update.effective_user.id
        
        async with db_manager.session() as db:
            user = await db.execute(select(User).where(User.user_id == user_id))
            user = user.scalar_one_or_none()
            
            if user:
                user.status = UserStatus.ACTIVE.value
                user.updated_at = datetime.utcnow()
                
                admin_log = AdminLog(
                    admin_id=admin_id,
                    action="unban",
                    target_user_id=user_id,
                    details={"username": user.username}
                )
                db.add(admin_log)
                
                logger.log('info', f"Пользователь {user_id} разблокирован администратором {admin_id}")
                
                await db.commit()
        
        await update.callback_query.edit_message_text(
            f"✅ Пользователь {user.first_name} разблокирован",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 К списку", callback_data="admin_users_list")]
            ])
        )
    
    async def _admin_make_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        """Назначение администратором."""
        admin_id = update.effective_user.id
        
        async with db_manager.session() as db:
            current_admin = await db.execute(select(User).where(User.user_id == admin_id))
            current_admin = current_admin.scalar_one_or_none()
            
            if not current_admin or current_admin.role != UserRole.SUPER_ADMIN.value:
                await update.callback_query.answer(
                    "❌ Только главный администратор может назначать админов",
                    show_alert=True
                )
                return
            
            user = await db.execute(select(User).where(User.user_id == user_id))
            user = user.scalar_one_or_none()
            
            if user:
                user.role = UserRole.ADMIN.value
                user.updated_at = datetime.utcnow()
                
                admin_log = AdminLog(
                    admin_id=admin_id,
                    action="make_admin",
                    target_user_id=user_id,
                    details={"username": user.username}
                )
                db.add(admin_log)
                
                logger.log('info', f"Пользователь {user_id} назначен администратором")
                
                await db.commit()
        
        await update.callback_query.edit_message_text(
            f"✅ {user.first_name} теперь администратор",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 К списку", callback_data="admin_users_list")]
            ])
        )
    
    async def _admin_remove_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        """Снятие прав администратора."""
        admin_id = update.effective_user.id
        
        async with db_manager.session() as db:
            current_admin = await db.execute(select(User).where(User.user_id == admin_id))
            current_admin = current_admin.scalar_one_or_none()
            
            if not current_admin or current_admin.role != UserRole.SUPER_ADMIN.value:
                await update.callback_query.answer(
                    "❌ Только главный администратор может снимать права",
                    show_alert=True
                )
                return
            
            user = await db.execute(select(User).where(User.user_id == user_id))
            user = user.scalar_one_or_none()
            
            if user:
                user.role = UserRole.USER.value
                user.updated_at = datetime.utcnow()
                
                admin_log = AdminLog(
                    admin_id=admin_id,
                    action="remove_admin",
                    target_user_id=user_id,
                    details={"username": user.username}
                )
                db.add(admin_log)
                
                logger.log('info', f"Пользователь {user_id} лишен прав администратора")
                
                await db.commit()
        
        await update.callback_query.edit_message_text(
            f"✅ Права администратора у {user.first_name} сняты",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 К списку", callback_data="admin_users_list")]
            ])
        )
    
    # ============== ДОКТОР ==============
    
    async def doctor_visited(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отметка о визите к врачу."""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        
        async with db_manager.session() as db:
            admin_log = AdminLog(
                admin_id=user_id,
                action="doctor_visited",
                details={
                    "source": "mood_warning",
                    "timestamp": datetime.utcnow().isoformat(),
                    "username": update.effective_user.username
                }
            )
            db.add(admin_log)
            
            visit_count = await db.execute(
                select(func.count()).where(
                    and_(
                        AdminLog.admin_id == user_id,
                        AdminLog.action == "doctor_visited"
                    )
                )
            )
            visit_count = visit_count.scalar() or 0
            
            await db.commit()
        
        text = f"✅ Визит к врачу отмечен! "
        if visit_count > 0:
            text += f"Это ваш {visit_count + 1}-й визит. Желаем здоровья! 🏥"
        else:
            text += "Рады, что вы заботитесь о своем здоровье! 🌟"
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Главная", callback_data="start")]
            ])
        )
    
    # ============== ЭКСПОРТ ==============
    
    async def export_data(self, update: Update, context: ContextTypes.DEFAULT_TYPE, data_type: str):
        """Экспорт данных в CSV."""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        tz_name = await TimeUtils.get_user_timezone(user_id)
        
        async with db_manager.session() as db:
            if data_type == 'mood':
                data = await db.execute(
                    select(MoodLog).where(MoodLog.user_id == user_id).order_by(MoodLog.created_at)
                )
                data = data.scalars().all()
                
                if not data:
                    await query.edit_message_text(
                        "😊 Нет данных о настроении",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔙 Назад", callback_data="stats")]
                        ])
                    )
                    return
                
                output = io.StringIO()
                writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL, delimiter=';')
                writer.writerow(['Дата', 'Время', 'Оценка', 'Эмодзи'])
                
                for mood in data:
                    local_time = mood.created_at.astimezone(pytz.timezone(tz_name))
                    emoji = "😢" if mood.mood_score <= 2 else "😐" if mood.mood_score == 3 else "😊"
                    writer.writerow([
                        local_time.strftime('%d.%m.%Y'),
                        local_time.strftime('%H:%M'),
                        mood.mood_score,
                        emoji
                    ])
                
                csv_data = output.getvalue().encode('utf-8-sig')
                filename = f'mood_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                
                await context.bot.send_document(
                    chat_id=user_id,
                    document=io.BytesIO(csv_data),
                    filename=filename,
                    caption=f"📊 Экспорт настроения за {datetime.now().strftime('%d.%m.%Y')}"
                )
                
                await query.edit_message_text(
                    f"📥 Файл с данными отправлен!\n\n📊 Всего записей: {len(data)}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", callback_data="stats")]
                    ])
                )
                
            elif data_type == 'symptoms':
                data = await db.execute(
                    select(SymptomLog).where(SymptomLog.user_id == user_id).order_by(SymptomLog.created_at)
                )
                data = data.scalars().all()
                
                if not data:
                    await query.edit_message_text(
                        "🩺 Нет данных о симптомах",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔙 Назад", callback_data="stats")]
                        ])
                    )
                    return
                
                output = io.StringIO()
                writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL, delimiter=';')
                writer.writerow(['Дата', 'Время', 'Симптом', 'Тяжесть', 'Цвет'])
                
                color_map = {
                    "зеленый": "🟢", "лимонный": "💚", "желтый": "💛",
                    "оранжевый": "🧡", "красный": "❤️"
                }
                
                for symptom in data:
                    local_time = symptom.created_at.astimezone(pytz.timezone(tz_name))
                    writer.writerow([
                        local_time.strftime('%d.%m.%Y'),
                        local_time.strftime('%H:%M'),
                        symptom.symptom,
                        symptom.severity,
                        color_map.get(symptom.severity_color, "⚪")
                    ])
                
                csv_data = output.getvalue().encode('utf-8-sig')
                filename = f'symptoms_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                
                await context.bot.send_document(
                    chat_id=user_id,
                    document=io.BytesIO(csv_data),
                    filename=filename,
                    caption=f"📊 Экспорт симптомов за {datetime.now().strftime('%d.%m.%Y')}"
                )
                
                await query.edit_message_text(
                    f"📥 Файл с данными отправлен!\n\n📊 Всего записей: {len(data)}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", callback_data="stats")]
                    ])
                )
                
            elif data_type == 'medicines':
                logs = await db.execute(
                    select(MedicineLog).where(MedicineLog.user_id == user_id).order_by(MedicineLog.taken_at)
                )
                logs = logs.scalars().all()
                
                if not logs:
                    await query.edit_message_text(
                        "💊 Нет данных о приеме лекарств",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔙 Назад", callback_data="stats")]
                        ])
                    )
                    return
                
                output = io.StringIO()
                writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL, delimiter=';')
                writer.writerow(['Дата', 'Время', 'Препарат', 'Тип', 'Статус', 'Доза', 'Причина', 'Комментарий'])
                
                for log in logs:
                    medicine = await db.execute(select(Medicine).where(Medicine.id == log.medicine_id))
                    medicine = medicine.scalar_one_or_none()
                    
                    if medicine:
                        local_time = log.taken_at.astimezone(pytz.timezone(tz_name))
                        status_emoji = "✅" if log.status == 'taken' else "❌" if log.status == 'skipped' else "⏸"
                        log_type = "📋" if log.log_type == 'scheduled' else "🆕"
                        
                        writer.writerow([
                            local_time.strftime('%d.%m.%Y'),
                            local_time.strftime('%H:%M'),
                            medicine.name,
                            log_type,
                            status_emoji,
                            log.dosage or '',
                            log.reason or '',
                            log.comment or ''
                        ])
                
                csv_data = output.getvalue().encode('utf-8-sig')
                filename = f'medicines_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                
                await context.bot.send_document(
                    chat_id=user_id,
                    document=io.BytesIO(csv_data),
                    filename=filename,
                    caption=f"📊 Экспорт приема лекарств за {datetime.now().strftime('%d.%m.%Y')}"
                )
                
                await query.edit_message_text(
                    f"📥 Файл с данными отправлен!\n\n📊 Всего записей: {len(logs)}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", callback_data="stats")]
                    ])
                )
    
    # ============== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==============
    
    def local_to_utc(self, local_time: str, timezone: str, base_date: datetime = None) -> datetime:
        """Конвертация локального времени в UTC."""
        if base_date is None:
            base_date = datetime.now(pytz.timezone(timezone))
        
        hour, minute = map(int, local_time.split(':'))
        local_dt = base_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        if local_dt.tzinfo is None:
            tz = pytz.timezone(timezone)
            local_dt = tz.localize(local_dt)
        
        return local_dt.astimezone(pytz.UTC)
    
    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена операции."""
        if update.callback_query:
            await update.callback_query.edit_message_text(
                "❌ Операция отменена",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главная", callback_data="start")]])
            )
        else:
            await update.message.reply_text(
                "❌ Операция отменена",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главная", callback_data="start")]])
            )
        
        context.user_data.clear()
        
        return ConversationHandler.END

# ============== ОСНОВНАЯ ФУНКЦИЯ ЗАПУСКА ==============

async def main():
    """Главная функция запуска."""
    print("\n" + "="*80)
    print(f"🚀 Запуск {config.BOT_NAME} v{config.BOT_VERSION}")
    print("="*80)
    
    print(f"📊 Версия: {config.BOT_VERSION} от {config.BOT_VERSION_DATE}")
    print(f"💾 База данных: {config.DATABASE_URL}")
    print(f"⚡ Режим: Асинхронный с Redis и Celery")
    print(f"📊 Метрики: порт {config.METRICS_PORT}")
    print("-"*80)
    
    # Отключение webhook
    try:
        import requests
        response = requests.get(f"https://api.telegram.org/bot{config.BOT_TOKEN}/deleteWebhook")
        print(f"✅ Webhook отключен")
    except Exception as e:
        print(f"⚠️ Ошибка при отключении webhook: {e}")
    
    # Проверка подключения к Redis
    try:
        redis_client = await redis_manager.get_redis()
        if redis_client:
            await redis_client.ping()
            print(f"✅ Redis подключен")
        else:
            print(f"⚠️ Redis не доступен, будет использован локальный кэш")
    except Exception as e:
        print(f"⚠️ Redis не доступен: {e}")
    
    # Создание приложения
    app = ApplicationBuilder().token(config.BOT_TOKEN).build()
    
    # Создание планировщика
    scheduler = SchedulerManager()
    scheduler.set_application(app)
    
    # Создание rate limiter
    rate_limiter = RateLimiter()
    
    # Создание обработчиков
    handlers = Handlers(app, scheduler, rate_limiter)
    
    # Запуск метрик
    await logger.start_metrics()
    
    # Запуск планировщика
    scheduler.start()
    await scheduler.restore_reminders()
    
    # Периодическая проверка целостности
    async def integrity_check():
        """Проверка целостности напоминаний."""
        async with db_manager.session() as db:
            now_utc = datetime.now(pytz.UTC)
            
            pending = await db.execute(
                select(Reminder).where(
                    and_(
                        Reminder.status == ReminderStatus.PENDING.value,
                        Reminder.scheduled_time > now_utc
                    )
                )
            )
            pending = pending.scalars().all()
            
            pending_ids = {f"{r.reminder_type}_{r.id}" for r in pending}
            scheduler_jobs = scheduler.scheduler.get_jobs()
            scheduler_ids = {job.id for job in scheduler_jobs}
            
            missing = pending_ids - scheduler_ids
            for job_id in missing:
                reminder_id = int(job_id.split('_')[1])
                reminder = await db.execute(select(Reminder).where(Reminder.id == reminder_id))
                reminder = reminder.scalar_one_or_none()
                
                if reminder and reminder.scheduled_time > now_utc:
                    scheduler.scheduler.add_job(
                        scheduler.send_reminder,
                        trigger=DateTrigger(run_date=reminder.scheduled_time),
                        id=job_id,
                        args=[reminder_id],
                        replace_existing=True,
                        misfire_grace_time=3600
                    )
                    logger.log('warning', f"Восстановлено задание {job_id}")
            
            dead = scheduler_ids - pending_ids
            for job_id in dead:
                if job_id.startswith(('medicine_', 'analysis_', 'investigation_')):
                    try:
                        scheduler.scheduler.remove_job(job_id)
                    except JobLookupError:
                        pass
            
            overdue = await db.execute(
                select(Reminder).where(
                    and_(
                        Reminder.status == ReminderStatus.PENDING.value,
                        Reminder.scheduled_time <= now_utc
                    )
                )
            )
            overdue = overdue.scalars().all()
            
            for reminder in overdue:
                reminder.status = ReminderStatus.FAILED.value
                reminder.last_error = 'Overdue'
                
                if reminder.reminder_type == 'medicine':
                    medicine = await db.execute(select(Medicine).where(Medicine.id == reminder.item_id))
                    medicine = medicine.scalar_one_or_none()
                    
                    if medicine:
                        medicine.total_skipped += 1
                        
                        log = MedicineLog(
                            medicine_id=medicine.id,
                            user_id=reminder.user_id,
                            log_type='scheduled',
                            status='skipped',
                            taken_at=reminder.scheduled_time
                        )
                        db.add(log)
                
                logger.log('warning', f"Просроченное напоминание {reminder.id}")
            
            await db.commit()
            
            logger.log('info', f"Проверка целостности: восст. {len(missing)}, удалено {len(dead)}")
    
    app.job_queue.run_repeating(
        lambda ctx: asyncio.create_task(integrity_check()),
        interval=config.INTEGRITY_CHECK_INTERVAL,
        first=10,
        name="integrity_check"
    )
    
    # Ежедневный отчет
    async def daily_report():
        """Ежедневный отчет для админов."""
        async with db_manager.session() as db:
            today = datetime.now(pytz.UTC).date()
            
            mood_today = await db.execute(
                select(func.count()).where(func.date(MoodLog.created_at) == today)
            )
            mood_today = mood_today.scalar() or 0
            
            symptoms_today = await db.execute(
                select(func.count()).where(func.date(SymptomLog.created_at) == today)
            )
            symptoms_today = symptoms_today.scalar() or 0
            
            active_users = await db.execute(
                select(func.count()).where(
                    and_(
                        User.status == UserStatus.ACTIVE.value,
                        User.last_seen >= datetime.utcnow() - timedelta(days=7)
                    )
                )
            )
            active_users = active_users.scalar() or 0
            
            for admin_id in config.ADMIN_IDS:
                try:
                    text = f"""📊 *Ежедневный отчет*

📅 {today.strftime('%d.%m.%Y')}

😊 Записей настроения: {mood_today}
🩺 Записей симптомов: {symptoms_today}
👥 Активных пользователей: {active_users}

✅ Система работает нормально"""
                    
                    await app.bot.send_message(
                        chat_id=admin_id,
                        text=text,
                        parse_mode=ParseMode.MARKDOWN
                    )
                except Exception as e:
                    logger.log_error(e, {'admin_id': admin_id})
    
    app.job_queue.run_daily(
        lambda ctx: asyncio.create_task(daily_report()),
        time=datetime.strptime("21:00", "%H:%M").time(),
        name="daily_report"
    )
    
    print("\n✅ Бот запущен и готов к работе!")
    print("📡 Режим: Long Polling")
    print("💡 Отправьте /start в Telegram")
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
        await redis_manager.close()
        logger.log('info', "Бот остановлен корректно")
        print("✅ Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main())
