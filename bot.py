#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ЛОР-Помощник Pro - Коммерческая версия
Telegram бот для управления приемом лекарств с поддержкой мультитенантности,
подписок и белого брендирования для клиник.

Версия: 13.0.0 (Коммерческая)
Автор: Денис Казарин (врач-оториноларинголог)
Лицензия: Проприетарная
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
from dataclasses import dataclass, field
from enum import Enum
import pytz
import json
import re
import uuid
import hashlib
import hmac
import secrets
import aiohttp
import asyncpg
import redis.asyncio as redis
from redis.asyncio import Redis
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import async_timeout
import backoff

# Отключаем предупреждения
import warnings
warnings.filterwarnings('ignore')

# ============== КОНФИГУРАЦИЯ ==============

@dataclass(frozen=True)
class Config:
    """Конфигурация коммерческого бота."""
    # Основные настройки
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
    BOT_VERSION: str = "13.0.0"
    BOT_VERSION_DATE: str = "06.03.2026"
    BOT_NAME: str = "ЛОР-Помощник Pro"
    
    # База данных (PostgreSQL вместо SQLite)
    DB_HOST: str = os.environ.get("DB_HOST", "localhost")
    DB_PORT: int = int(os.environ.get("DB_PORT", "5432"))
    DB_NAME: str = os.environ.get("DB_NAME", "lor_bot")
    DB_USER: str = os.environ.get("DB_USER", "lor_bot")
    DB_PASSWORD: str = os.environ.get("DB_PASSWORD", "")
    DB_POOL_SIZE: int = int(os.environ.get("DB_POOL_SIZE", "50"))
    DB_MAX_QUERIES: int = int(os.environ.get("DB_MAX_QUERIES", "50000"))
    
    # Redis для кэширования и очередей
    REDIS_URL: str = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    
    # Платежные системы
    TELEGRAM_PAYMENT_TOKEN: str = os.environ.get("TELEGRAM_PAYMENT_TOKEN", "")
    YOOKASSA_SHOP_ID: str = os.environ.get("YOOKASSA_SHOP_ID", "")
    YOOKASSA_SECRET_KEY: str = os.environ.get("YOOKASSA_SECRET_KEY", "")
    YOOKASSA_RETURN_URL: str = os.environ.get("YOOKASSA_RETURN_URL", "https://t.me/your_bot")
    
    # Webhook для уведомлений
    WEBHOOK_URL: str = os.environ.get("WEBHOOK_URL", "")
    WEBHOOK_SECRET: str = os.environ.get("WEBHOOK_SECRET", "")
    
    # Администраторы платформы
    PLATFORM_ADMIN_IDS: tuple = tuple(int(id) for id in os.environ.get("PLATFORM_ADMIN_IDS", "").split(",") if id)
    
    # Настройки производительности
    REQUEST_TIMEOUT: int = 10
    CACHE_TTL: int = 300
    RATE_LIMIT_USER: float = 0.5
    
    def __post_init__(self):
        if not self.BOT_TOKEN:
            raise ValueError("❌ Не установлен BOT_TOKEN")

config = Config()

# ============== МЕТРИКИ ==============

class Metrics:
    """Система метрик для мониторинга бизнес-показателей."""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._setup_metrics()
        return cls._instance
    
    def _setup_metrics(self):
        """Инициализация бизнес-метрик."""
        # Технические метрики
        self.requests = Counter('bot_requests', 'Requests', ['handler', 'tenant'])
        self.errors = Counter('bot_errors', 'Errors', ['type', 'tenant'])
        self.response_time = Histogram('bot_response_time', 'Response time', ['handler'])
        
        # Бизнес-метрики
        self.active_users = Gauge('business_active_users', 'Active users', ['tenant'])
        self.paid_users = Gauge('business_paid_users', 'Paid users', ['plan'])
        self.revenue = Counter('business_revenue', 'Revenue', ['plan', 'currency'])
        self.subscriptions = Counter('business_subscriptions', 'Subscriptions', ['plan', 'action'])
        self.tenant_count = Gauge('business_tenants', 'Active tenants')
        
        # Медицинские метрики
        self.medicines_tracked = Counter('medical_medicines', 'Medicines tracked', ['tenant'])
        self.adherence_rate = Histogram('medical_adherence', 'Adherence rate', ['tenant'])
        self.mood_logs = Counter('medical_mood', 'Mood logs', ['score'])
        self.symptom_logs = Counter('medical_symptoms', 'Symptom logs', ['severity'])
        
        # Запуск HTTP сервера для Prometheus
        try:
            start_http_server(9090)
            print(f"📊 Метрики доступны на порту 9090")
        except Exception as e:
            print(f"⚠️ Не удалось запустить метрики: {e}")

metrics = Metrics()

# ============== МОДЕЛИ ДАННЫХ ==============

class SubscriptionPlan(str, Enum):
    """Тарифные планы."""
    FREE = "free"
    PREMIUM = "premium"
    PROFESSIONAL = "professional"
    ENTERPRISE = "enterprise"

class UserRole(str, Enum):
    """Роли пользователей."""
    PATIENT = "patient"           # Обычный пациент
    DOCTOR = "doctor"              # Врач (арендатор)
    DOCTOR_ADMIN = "doctor_admin"  # Главный врач клиники
    TENANT_ADMIN = "tenant_admin"  # Администратор клиента
    PLATFORM_ADMIN = "platform_admin"  # Владелец платформы

class PaymentStatus(str, Enum):
    """Статусы платежей."""
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    REFUNDED = "refunded"
    CANCELLED = "cancelled"

class SubscriptionStatus(str, Enum):
    """Статусы подписок."""
    ACTIVE = "active"
    TRIAL = "trial"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    SUSPENDED = "suspended"

# ============== МЕНЕДЖЕР БАЗЫ ДАННЫХ (POSTGRESQL) ==============

class DatabaseManager:
    """Асинхронный менеджер PostgreSQL с пулом соединений."""
    
    _instance = None
    _pool: Optional[asyncpg.Pool] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    async def init_pool(self):
        """Инициализация пула соединений."""
        if not self._pool:
            self._pool = await asyncpg.create_pool(
                host=config.DB_HOST,
                port=config.DB_PORT,
                database=config.DB_NAME,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                min_size=10,
                max_size=config.DB_POOL_SIZE,
                max_queries=config.DB_MAX_QUERIES,
                command_timeout=config.REQUEST_TIMEOUT,
                ssl='require' if os.environ.get('DB_SSL') else None
            )
            print(f"✅ PostgreSQL пул соединений создан (размер: {config.DB_POOL_SIZE})")
    
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
                async with async_timeout.timeout(config.REQUEST_TIMEOUT):
                    yield conn
            except asyncio.TimeoutError:
                print(f"⚠️ Таймаут получения соединения из пула")
                raise
    
    async def execute(self, query: str, *args):
        """Выполнение запроса без возврата результата."""
        async with self.acquire() as conn:
            return await conn.execute(query, *args)
    
    async def fetch(self, query: str, *args):
        """Выполнение запроса с возвратом списка строк."""
        async with self.acquire() as conn:
            return await conn.fetch(query, *args)
    
    async def fetchrow(self, query: str, *args):
        """Выполнение запроса с возвратом одной строки."""
        async with self.acquire() as conn:
            return await conn.fetchrow(query, *args)
    
    async def fetchval(self, query: str, *args):
        """Выполнение запроса с возвратом одного значения."""
        async with self.acquire() as conn:
            return await conn.fetchval(query, *args)
    
    @backoff.on_exception(
        backoff.expo,
        (asyncpg.exceptions.ConnectionDoesNotExistError, 
         asyncpg.exceptions.InterfaceError),
        max_tries=3
    )
    async def execute_with_retry(self, query: str, *args):
        """Выполнение запроса с повторными попытками."""
        return await self.execute(query, *args)

db = DatabaseManager()

# ============== МЕНЕДЖЕР REDIS ==============

class RedisManager:
    """Менеджер Redis для кэширования и очередей."""
    
    _instance = None
    _redis: Optional[Redis] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    async def get_connection(self) -> Redis:
        """Получение подключения к Redis."""
        if not self._redis:
            self._redis = await redis.from_url(
                config.REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
                max_connections=20
            )
        return self._redis
    
    async def close(self):
        """Закрытие подключения."""
        if self._redis:
            await self._redis.close()
            self._redis = None
    
    async def cache_get(self, key: str) -> Optional[str]:
        """Получение значения из кэша."""
        redis = await self.get_connection()
        return await redis.get(key)
    
    async def cache_set(self, key: str, value: str, ttl: int = config.CACHE_TTL):
        """Сохранение значения в кэш."""
        redis = await self.get_connection()
        await redis.setex(key, ttl, value)
    
    async def cache_delete(self, key: str):
        """Удаление из кэша."""
        redis = await self.get_connection()
        await redis.delete(key)
    
    async def publish(self, channel: str, message: str):
        """Публикация сообщения в канал."""
        redis = await self.get_connection()
        await redis.publish(channel, message)
    
    async def enqueue(self, queue: str, data: dict):
        """Добавление задачи в очередь."""
        redis = await self.get_connection()
        await redis.rpush(queue, json.dumps(data))

redis_cache = RedisManager()

# ============== ПЛАТЕЖНЫЙ МЕНЕДЖЕР ==============

class PaymentManager:
    """Менеджер платежей с поддержкой Telegram Stars и ЮKassa."""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    @staticmethod
    def get_plan_prices(plan: SubscriptionPlan) -> dict:
        """Получение цен для тарифного плана."""
        prices = {
            SubscriptionPlan.PREMIUM: {
                "monthly": {"stars": 100, "rub": 199},
                "yearly": {"stars": 1000, "rub": 1990}
            },
            SubscriptionPlan.PROFESSIONAL: {
                "monthly": {"stars": 500, "rub": 990},
                "yearly": {"stars": 5000, "rub": 9900}
            },
            SubscriptionPlan.ENTERPRISE: {
                "monthly": {"stars": 5000, "rub": 9990},
                "yearly": {"stars": 50000, "rub": 99900}
            }
        }
        return prices.get(plan, {})
    
    async def create_telegram_stars_invoice(
        self,
        bot,
        user_id: int,
        plan: SubscriptionPlan,
        period: str = "monthly"
    ) -> Optional[str]:
        """Создание счета в Telegram Stars."""
        if not config.TELEGRAM_PAYMENT_TOKEN:
            return None
        
        prices = self.get_plan_prices(plan)
        if not prices:
            return None
        
        amount = prices[period]["stars"]
        plan_names = {
            SubscriptionPlan.PREMIUM: "Премиум",
            SubscriptionPlan.PROFESSIONAL: "Профессиональный",
            SubscriptionPlan.ENTERPRISE: "Корпоративный"
        }
        
        period_text = "месяц" if period == "monthly" else "год"
        
        try:
            invoice = await bot.create_invoice_link(
                title=f"Подписка {plan_names[plan]}",
                description=f"Доступ к расширенному функционалу на {period_text}",
                payload=f"subscription_{user_id}_{plan.value}_{period}_{int(time())}",
                provider_token=config.TELEGRAM_PAYMENT_TOKEN,
                currency="XTR",
                prices=[{"label": plan_names[plan], "amount": amount}],
                subscription_period=2592000 if period == "monthly" else 31536000  # 30 или 365 дней
            )
            return invoice
        except Exception as e:
            print(f"❌ Ошибка создания счета: {e}")
            return None
    
    async def create_yookassa_payment(
        self,
        user_id: int,
        plan: SubscriptionPlan,
        period: str = "monthly",
        email: str = None
    ) -> Optional[dict]:
        """Создание платежа через ЮKassa."""
        if not config.YOOKASSA_SHOP_ID or not config.YOOKASSA_SECRET_KEY:
            return None
        
        prices = self.get_plan_prices(plan)
        if not prices:
            return None
        
        amount = prices[period]["rub"]
        plan_names = {
            SubscriptionPlan.PREMIUM: "Премиум",
            SubscriptionPlan.PROFESSIONAL: "Профессиональный",
            SubscriptionPlan.ENTERPRISE: "Корпоративный"
        }
        
        period_text = "месяц" if period == "monthly" else "год"
        
        idempotence_key = str(uuid.uuid4())
        payment_data = {
            "amount": {
                "value": f"{amount}.00",
                "currency": "RUB"
            },
            "capture": True,
            "confirmation": {
                "type": "redirect",
                "return_url": config.YOOKASSA_RETURN_URL
            },
            "description": f"Подписка {plan_names[plan]} на {period_text}",
            "metadata": {
                "user_id": user_id,
                "plan": plan.value,
                "period": period
            },
            "receipt": {
                "customer": {
                    "email": email or f"user_{user_id}@example.com"
                },
                "items": [{
                    "description": f"Подписка {plan_names[plan]} на {period_text}",
                    "quantity": "1.00",
                    "amount": {
                        "value": f"{amount}.00",
                        "currency": "RUB"
                    },
                    "vat_code": 1,
                    "payment_subject": "service",
                    "payment_mode": "full_prepayment"
                }]
            }
        }
        
        auth = aiohttp.BasicAuth(
            config.YOOKASSA_SHOP_ID,
            config.YOOKASSA_SECRET_KEY
        )
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://api.yookassa.ru/v3/payments",
                    json=payment_data,
                    auth=auth,
                    headers={
                        "Idempotence-Key": idempotence_key,
                        "Content-Type": "application/json"
                    }
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        return {
                            "id": result["id"],
                            "confirmation_url": result["confirmation"]["confirmation_url"],
                            "amount": amount
                        }
        except Exception as e:
            print(f"❌ Ошибка создания платежа ЮKassa: {e}")
        
        return None

payments = PaymentManager()

# ============== МЕНЕДЖЕР ПОДПИСОК ==============

class SubscriptionManager:
    """Менеджер подписок и прав доступа."""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    async def init_tables(self):
        """Инициализация таблиц для подписок."""
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tenants (
                id SERIAL PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                slug VARCHAR(50) UNIQUE NOT NULL,
                admin_id BIGINT UNIQUE,
                settings JSONB DEFAULT '{}',
                branding JSONB DEFAULT '{}',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS subscription_plans (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                code VARCHAR(50) UNIQUE NOT NULL,
                price_monthly_stars INTEGER,
                price_yearly_stars INTEGER,
                price_monthly_rub DECIMAL(10,2),
                price_yearly_rub DECIMAL(10,2),
                features JSONB NOT NULL,
                max_patients INTEGER,
                max_doctors INTEGER,
                has_analytics BOOLEAN DEFAULT FALSE,
                has_export BOOLEAN DEFAULT FALSE,
                has_api BOOLEAN DEFAULT FALSE,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_subscriptions (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                tenant_id INTEGER REFERENCES tenants(id),
                plan_code VARCHAR(50) NOT NULL,
                status VARCHAR(50) NOT NULL,
                started_at TIMESTAMP NOT NULL,
                expires_at TIMESTAMP,
                auto_renew BOOLEAN DEFAULT TRUE,
                payment_method VARCHAR(50),
                payment_id VARCHAR(200),
                metadata JSONB DEFAULT '{}',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                INDEX idx_user_subscriptions_user (user_id),
                INDEX idx_user_subscriptions_status (status)
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                tenant_id INTEGER REFERENCES tenants(id),
                amount DECIMAL(10,2) NOT NULL,
                currency VARCHAR(10) NOT NULL,
                payment_method VARCHAR(50) NOT NULL,
                payment_system VARCHAR(50) NOT NULL,
                payment_id VARCHAR(200) UNIQUE,
                status VARCHAR(50) NOT NULL,
                description TEXT,
                metadata JSONB DEFAULT '{}',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                INDEX idx_payments_user (user_id),
                INDEX idx_payments_status (status)
            )
        """)
        
        # Инициализация тарифных планов
        await self.init_default_plans()
    
    async def init_default_plans(self):
        """Создание тарифных планов по умолчанию."""
        plans = [
            {
                "code": "free",
                "name": "Бесплатный",
                "features": {
                    "max_medicines": 3,
                    "max_analyses": 3,
                    "has_stats": True,
                    "has_export": False,
                    "has_analytics": False
                },
                "max_patients": 1,
                "max_doctors": 0,
                "has_analytics": False,
                "has_export": False,
                "has_api": False
            },
            {
                "code": "premium",
                "name": "Премиум",
                "price_monthly_stars": 100,
                "price_yearly_stars": 1000,
                "price_monthly_rub": 199,
                "price_yearly_rub": 1990,
                "features": {
                    "max_medicines": 100,
                    "max_analyses": 100,
                    "has_stats": True,
                    "has_export": True,
                    "has_analytics": True
                },
                "max_patients": 1,
                "max_doctors": 0,
                "has_analytics": True,
                "has_export": True,
                "has_api": False
            },
            {
                "code": "professional",
                "name": "Профессиональный",
                "price_monthly_stars": 500,
                "price_yearly_stars": 5000,
                "price_monthly_rub": 990,
                "price_yearly_rub": 9900,
                "features": {
                    "max_medicines": 1000,
                    "max_analyses": 1000,
                    "has_stats": True,
                    "has_export": True,
                    "has_analytics": True
                },
                "max_patients": 50,
                "max_doctors": 5,
                "has_analytics": True,
                "has_export": True,
                "has_api": True
            },
            {
                "code": "enterprise",
                "name": "Корпоративный",
                "price_monthly_stars": 5000,
                "price_yearly_stars": 50000,
                "price_monthly_rub": 9990,
                "price_yearly_rub": 99900,
                "features": {
                    "max_medicines": 10000,
                    "max_analyses": 10000,
                    "has_stats": True,
                    "has_export": True,
                    "has_analytics": True
                },
                "max_patients": 1000,
                "max_doctors": 50,
                "has_analytics": True,
                "has_export": True,
                "has_api": True
            }
        ]
        
        for plan in plans:
            await db.execute("""
                INSERT INTO subscription_plans (
                    code, name, price_monthly_stars, price_yearly_stars,
                    price_monthly_rub, price_yearly_rub, features,
                    max_patients, max_doctors, has_analytics, has_export, has_api
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (code) DO UPDATE SET
                    name = EXCLUDED.name,
                    price_monthly_stars = EXCLUDED.price_monthly_stars,
                    price_yearly_stars = EXCLUDED.price_yearly_stars,
                    price_monthly_rub = EXCLUDED.price_monthly_rub,
                    price_yearly_rub = EXCLUDED.price_yearly_rub,
                    features = EXCLUDED.features,
                    max_patients = EXCLUDED.max_patients,
                    max_doctors = EXCLUDED.max_doctors,
                    has_analytics = EXCLUDED.has_analytics,
                    has_export = EXCLUDED.has_export,
                    has_api = EXCLUDED.has_api
            """, 
                plan["code"], plan["name"],
                plan.get("price_monthly_stars"), plan.get("price_yearly_stars"),
                plan.get("price_monthly_rub"), plan.get("price_yearly_rub"),
                json.dumps(plan["features"]),
                plan["max_patients"], plan["max_doctors"],
                plan["has_analytics"], plan["has_export"], plan["has_api"]
            )
    
    async def create_tenant(
        self,
        name: str,
        admin_id: int,
        settings: dict = None,
        branding: dict = None
    ) -> int:
        """Создание нового арендатора (клиники/врача)."""
        slug = re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')
        
        # Убеждаемся, что slug уникален
        base_slug = slug
        counter = 1
        while True:
            existing = await db.fetchval(
                "SELECT id FROM tenants WHERE slug = $1",
                slug
            )
            if not existing:
                break
            slug = f"{base_slug}-{counter}"
            counter += 1
        
        tenant_id = await db.fetchval("""
            INSERT INTO tenants (name, slug, admin_id, settings, branding)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
        """, name, slug, admin_id, 
            json.dumps(settings or {}),
            json.dumps(branding or {})
        )
        
        # Создаем бесплатную подписку для администратора
        await self.create_subscription(
            user_id=admin_id,
            tenant_id=tenant_id,
            plan_code="free",
            status=SubscriptionStatus.ACTIVE,
            auto_renew=False
        )
        
        metrics.tenant_count.inc()
        return tenant_id
    
    async def get_tenant(self, tenant_id: int) -> Optional[dict]:
        """Получение информации об арендаторе."""
        return await db.fetchrow(
            "SELECT * FROM tenants WHERE id = $1",
            tenant_id
        )
    
    async def get_tenant_by_admin(self, admin_id: int) -> Optional[dict]:
        """Получение арендатора по ID администратора."""
        return await db.fetchrow(
            "SELECT * FROM tenants WHERE admin_id = $1",
            admin_id
        )
    
    async def get_user_tenant(self, user_id: int) -> Optional[dict]:
        """Получение арендатора для пользователя."""
        # Сначала проверяем, не является ли пользователь администратором
        tenant = await self.get_tenant_by_admin(user_id)
        if tenant:
            return tenant
        
        # Ищем подписку пользователя
        sub = await db.fetchrow("""
            SELECT t.* FROM tenants t
            JOIN user_subscriptions us ON us.tenant_id = t.id
            WHERE us.user_id = $1 AND us.status = $2
            LIMIT 1
        """, user_id, SubscriptionStatus.ACTIVE.value)
        
        return sub
    
    async def create_subscription(
        self,
        user_id: int,
        tenant_id: Optional[int],
        plan_code: str,
        status: SubscriptionStatus = SubscriptionStatus.ACTIVE,
        duration_days: int = 30,
        auto_renew: bool = True,
        payment_method: str = None,
        payment_id: str = None,
        metadata: dict = None
    ) -> int:
        """Создание подписки для пользователя."""
        now = datetime.utcnow()
        expires_at = now + timedelta(days=duration_days) if duration_days else None
        
        # Деактивируем предыдущие активные подписки
        await db.execute("""
            UPDATE user_subscriptions
            SET status = $1, updated_at = NOW()
            WHERE user_id = $2 AND status = $3
        """, SubscriptionStatus.EXPIRED.value, user_id, SubscriptionStatus.ACTIVE.value)
        
        sub_id = await db.fetchval("""
            INSERT INTO user_subscriptions (
                user_id, tenant_id, plan_code, status,
                started_at, expires_at, auto_renew,
                payment_method, payment_id, metadata
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING id
        """,
            user_id, tenant_id, plan_code, status.value,
            now, expires_at, auto_renew,
            payment_method, payment_id, json.dumps(metadata or {})
        )
        
        # Обновляем метрики
        metrics.subscriptions.labels(plan=plan_code, action="created").inc()
        
        # Если это платный план, обновляем счетчик платных пользователей
        if plan_code != "free":
            metrics.paid_users.labels(plan=plan_code).inc()
        
        return sub_id
    
    async def get_user_subscription(self, user_id: int) -> Optional[dict]:
        """Получение активной подписки пользователя."""
        sub = await db.fetchrow("""
            SELECT us.*, sp.*
            FROM user_subscriptions us
            JOIN subscription_plans sp ON sp.code = us.plan_code
            WHERE us.user_id = $1
                AND us.status = $2
                AND (us.expires_at IS NULL OR us.expires_at > NOW())
            ORDER BY us.created_at DESC
            LIMIT 1
        """, user_id, SubscriptionStatus.ACTIVE.value)
        
        return sub
    
    async def check_feature_access(
        self,
        user_id: int,
        feature: str,
        default_value: Any = None
    ) -> bool:
        """Проверка доступа к функции на основе подписки."""
        # Проверка кэша
        cache_key = f"feature:{user_id}:{feature}"
        cached = await redis_cache.cache_get(cache_key)
        if cached is not None:
            return cached == "true"
        
        sub = await self.get_user_subscription(user_id)
        
        if not sub:
            # Бесплатный доступ по умолчанию
            if feature in ["max_medicines", "max_analyses"]:
                await redis_cache.cache_set(cache_key, "true" if 3 > 0 else "false", 300)
                return True
            return False
        
        features = sub.get("features", {})
        has_access = features.get(feature, False)
        
        # Проверка лимитов
        if feature == "max_medicines":
            limit = features.get("max_medicines", 0)
            if limit > 0:
                # Считаем текущее количество лекарств
                count = await db.fetchval(
                    "SELECT COUNT(*) FROM medicines WHERE user_id = $1 AND status = 'active'",
                    user_id
                )
                has_access = count < limit
        
        await redis_cache.cache_set(cache_key, str(has_access).lower(), 300)
        return has_access
    
    async def check_rate_limit(self, user_id: int, resource: str, limit: int = 100) -> bool:
        """Проверка лимитов скорости."""
        key = f"ratelimit:{user_id}:{resource}"
        redis = await redis_cache.get_connection()
        
        current = await redis.incr(key)
        if current == 1:
            await redis.expire(key, 60)  # 1 минута
        
        return current <= limit
    
    async def log_payment(
        self,
        user_id: int,
        tenant_id: Optional[int],
        amount: float,
        currency: str,
        payment_method: str,
        payment_system: str,
        payment_id: str,
        status: PaymentStatus,
        description: str = None,
        metadata: dict = None
    ) -> int:
        """Логирование платежа."""
        payment_db_id = await db.fetchval("""
            INSERT INTO payments (
                user_id, tenant_id, amount, currency,
                payment_method, payment_system, payment_id,
                status, description, metadata
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING id
        """,
            user_id, tenant_id, amount, currency,
            payment_method, payment_system, payment_id,
            status.value, description, json.dumps(metadata or {})
        )
        
        if status == PaymentStatus.SUCCEEDED:
            metrics.revenue.labels(
                plan=metadata.get("plan", "unknown"),
                currency=currency
            ).inc(amount)
        
        return payment_db_id

subscriptions = SubscriptionManager()

# ============== БРЕНДИРОВАНИЕ ==============

class BrandingManager:
    """Менеджер брендирования для клиник."""
    
    _instance = None
    _brand_cache = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    async def get_branding(self, tenant_id: Optional[int]) -> dict:
        """Получение настроек бренда для арендатора."""
        if not tenant_id:
            return self.get_default_branding()
        
        # Проверка кэша
        if tenant_id in self._brand_cache:
            return self._brand_cache[tenant_id]
        
        tenant = await subscriptions.get_tenant(tenant_id)
        if not tenant:
            return self.get_default_branding()
        
        branding = tenant.get("branding", {})
        self._brand_cache[tenant_id] = branding
        return branding
    
    def get_default_branding(self) -> dict:
        """Настройки бренда по умолчанию."""
        return {
            "name": "ЛОР-Помощник",
            "description": "Персональный медицинский бот",
            "doctor_name": "Денис Казарин",
            "doctor_title": "врач-оториноларинголог",
            "welcome_text": "👋 *Здравствуйте!*\n\nЯ помогу вам следить за приемом лекарств и самочувствием.",
            "colors": {
                "primary": "#2A9D8F",
                "secondary": "#E9C46A"
            }
        }
    
    async def format_welcome_message(
        self,
        tenant_id: Optional[int],
        user_first_name: str
    ) -> str:
        """Форматирование приветственного сообщения с учетом бренда."""
        branding = await self.get_branding(tenant_id)
        
        text = f"""👋 *Здравствуйте, {user_first_name}!*

Я *{branding['name']}* — персональный медицинский бот.

👨‍⚕️ *О враче:* {branding['doctor_name']}, {branding['doctor_title']}

*Мои возможности:*
• 💊 Напоминания о приеме лекарств
• 🩺 Напоминания об анализах
• 📊 Отслеживание самочувствия
• 📈 Статистика и отчеты"""
        
        return text

branding = BrandingManager()

# ============== БЕЗОПАСНОСТЬ ==============

class SecurityManager:
    """Менеджер безопасности для коммерческого продукта."""
    
    @staticmethod
    def generate_api_key(tenant_id: int) -> str:
        """Генерация API ключа для арендатора."""
        random_part = secrets.token_urlsafe(32)
        hash_part = hashlib.sha256(f"{tenant_id}:{random_part}".encode()).hexdigest()[:16]
        return f"lk_{tenant_id}_{random_part}_{hash_part}"
    
    @staticmethod
    def verify_webhook_signature(payload: bytes, signature: str, secret: str) -> bool:
        """Проверка подписи webhook."""
        if not secret:
            return False
        expected = hmac.new(
            secret.encode(),
            payload,
            hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(expected, signature)
    
    @staticmethod
    async def check_tenant_access(user_id: int, tenant_id: int) -> bool:
        """Проверка доступа пользователя к арендатору."""
        # Платформенные администраторы имеют доступ ко всему
        if user_id in config.PLATFORM_ADMIN_IDS:
            return True
        
        # Проверяем, является ли пользователь администратором арендатора
        tenant = await subscriptions.get_tenant_by_admin(user_id)
        if tenant and tenant["id"] == tenant_id:
            return True
        
        # Проверяем подписку пользователя
        sub = await subscriptions.get_user_subscription(user_id)
        return sub and sub["tenant_id"] == tenant_id

# ============== МОДЕЛИ ДАННЫХ ПОЛЬЗОВАТЕЛЕЙ ==============

class UserTables:
    """Менеджер таблиц пользовательских данных."""
    
    @staticmethod
    async def init_tables():
        """Инициализация таблиц для пользовательских данных."""
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                tenant_id INTEGER REFERENCES tenants(id),
                username VARCHAR(100),
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                language VARCHAR(10) DEFAULT 'ru',
                timezone VARCHAR(50) DEFAULT 'Europe/Moscow',
                role VARCHAR(50) DEFAULT 'patient',
                status VARCHAR(50) DEFAULT 'active',
                has_seen_welcome BOOLEAN DEFAULT FALSE,
                first_seen TIMESTAMP DEFAULT NOW(),
                last_seen TIMESTAMP DEFAULT NOW(),
                total_interactions INTEGER DEFAULT 0,
                metadata JSONB DEFAULT '{}',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(user_id, tenant_id),
                INDEX idx_users_user (user_id),
                INDEX idx_users_tenant (tenant_id),
                INDEX idx_users_role (role)
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS medicines (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                tenant_id INTEGER REFERENCES tenants(id),
                name VARCHAR(200) NOT NULL,
                dosage VARCHAR(100),
                times_per_day INTEGER DEFAULT 1,
                schedule_times JSONB,
                schedule VARCHAR(200),
                course_duration INTEGER,
                repeat_type VARCHAR(20) DEFAULT 'none',
                repeat_interval INTEGER,
                start_date TIMESTAMP,
                user_timezone VARCHAR(50) NOT NULL,
                status VARCHAR(20) DEFAULT 'active',
                total_taken INTEGER DEFAULT 0,
                total_skipped INTEGER DEFAULT 0,
                total_postponed INTEGER DEFAULT 0,
                total_unscheduled INTEGER DEFAULT 0,
                stats JSONB DEFAULT '{}',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                INDEX idx_medicines_user (user_id),
                INDEX idx_medicines_tenant (tenant_id),
                INDEX idx_medicines_status (status)
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS analyses (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                tenant_id INTEGER REFERENCES tenants(id),
                name VARCHAR(200) NOT NULL,
                scheduled_date TIMESTAMP NOT NULL,
                scheduled_time VARCHAR(10) NOT NULL,
                repeat_type VARCHAR(20) DEFAULT 'once',
                repeat_interval INTEGER,
                reminder_before INTEGER DEFAULT 24,
                notes TEXT,
                status VARCHAR(20) DEFAULT 'pending',
                user_timezone VARCHAR(50) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                INDEX idx_analyses_user (user_id),
                INDEX idx_analyses_tenant (tenant_id),
                INDEX idx_analyses_status (status)
            )
        """)

# ============== ОСНОВНОЙ КЛАСС БОТА ==============

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ConversationHandler, MessageHandler, filters, ContextTypes,
    PreCheckoutQueryHandler, ShippingQueryHandler
)
from telegram.constants import ParseMode

class States:
    """Состояния для диалогов."""
    MAIN_MENU = 0
    SELECT_PLAN = 1
    SELECT_PERIOD = 2
    PAYMENT_METHOD = 3
    PROCESS_PAYMENT = 4
    ADD_MEDICINE = 10
    ADD_ANALYSIS = 20
    TENANT_SETUP = 30
    BRANDING_SETUP = 31

class LorBot:
    """Основной класс коммерческого бота."""
    
    def __init__(self):
        self.app = None
        self._setup_handlers()
    
    def _setup_handlers(self):
        """Настройка обработчиков."""
        self.app = Application.builder().token(config.BOT_TOKEN).build()
        
        # Основные команды
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("help", self.help))
        self.app.add_handler(CommandHandler("subscription", self.show_subscription))
        self.app.add_handler(CommandHandler("buy", self.buy_subscription))
        self.app.add_handler(CommandHandler("tenant", self.tenant_panel))
        
        # Платежные обработчики
        self.app.add_handler(PreCheckoutQueryHandler(self.pre_checkout))
        self.app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, self.successful_payment))
        
        # Callback обработчики
        self.app.add_handler(CallbackQueryHandler(self.callback_handler))
        
        # Conversation для подписки
        sub_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.select_plan, pattern="^buy_")],
            states={
                States.SELECT_PLAN: [CallbackQueryHandler(self.select_plan, pattern="^plan_")],
                States.SELECT_PERIOD: [CallbackQueryHandler(self.select_period, pattern="^period_")],
                States.PAYMENT_METHOD: [CallbackQueryHandler(self.payment_method, pattern="^pay_")],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
            name="subscription"
        )
        self.app.add_handler(sub_conv)
        
        # Conversation для настройки арендатора
        tenant_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.tenant_setup, pattern="^setup_tenant$")],
            states={
                States.TENANT_SETUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.tenant_name)],
                States.BRANDING_SETUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.branding_info)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
            name="tenant_setup"
        )
        self.app.add_handler(tenant_conv)
    
    # ============== ОСНОВНЫЕ ОБРАБОТЧИКИ ==============
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start."""
        user = update.effective_user
        tenant_id = context.args[0] if context.args else None
        
        # Определяем арендатора
        if tenant_id and tenant_id.isdigit():
            tenant = await subscriptions.get_tenant(int(tenant_id))
            if tenant:
                context.user_data["tenant_id"] = int(tenant_id)
        
        # Регистрируем пользователя
        await self._register_user(user.id, user, context.user_data.get("tenant_id"))
        
        # Получаем подписку
        sub = await subscriptions.get_user_subscription(user.id)
        plan_code = sub["plan_code"] if sub else "free"
        
        # Форматируем приветствие с учетом бренда
        welcome_text = await branding.format_welcome_message(
            context.user_data.get("tenant_id"),
            user.first_name
        )
        
        # Основное меню
        keyboard = self._get_main_keyboard(plan_code)
        
        await update.message.reply_text(
            welcome_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        
        metrics.requests.labels(handler="start", tenant=str(context.user_data.get("tenant_id"))).inc()
    
    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help."""
        user_id = update.effective_user.id
        sub = await subscriptions.get_user_subscription(user_id)
        
        help_text = """❓ *Помощь*

*Основные команды:*
/start - Главное меню
/subscription - Моя подписка
/buy - Купить подписку
/help - Эта справка

*Как пользоваться ботом:*
1️⃣ Добавьте лекарство через меню
2️⃣ Укажите время приема
3️⃣ Получайте напоминания
4️⃣ Отмечайте приемы

*Премиум-функции:*
• Неограниченное количество лекарств
• Детальная статистика
• Экспорт данных
• Приоритетная поддержка"""
        
        if sub and sub["plan_code"] != "free":
            help_text += f"\n\n✨ Ваш тариф: *{sub['name']}*"
        
        keyboard = [[InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]]
        
        await update.message.reply_text(
            help_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def show_subscription(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показ информации о подписке."""
        user_id = update.effective_user.id
        sub = await subscriptions.get_user_subscription(user_id)
        
        if sub and sub["plan_code"] != "free":
            expires = sub["expires_at"].strftime("%d.%m.%Y") if sub["expires_at"] else "бессрочно"
            text = f"""✨ *Моя подписка*

📊 Тариф: *{sub['name']}*
⏱ Статус: *Активна*
📅 Действует до: *{expires}*

*Доступные функции:*
"""
            features = sub.get("features", {})
            for key, value in features.items():
                text += f"• {key}: {value}\n"
            
            keyboard = [
                [InlineKeyboardButton("📈 Статистика", callback_data="stats")],
                [InlineKeyboardButton("🔄 Продлить", callback_data="renew")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ]
        else:
            text = """🔓 *У вас бесплатная подписка*

*Преимущества Premium:*
• Неограниченное количество лекарств
• Детальная статистика
• Экспорт данных в CSV
• Приоритетная поддержка

Выберите тариф для покупки:"""
            
            keyboard = await self._get_plans_keyboard()
        
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
    
    async def buy_subscription(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начало процесса покупки подписки."""
        text = """🛒 *Выберите тарифный план*

*Premium* - 199₽/мес
• 100 лекарств
• Статистика
• Экспорт данных

*Professional* - 990₽/мес
• 1000 лекарств
• До 50 пациентов
• API доступ
• Приоритетная поддержка

*Enterprise* - 9990₽/мес
• Неограниченно
• White-label
• Интеграция с CRM
• Выделенный менеджер"""
        
        keyboard = await self._get_plans_keyboard()
        
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
    
    async def tenant_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Панель управления для арендаторов (врачей/клиник)."""
        user_id = update.effective_user.id
        
        # Проверяем, является ли пользователь арендатором
        tenant = await subscriptions.get_tenant_by_admin(user_id)
        
        if not tenant:
            # Предлагаем стать арендатором
            text = """🏥 *Станьте партнером!*

Используйте бота со своими пациентами:
• Управляйте назначениями
• Отслеживайте приверженность лечению
• Получайте статистику по пациентам
• Брендирование под вашу клинику

*Тарифы для врачей:*
• Professional - 990₽/мес (до 50 пациентов)
• Enterprise - 9990₽/мес (безлимит)

Начните с настройки вашего кабинета:"""
            
            keyboard = [
                [InlineKeyboardButton("🚀 Настроить кабинет", callback_data="setup_tenant")],
                [InlineKeyboardButton("📊 Сравнить тарифы", callback_data="compare_plans")]
            ]
        else:
            # Панель управления арендатора
            text = f"""🏥 *Кабинет врача*

Клиника: *{tenant['name']}*
ID: `{tenant['id']}`

*Статистика:*
👥 Пациентов: {await self._count_patients(tenant['id'])}
💊 Назначений: {await self._count_prescriptions(tenant['id'])}
📊 Приверженность: {await self._get_adherence(tenant['id'])}%

*Настройки бренда:*
• Название: {tenant['branding'].get('name', 'не задано')}
• Врач: {tenant['branding'].get('doctor_name', 'не задан')}"""
            
            keyboard = [
                [InlineKeyboardButton("👥 Пациенты", callback_data="tenant_patients"),
                 InlineKeyboardButton("📊 Статистика", callback_data="tenant_stats")],
                [InlineKeyboardButton("🎨 Брендирование", callback_data="tenant_branding"),
                 InlineKeyboardButton("🔑 API ключи", callback_data="tenant_api")],
                [InlineKeyboardButton("💰 Финансы", callback_data="tenant_finance")]
            ]
        
        keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
        
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
    
    # ============== ПЛАТЕЖНЫЕ ОБРАБОТЧИКИ ==============
    
    async def select_plan(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор тарифного плана."""
        query = update.callback_query
        await query.answer()
        
        data = query.data
        if data.startswith("plan_"):
            plan = data.replace("plan_", "")
            context.user_data["selected_plan"] = plan
            
            keyboard = [
                [InlineKeyboardButton("📅 1 месяц", callback_data="period_monthly"),
                 InlineKeyboardButton("📆 1 год (скидка 20%)", callback_data="period_yearly")],
                [InlineKeyboardButton("🔙 Назад", callback_data="buy")]
            ]
            
            plan_names = {
                "premium": "Premium",
                "professional": "Professional",
                "enterprise": "Enterprise"
            }
            
            await query.edit_message_text(
                f"🛒 *Тариф {plan_names[plan]}*\n\nВыберите период подписки:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
            return States.SELECT_PERIOD
        
        return await self.buy_subscription(update, context)
    
    async def select_period(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор периода подписки."""
        query = update.callback_query
        await query.answer()
        
        period = query.data.replace("period_", "")
        plan = context.user_data.get("selected_plan", "premium")
        context.user_data["selected_period"] = period
        
        keyboard = [
            [InlineKeyboardButton("⭐ Telegram Stars", callback_data="pay_stars"),
             InlineKeyboardButton("💳 Банковская карта", callback_data="pay_card")],
            [InlineKeyboardButton("🔙 Назад", callback_data=f"plan_{plan}")]
        ]
        
        await query.edit_message_text(
            "💳 *Способ оплаты*\n\nВыберите удобный способ:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
        return States.PAYMENT_METHOD
    
    async def payment_method(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка выбора способа оплаты."""
        query = update.callback_query
        await query.answer()
        
        method = query.data.replace("pay_", "")
        user_id = update.effective_user.id
        plan = SubscriptionPlan(context.user_data.get("selected_plan", "premium"))
        period = context.user_data.get("selected_period", "monthly")
        
        if method == "stars":
            # Оплата Telegram Stars
            invoice = await payments.create_telegram_stars_invoice(
                self.app.bot,
                user_id,
                plan,
                period
            )
            
            if invoice:
                await query.edit_message_text(
                    "⭐ *Оплата через Telegram Stars*\n\n"
                    "Нажмите кнопку ниже для оплаты:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⭐ Оплатить", url=invoice)],
                        [InlineKeyboardButton("🔙 Назад", callback_data="payment_method")]
                    ]),
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await query.edit_message_text(
                    "❌ Ошибка создания счета. Попробуйте позже.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", callback_data="payment_method")]
                    ])
                )
        
        elif method == "card":
            # Оплата банковской картой через ЮKassa
            payment = await payments.create_yookassa_payment(
                user_id,
                plan,
                period,
                update.effective_user.username
            )
            
            if payment:
                await query.edit_message_text(
                    "💳 *Оплата банковской картой*\n\n"
                    f"Сумма: {payment['amount']} ₽\n"
                    "Нажмите кнопку для перехода на страницу оплаты:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💳 Оплатить", url=payment["confirmation_url"])],
                        [InlineKeyboardButton("🔙 Назад", callback_data="payment_method")]
                    ]),
                    parse_mode=ParseMode.MARKDOWN
                )
                
                # Логируем создание платежа
                await subscriptions.log_payment(
                    user_id=user_id,
                    tenant_id=context.user_data.get("tenant_id"),
                    amount=payment["amount"],
                    currency="RUB",
                    payment_method="card",
                    payment_system="yookassa",
                    payment_id=payment["id"],
                    status=PaymentStatus.PENDING,
                    metadata={"plan": plan.value, "period": period}
                )
            else:
                await query.edit_message_text(
                    "❌ Ошибка создания платежа. Попробуйте позже.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", callback_data="payment_method")]
                    ])
                )
        
        return ConversationHandler.END
    
    async def pre_checkout(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Проверка перед оплатой (для Telegram Stars)."""
        query = update.pre_checkout_query
        
        # Проверяем корректность данных
        if query.invoice_payload.startswith("subscription_"):
            await query.answer(ok=True)
        else:
            await query.answer(ok=False, error_message="Неверные данные платежа")
    
    async def successful_payment(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка успешной оплаты."""
        payment = update.message.successful_payment
        payload = payment.invoice_payload
        
        if payload.startswith("subscription_"):
            parts = payload.split("_")
            user_id = int(parts[1])
            plan = parts[2]
            period = parts[3]
            
            # Активируем подписку
            await subscriptions.create_subscription(
                user_id=user_id,
                tenant_id=context.user_data.get("tenant_id"),
                plan_code=plan,
                status=SubscriptionStatus.ACTIVE,
                duration_days=30 if period == "monthly" else 365,
                auto_renew=True,
                payment_method="telegram_stars",
                payment_id=payment.provider_payment_charge_id,
                metadata={
                    "telegram_payment_id": payment.telegram_payment_charge_id,
                    "amount": payment.total_amount / 100,
                    "currency": payment.currency
                }
            )
            
            # Логируем платеж
            await subscriptions.log_payment(
                user_id=user_id,
                tenant_id=context.user_data.get("tenant_id"),
                amount=payment.total_amount / 100,
                currency=payment.currency,
                payment_method="telegram_stars",
                payment_system="telegram",
                payment_id=payment.provider_payment_charge_id,
                status=PaymentStatus.SUCCEEDED,
                metadata={"plan": plan, "period": period}
            )
            
            await update.message.reply_text(
                "✅ *Оплата прошла успешно!*\n\n"
                f"Ваш тариф: *{plan.capitalize()}*\n"
                f"Период: *{'30 дней' if period == 'monthly' else '1 год'}*\n\n"
                "Спасибо за использование бота! 🎉",
                parse_mode=ParseMode.MARKDOWN
            )
    
    # ============== НАСТРОЙКА АРЕНДАТОРА ==============
    
    async def tenant_setup(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начало настройки арендатора."""
        query = update.callback_query
        await query.answer()
        
        await query.edit_message_text(
            "🏥 *Настройка кабинета врача*\n\n"
            "Шаг 1/2: Введите название вашей клиники или практики:",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.TENANT_SETUP
    
    async def tenant_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Получение названия клиники."""
        name = update.message.text
        context.user_data["tenant_name"] = name
        
        await update.message.reply_text(
            "✅ Название сохранено\n\n"
            "Шаг 2/2: Расскажите о себе (будет отображаться в приветствии):\n"
            "Например: *Иванов Иван Петрович, врач-терапевт*",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return States.BRANDING_SETUP
    
    async def branding_info(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Сохранение информации о враче и создание арендатора."""
        doctor_info = update.message.text
        user_id = update.effective_user.id
        name = context.user_data["tenant_name"]
        
        # Создаем арендатора
        tenant_id = await subscriptions.create_tenant(
            name=name,
            admin_id=user_id,
            branding={
                "doctor_name": doctor_info,
                "doctor_title": "врач",
                "welcome_text": f"👋 Здравствуйте! Вас приветствует {doctor_info}"
            }
        )
        
        await update.message.reply_text(
            f"✅ *Кабинет успешно создан!*\n\n"
            f"ID вашего кабинета: `{tenant_id}`\n\n"
            f"Теперь вы можете:\n"
            f"• Приглашать пациентов по ссылке: https://t.me/your_bot?start={tenant_id}\n"
            f"• Настроить брендирование в панели управления\n"
            f"• Подключить API для интеграции с CRM\n\n"
            f"Для начала работы выполните /tenant",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return ConversationHandler.END
    
    # ============== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==============
    
    async def _register_user(self, user_id: int, user, tenant_id: Optional[int]):
        """Регистрация пользователя в системе."""
        try:
            await db.execute("""
                INSERT INTO users (
                    user_id, tenant_id, username, first_name, last_name,
                    last_seen, total_interactions
                ) VALUES ($1, $2, $3, $4, $5, NOW(), 1)
                ON CONFLICT (user_id, tenant_id) DO UPDATE SET
                    last_seen = NOW(),
                    total_interactions = users.total_interactions + 1,
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name
            """,
                user_id,
                tenant_id,
                user.username,
                user.first_name,
                user.last_name
            )
            
            metrics.active_users.labels(tenant=str(tenant_id)).inc()
        except Exception as e:
            print(f"Ошибка регистрации пользователя: {e}")
    
    def _get_main_keyboard(self, plan_code: str) -> list:
        """Получение клавиатуры главного меню с учетом тарифа."""
        keyboard = [
            [InlineKeyboardButton("💊 Добавить лекарство", callback_data="add_medicine")],
            [InlineKeyboardButton("📋 Список лекарств", callback_data="list_medicines")],
            [InlineKeyboardButton("💊 Принять препарат", callback_data="take_unscheduled")],
            [InlineKeyboardButton("🩺 Добавить анализ", callback_data="add_analysis")],
            [InlineKeyboardButton("📋 Список анализов", callback_data="list_analyses")],
            [InlineKeyboardButton("📊 Самочувствие", callback_data="mood")],
            [InlineKeyboardButton("📈 Статистика", callback_data="stats")],
        ]
        
        if plan_code != "free":
            keyboard.append([InlineKeyboardButton("📥 Экспорт данных", callback_data="export")])
        
        keyboard.append([InlineKeyboardButton("👨‍⚕️ О враче", callback_data="about")])
        keyboard.append([InlineKeyboardButton("💳 Моя подписка", callback_data="subscription")])
        
        return keyboard
    
    async def _get_plans_keyboard(self) -> list:
        """Получение клавиатуры с тарифными планами."""
        return [
            [InlineKeyboardButton("⭐ Premium - 199₽/мес", callback_data="plan_premium")],
            [InlineKeyboardButton("🚀 Professional - 990₽/мес", callback_data="plan_professional")],
            [InlineKeyboardButton("🏢 Enterprise - 9990₽/мес", callback_data="plan_enterprise")],
            [InlineKeyboardButton("🏥 Для врачей", callback_data="tenant_panel")],
            [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")]
        ]
    
    async def callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Основной обработчик callback запросов."""
        query = update.callback_query
        await query.answer()
        
        data = query.data
        
        if data == "main_menu":
            await self.start(update, context)
        elif data == "subscription":
            await self.show_subscription(update, context)
        elif data == "buy":
            await self.buy_subscription(update, context)
        elif data == "tenant_panel":
            await self.tenant_panel(update, context)
        elif data == "setup_tenant":
            await self.tenant_setup(update, context)
        elif data == "renew":
            # Продление подписки
            context.user_data["selected_plan"] = "premium"
            await self.select_period(update, context)
    
    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена операции."""
        await update.message.reply_text(
            "❌ Операция отменена",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ])
        )
        return ConversationHandler.END
    
    async def _count_patients(self, tenant_id: int) -> int:
        """Подсчет пациентов арендатора."""
        return await db.fetchval(
            "SELECT COUNT(*) FROM users WHERE tenant_id = $1 AND role = 'patient'",
            tenant_id
        ) or 0
    
    async def _count_prescriptions(self, tenant_id: int) -> int:
        """Подсчет назначений арендатора."""
        return await db.fetchval("""
            SELECT COUNT(*) FROM medicines m
            JOIN users u ON u.user_id = m.user_id
            WHERE u.tenant_id = $1 AND m.status = 'active'
        """, tenant_id) or 0
    
    async def _get_adherence(self, tenant_id: int) -> float:
        """Получение средней приверженности лечению."""
        result = await db.fetchrow("""
            SELECT 
                AVG(CASE 
                    WHEN m.total_taken + m.total_skipped > 0 
                    THEN m.total_taken::float / (m.total_taken + m.total_skipped) * 100 
                    ELSE NULL 
                END) as avg_adherence
            FROM medicines m
            JOIN users u ON u.user_id = m.user_id
            WHERE u.tenant_id = $1 AND m.status = 'active'
        """, tenant_id)
        
        return round(result["avg_adherence"] or 0, 1)
    
    async def run(self):
        """Запуск бота."""
        # Инициализация базы данных
        await db.init_pool()
        await subscriptions.init_tables()
        await UserTables.init_tables()
        
        # Запуск бота
        print(f"\n{'='*60}")
        print(f"🚀 Запуск {config.BOT_NAME} v{config.BOT_VERSION}")
        print(f"{'='*60}")
        print(f"📊 База данных: PostgreSQL на {config.DB_HOST}:{config.DB_PORT}")
        print(f"💾 Кэш: Redis на {config.REDIS_URL}")
        print(f"💳 Платежи: Telegram Stars + ЮKassa")
        print(f"{'='*60}\n")
        
        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling()
        
        print("✅ Бот запущен и готов к работе!")
        print("⏎ Нажмите Ctrl+C для остановки\n")
        
        # Ждем сигнала остановки
        stop_signal = asyncio.Future()
        
        def signal_handler():
            stop_signal.set_result(None)
        
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)
        
        try:
            await stop_signal
        except KeyboardInterrupt:
            pass
        finally:
            print("\n🛑 Останавливаем бота...")
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()
            await db.close()
            await redis_cache.close()
            print("✅ Бот остановлен")

# ============== ЗАПУСК ==============

if __name__ == "__main__":
    bot = LorBot()
    asyncio.run(bot.run())
