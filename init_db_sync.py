from bot import Base
from sqlalchemy import create_engine
import os

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite+aiosqlite:///lor_reminder.db")
# Преобразуем асинхронный URL в синхронный
SYNC_URL = DATABASE_URL.replace("sqlite+aiosqlite://", "sqlite://")

print(f"🔄 Создание таблиц в {SYNC_URL}...")
engine = create_engine(SYNC_URL)
Base.metadata.create_all(engine)
print("✅ Таблицы успешно созданы!")

# Проверяем созданные таблицы
from sqlalchemy import inspect
inspector = inspect(engine)
tables = inspector.get_table_names()
print(f"📊 Созданные таблицы: {', '.join(tables)}")
