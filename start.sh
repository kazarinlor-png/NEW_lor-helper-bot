cat > start.sh << 'EOF'
#!/bin/bash

# Цвета для красивого вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}🚀 Запуск ЛОР-Помощника...${NC}"

# Активация виртуального окружения
source venv/bin/activate

# Загрузка токена
export BOT_TOKEN=7374353352:AAHuRJALtv1JwcTdi2VMlziCM25KiLTwb80
export ADMIN_IDS=123456789

# Инициализация базы данных (если нужно)
python -c "
import asyncio
from bot import db_manager, Base
async def init():
    async with db_manager._engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
asyncio.run(init())
print('✅ База данных готова')
" 2>/dev/null

# Запуск бота
echo -e "${GREEN}✅ Запуск бота...${NC}"
python bot.py
EOF

chmod +x start.sh
