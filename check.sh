 cat > check.sh << 'EOF'
#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "🔍 Проверка установки ЛОР-Помощника..."
echo "========================================"

# Проверка Python
echo -n "Python: "
python3 --version

# Проверка виртуального окружения
if [ -d "venv" ]; then
    echo -e "${GREEN}✅ Виртуальное окружение: найдено${NC}"
    source venv/bin/activate
    
    # Проверка основных библиотек
    for lib in pytz telegram sqlalchemy redis; do
        python -c "import $lib" 2>/dev/null
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✅ $lib: установлен${NC}"
        else
            echo -e "${RED}❌ $lib: НЕ установлен${NC}"
        fi
    done
else
    echo -e "${RED}❌ Виртуальное окружение: НЕ найдено${NC}"
fi

# Проверка файла .env
if [ -f ".env" ]; then
    echo -e "${GREEN}✅ Файл .env: найден${NC}"
    grep -q "BOT_TOKEN" .env && echo -e "${GREEN}✅ BOT_TOKEN: настроен${NC}" || echo -e "${RED}❌ BOT_TOKEN: не настроен${NC}"
else
    echo -e "${RED}❌ Файл .env: НЕ найден${NC}"
fi

echo "========================================"
EOF

chmod +x check.sh
