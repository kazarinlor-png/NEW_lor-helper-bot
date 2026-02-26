cat > update.sh << 'EOF'
#!/bin/bash

echo "🔄 Обновление ЛОР-Помощника..."

# Сохраняем текущую версию
cp bot.py bot.py.backup
cp .env .env.backup 2>/dev/null

# Обновляем код
git pull

# Восстанавливаем .env если нужно
if [ ! -f ".env" ] && [ -f ".env.backup" ]; then
    cp .env.backup .env
fi

# Обновляем зависимости
source venv/bin/activate
pip install --upgrade -r requirements.txt 2>/dev/null

echo "✅ Обновление завершено!"
echo "👉 Если что-то пошло не так, используйте: cp bot.py.backup bot.py"
EOF

chmod +x update.sh
