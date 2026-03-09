#!/bin/bash
if pgrep -f "python.*bot.py" > /dev/null; then
    echo "✅ Бот запущен (PID: $(pgrep -f
