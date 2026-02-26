cat > fix_markdown.py << 'EOF'
#!/usr/bin/env python3
"""
Скрипт для исправления ошибок Markdown в боте
"""
import re

def fix_markdown_errors(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Исправляем проблемы с Markdown в about_callback
    pattern = r'(await query\.edit_message_text\(.*?)([\*_])(.*?)([\*_])(.*?\))'
    
    def replace_bold(match):
        # Заменяем * на корректное форматирование
        return match.group(1) + '*' + match.group(3) + '*' + match.group(5)
    
    fixed_content = re.sub(pattern, replace_bold, content, flags=re.DOTALL)
    
    # Сохраняем исправленный файл
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(fixed_content)
    
    print(f"✅ Исправления применены к {filename}")

if __name__ == "__main__":
    fix_markdown_errors("bot.py")
EOF

chmod +x fix_markdown.py
