# 🚀 Развертывание ЛОР-Помощника

## Системные требования

- **OS**: Ubuntu 20.04+ / Debian 11+ / CentOS 8+
- **CPU**: 2+ ядра
- **RAM**: 4+ GB
- **Disk**: 20+ GB SSD
- **Docker**: 20.10+
- **Docker Compose**: 2.0+

## Быстрый старт

### 1. Установка Docker

```bash
# Ubuntu/Debian
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER
newgrp docker

# Установка Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
