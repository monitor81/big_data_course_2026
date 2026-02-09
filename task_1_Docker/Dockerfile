# Используем официальный образ PostgreSQL
FROM postgres:13

# Устанавливаем wget для скачивания файла
RUN apt-get update && apt-get install -y wget

# Копируем скрипт инициализации, который будет исполнен при старте
COPY init.sh /docker-entrypoint-initdb.d/
