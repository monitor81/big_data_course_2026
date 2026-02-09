#!/bin/bash
set -e

# Выполняем SQL команды, используя psql
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Создаем таблицу для логов
    CREATE TABLE user_logs (
        courseid INTEGER,
        userid INTEGER,
        num_week INTEGER,
        s_all INTEGER,
        s_all_avg VARCHAR(255),
        s_course_viewed INTEGER,
        s_course_viewed_avg VARCHAR(255),
        s_q_attempt_viewed INTEGER,
        s_q_attempt_viewed_avg VARCHAR(255),
        s_a_course_module_viewed INTEGER,
        s_a_course_module_viewed_avg VARCHAR(255),
        s_a_submission_status_viewed INTEGER,
        s_a_submission_status_viewed_avg VARCHAR(255),
        NameR_Level VARCHAR(255),
        Name_vAtt VARCHAR(255),
        Depart VARCHAR(255),
        Name_OsnO VARCHAR(255),
        Name_FormOPril VARCHAR(255),
        LevelEd VARCHAR(255),
        Num_Sem INTEGER,
        Kurs INTEGER,
        Date_vAtt VARCHAR(255)
    );

    -- Копируем данные из локального CSV файла (внутри контейнера) в созданную таблицу
    \copy user_logs FROM '/datasets/aggrigation_logs_per_week.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

EOSQL





