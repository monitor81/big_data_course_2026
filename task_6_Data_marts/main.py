"""
 Требуется создать отдельную схему dmr  (Data Mart Repository) для аналитических данных и 
 разместить в ней витрину analytics_student_performancee.

 Требования:
- Создать схему dmr если она не существует
- Создать витрину dmr.analytics_student_performance с агрегированными данными.
- Реализация через функции

Структура витрины: 
Поле	- Тип данных	- Описание
student_id	- INTEGER	- ID студента
course_id -	INTEGER	ID - курса
department_id -	INTEGER	- Код кафедры
department_name	 - VARCHAR - Название кафедры
education_level	- VARCHAR	- Уровень образования
education_base - VARCHAR -	Основа обучения
semester	- INTEGER	- Номер семестра
course_year	- INTEGER	- Курс обучения
final_grade -	INTEGER -	Итоговая оценка
total_events -	INTEGER	- Всего событий за семестр
avg_weekly_events	- DECIMAL(10,2)	- Среднее событий в неделю
total_course_views	- INTEGER	- Всего просмотров курса
total_quiz_views	- INTEGER	- Всего просмотров тестов
total_module_views -	INTEGER - Всего просмотров модулей
total_submissions	- INTEGER	- Всего отправленных заданий
peak_activity_week	- INTEGER	- Неделя с максимальной активностью
consistency_score	- DECIMAL(5,2)	- Коэффициент стабильности активности (0-1)
activity_category	- VARCHAR	- Категория активности (низкая/средняя/высокая)
last_update	- TIMESTAMP	- Дата обновления записи

"""


# Ниже представлен пример реализации витрины dmr.analytics_student
# Поле	- Тип данных	- Описание
# student_id	- INTEGER	- ID студента
# course_id -	INTEGER	ID - курса
# department_id -	INTEGER	- Код кафедры
# semester	- INTEGER	- Номер семестра
# course_year	- INTEGER	- Курс обучения
# final_grade -	INTEGER -	Итоговая оценка
# last_update	- TIMESTAMP	- Дата обновления записи

import os
import sys
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# Загружаем переменные из .env файла (если он есть)
load_dotenv()

# получение параметров подключения
def get_db_config():
    """
    Формирует словарь с параметрами подключения к БД.    
    """
    load_dotenv()
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB', 'educational_portal'),
        'user': os.getenv('USER', 'postgres'),
        'password': os.getenv('PASSWORD', '')
    }  
    print (config)
    return config

# подключение к БД
def get_connection():
    """Устанавливает и возвращает соединение с БД."""
    try:
        config = get_db_config()
        conn = psycopg2.connect(**config)
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        sys.exit(1)

# создание нового слоя в БД (схема dmr)
def create_schema(conn):
    """Создаёт схему dmr, если она ещё не существует."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dmr;")
        conn.commit()
        print("Схема dmr успешно создана (или уже существовала).")

# создание таблицы для витрины
def create_table(conn):
    """Создаёт таблицу dmr.analytics_student с заданной структурой."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dmr.analytics_student (
        student_id     INTEGER NOT NULL,
        course_id      INTEGER NOT NULL,
        department_id  INTEGER,
        semester       INTEGER,
        course_year    INTEGER,
        final_grade    INTEGER CHECK (final_grade IN (2,3,4,5)),
        last_update    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (student_id, course_id)
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
        print("Таблица dmr.analytics_student успешно создана.")

# заполнение таблицы dmr.analytics_student
def insert_data(conn):
    """
    Заполняет витрину данными из public.user_logs.
    """
    select_query = """
    WITH student_final AS (
        SELECT 
            userid,
            courseid,
            MAX(depart) AS department_id,
            MAX(num_sem) AS semester,
            MAX(kurs) AS course_year,
            MAX(CAST(namer_level AS INTEGER)) AS final_grade
        FROM public.user_logs
        WHERE namer_level IS NOT NULL
        GROUP BY userid, courseid
    )
    SELECT 
        userid,
        courseid,
        department_id,
        semester,
        course_year,
        final_grade
    FROM student_final
    WHERE final_grade IN (2,3,4,5);
    """

    insert_query = sql.SQL("""
        INSERT INTO dmr.analytics_student 
        (student_id, course_id, department_id, semester, course_year, final_grade)
        VALUES %s
        ON CONFLICT (student_id, course_id) 
        DO UPDATE SET
            department_id = EXCLUDED.department_id,
            semester      = EXCLUDED.semester,
            course_year   = EXCLUDED.course_year,
            final_grade   = EXCLUDED.final_grade,
            last_update   = CURRENT_TIMESTAMP;
    """)

    with conn.cursor() as cur:
        cur.execute(select_query)
        rows = cur.fetchall()
        
        if not rows:
            print("Нет данных для вставки.")
            return
        
        data_tuples = [(row[0], row[1], row[2], row[3], row[4], row[5]) for row in rows]
        execute_values(cur, insert_query, data_tuples, page_size=1000)
        conn.commit()        
        print(f"Витрина заполнена. Добавлено/обновлено записей: {cur.rowcount}")

def main():
    """Последовательное выполнение шагов."""
    conn = None
    try:
        conn = get_connection()
        create_schema(conn)
        create_table(conn)
        insert_data(conn)
        print("\nВсе операции выполнены успешно!")
    except Exception as e:
        print(f"Ошибка в процессе выполнения: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Соединение с БД закрыто.")

if __name__ == "__main__":
    main()