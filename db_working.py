import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import Error
from psycopg2.extras import execute_batch
from werkzeug.utils import secure_filename
import json
from datetime import datetime, timezone
import psycopg2.extensions
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

def init_analytics():
    conn = None
    try:
        conn = create_connection()
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS file_analytics (
            id SERIAL PRIMARY KEY,
            filename TEXT NOT NULL UNIQUE,
            uploaded_at TIMESTAMP NOT NULL,
            analytics JSONB NOT NULL
        );
        """)        
        conn.commit()
        cursor.close()
        conn.close()
        return  f'Создана таблица {'file_analytics'}. '
    except Exception as e:
        print(f'Ошибка создания таблицы {e}')
    return 
    


def create_connection():
    try:
        conn = psycopg2.connect(database="esoft", host="127.0.0.1", port="", user="postgres", password=111)
        return conn
    except Exception as e:
        print(f"Ошибка при подключении к БД: {e}")    


def create_table(table_name,columns,df):
    conn = None
    try:
        conn = create_connection()
        cursor = conn.cursor()
        #init_analytics(conn, cursor)  
        cols = ', '.join([f'"{col}" TEXT' for col in columns])
        query = f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
        id SERIAL PRIMARY KEY,
        {cols}
        );
        """
        cursor.execute(query)
        
        print(f'Создана таблица {table_name}. ')
        insert_data_in_table(cursor, table_name, df)
        conn.commit()
        cursor.close()
        conn.close()

        #save_results(table_name,analytics)

    except Exception as e:
        print(f'Ошибка записи в базу данных {e}')
    return 
    
def insert_data_in_table(cursor, table_name: str, data):
    if data.empty:
        return 0

    cols = ', '.join([f'"{col}"' for col in data.columns])
    placeholders = ', '.join(['%s'] * len(data.columns))
    query = f'INSERT INTO "{table_name}" ({cols}) VALUES ({placeholders})'

    rows = [tuple(row) for row in data.itertuples(index=False, name=None)]
    cursor.executemany(query, rows)
    return f' Добавлено {len(rows)} строк'

def delete_table(table_name):
    conn = None
    try:
        conn = create_connection()
        cursor = conn.cursor()

        query = f"""
            DROP TABLE IF EXISTS "{table_name}"
            """
        cursor.execute(query)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f'Ошибка удаления из базы данных {e}')
        return f'error : {e}'
    return 'ok'


def delete_data_from_analytics(filename):
    conn = None
    try:
        conn = create_connection()
        cursor = conn.cursor()
        
        query = f"""
            DELETE FROM file_analytics
            WHERE filename = '{filename}'
            """
        print(f' query: {query}')
        cursor.execute(query)
        conn.commit()
        
        return 'ok'    
    except Exception as e:
        print(f'Ошибка удаления из базы данных {e}')
        return f'error : {e}'
    finally:
        if cursor:
            cursor.close()
            conn.close()

    


def extract_data(table_name):
    conn = None
    try:    
        conn = create_connection()
        cursor = conn.cursor()
        cursor.execute("""
        SELECT analytics FROM file_analytics
        WHERE filename = %s
        """, (table_name,) )
        row = cursor.fetchone()
        return row[0] if row else None
    except Exception as e:
        print(f' Не удалось извлечь данные: {e} ')
    finally:
        if cursor:
            cursor.close()
            conn.close()


def save_results(filename, analytics): 
    print("Тип analytics:", type(analytics))
    print("Пример значения:", analytics.get('mean'))   
    conn = None    
    try:
        init_analytics() 
        conn = create_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
        INSERT INTO file_analytics (filename, uploaded_at, analytics)
            VALUES (%s, %s, %s)
            ON CONFLICT (filename) DO UPDATE
            SET uploaded_at = EXCLUDED.uploaded_at,
                analytics = EXCLUDED.analytics;
        """, (filename, datetime.now(timezone.utc), analytics))
        conn.commit()
        return 'ok'    
    except Exception as e:
        print(f'Ошибка сохранения в базу данных {e}')
        return f'error : {e}'
    finally:
        if cursor:
            cursor.close()
            conn.close()


