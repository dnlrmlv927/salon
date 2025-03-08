import logging
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from pathlib import Path
import psycopg2
import pymongo
import json
from airflow.models import Variable
from psycopg2 import sql
from bson import ObjectId
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("dag_etl_beauty_salon")

# Конфигурация подключения
POSTGRES_CONFIG = {
    'host': Variable.get('postgres_host'),
    'database': Variable.get('postgres_database'),
    'user': Variable.get('postgres_user'),
    'password': Variable.get('postgres_password'),
    'port': Variable.get('postgres_port')
}

MONGO_URI = Variable.get('MONGO_URI')
MONGO_DB = Variable.get('MONGO_DB')

# MONGO_URI = "mongodb://root:example@salon-mongodb-1:27017/"
# MONGO_DB = "salon_row"


# Универсальная функция для выполнения SQL-запроса
def load_to_db(conn_info, file_path, table_name):
    try:
        file_path = Path(file_path)
        query = file_path.read_text()

        with psycopg2.connect(**conn_info) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()

        logger.info(f"Данные успешно загружены в {table_name}.")
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в {table_name}: {e}")
        raise


# Функции для загрузки данных из MongoDB в STG
def remove_cycles(obj, seen=None):
    """ Рекурсивно очищает циклы в JSON """
    if seen is None:
        seen = set()

    if isinstance(obj, dict):
        obj_id = id(obj)
        if obj_id in seen:  # Найден цикл, убираем его
            return None
        seen.add(obj_id)

        return {k: remove_cycles(v, seen) for k, v in obj.items()}

    elif isinstance(obj, list):
        return [remove_cycles(v, seen) for v in obj]

    elif isinstance(obj, (ObjectId, datetime)):
        return str(obj)  # Преобразуем ObjectId и даты в строку

    elif obj != obj:  # Проверка на NaN (NaN != NaN)
        return None

    return obj

def get_last_batch_date(workflow_key):
    try:
        conn = psycopg2.connect(dbname=POSTGRES_CONFIG['database'], user=POSTGRES_CONFIG['user'], password=POSTGRES_CONFIG['password'], host=POSTGRES_CONFIG['host'], port=POSTGRES_CONFIG['port'])
        cursor = conn.cursor()
        cursor.execute("""
            SELECT last_batch_date FROM staging_beauty_salon.srv_wf_settings WHERE workflow_key = %s
        """, (workflow_key,))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Ошибка при получении last_batch_date: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

def get_records_from_collection(collection_name, date_field, last_batch_date):
    try:
        mongo_client = pymongo.MongoClient(MONGO_URI)
        mongo_db = mongo_client[MONGO_DB]
        query = {date_field: {"$gt": last_batch_date}} if last_batch_date else {}
        return list(mongo_db[collection_name].find(query).sort(date_field, -1))
    except Exception as e:
        logger.error(f"Ошибка при запросе к MongoDB ({collection_name}): {e}")
        return []

def save_to_postgresql(docs, table_name, date_field, workflow_key):
    if not docs:
        logger.warning(f"Нет новых данных для {table_name}.")
        return
    try:
        conn = psycopg2.connect(dbname=POSTGRES_CONFIG['database'], user=POSTGRES_CONFIG['user'], password=POSTGRES_CONFIG['password'], host=POSTGRES_CONFIG['host'], port=POSTGRES_CONFIG['port'])
        cursor = conn.cursor()
        last_batch_date = docs[0].get(date_field)
        for doc in docs:
            cleaned_doc = remove_cycles(doc)
            object_value = json.dumps(cleaned_doc)
            update_ts = cleaned_doc.get(date_field)
            if update_ts:
                query = sql.SQL("""
                    INSERT INTO staging_beauty_salon.{} (id, object_value, update_ts) 
                    VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING;
                """).format(sql.Identifier(table_name))
                cursor.execute(query, (str(doc.get("_id")), object_value, update_ts))
        if last_batch_date:
            cursor.execute("""
                INSERT INTO staging_beauty_salon.srv_wf_settings (workflow_key, last_batch_date) 
                VALUES (%s, %s) ON CONFLICT (workflow_key) DO UPDATE SET last_batch_date = EXCLUDED.last_batch_date;
            """, (workflow_key, last_batch_date))
        conn.commit()
        logger.info(f"Данные сохранены в PostgreSQL: {table_name}.")
    except Exception as e:
        logger.error(f"Ошибка при сохранении в PostgreSQL: {e}")
    finally:
        cursor.close()
        conn.close()

def process_collection(collection_name, date_field, workflow_key):
    last_batch_date = get_last_batch_date(workflow_key)
    docs = get_records_from_collection(collection_name, date_field, last_batch_date)
    save_to_postgresql(docs, collection_name, date_field, workflow_key)


# Конфигурация DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 2, 17, tz="UTC"),
    'retries': 3,
    'retry_delay': pendulum.duration(minutes=5),
}

with DAG(
    dag_id='ETL_SALON',
    default_args=default_args,
    description='ETL для салона красоты',
    schedule_interval="@daily",
    catchup=False,
) as dag:

    sql_folder_path = Path("/opt/airflow/sql")

    # 1. Группа задач для загрузки из MongoDB в STG
    collections = [
        ("ads", "created_at", "wf_ads"),
        ("leads", "lead_created_at", "wf_leads"),
        ("purchases", "purchase_created_at", "wf_purchases")
    ]

    with TaskGroup("stg_tasks") as stg_tasks:
        stg_load_tasks = [
            PythonOperator(
                task_id=f'load_stg_{collection}',
                python_callable=process_collection,
                op_args=[collection, date_field, workflow_key]
            ) for collection, date_field, workflow_key in collections
        ]

    # 2. Группа задач для переноса из STG в DDS
    dds_tables = ["ads", "leads", "purchases"]

    with TaskGroup("dds_tasks") as dds_tasks:
        dds_load_tasks = [
            PythonOperator(
                task_id=f'transfer_{table}',
                python_callable=load_to_db,
                op_kwargs={
                    'conn_info': POSTGRES_CONFIG,
                    'file_path': sql_folder_path / f"transfer_{table}.sql",
                    'table_name': table
                }
            ) for table in dds_tables
        ]

    # 3. Финальная задача для наполнения витрины
    transfer_mart_task = PythonOperator(
        task_id='transfer_mart_sales',
        python_callable=load_to_db,
        op_kwargs={
            'conn_info': POSTGRES_CONFIG,
            'file_path': sql_folder_path / "transfer_mart_sales.sql",
            'table_name': "mart_sales"
        }
    )


    stg_tasks >> dds_tasks >> transfer_mart_task

