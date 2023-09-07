import os
from getpass import getpass

import findspark
from pyspark.sql import SparkSession 
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

from sqlalchemy.engine.base import Engine
from sqlalchemy import create_engine
import pandas as pd




ENGINE: Engine = None
global PATH, RESULT_PATH, TABLE_NAME, PK_EQUALS # вводятся с консоли


clear = lambda: os.system('clear') # очистка консоли для linux


def log_start(delta_id, table_name):
    """ Логирование. Начало расчёта дельты """
    log_insert = f'''
        INSERT INTO 
            logs.logs_deltas (delta_id, start_date, end_date, table_name)
        VALUES ({delta_id}, current_timestamp, null, '{table_name}');
    '''
    log_get_id = f'''
        SELECT log_id
        FROM logs.logs_deltas 
        ORDER BY log_id DESC 
        LIMIT 1;
    '''
    ENGINE.execute(log_insert)
    id = ENGINE.execute(log_get_id).fetchone()[0]

    return id


def log_end(log_id):
    """ Логирование. Конец расчёта дельты """
    log_update_end = f'''
        UPDATE logs.logs_deltas 
        SET end_date = current_timestamp 
        WHERE 
            log_id = {log_id};
    '''
    ENGINE.execute(log_update_end)


def logs_show():
    """ Логирование. Вывод последних 10 логов """
    clear()
    query = f'''
        SELECT
            log_id,
            delta_id,
            start_date,
            end_date,
            table_name
        FROM logs.logs_deltas
        ORDER BY log_id DESC
        LIMIT 10;
    '''
    logs = pd.read_sql(query, ENGINE)
    clear()
    print("Вывод логов")
    print(logs)
    print("Последние 10 записей ...")
    input("Нажмите enter для продолжения ")


def find_logs_delta(delta_id, table_name):
    """ Обработка логов. Поиск законченных расчётов дельты """
    query = f'''
    SELECT 
    	log_id
    FROM logs.logs_deltas
    WHERE 
    	delta_id = {delta_id} AND
    	table_name = '{table_name}' AND
    	end_date is not null
    '''
    logs = ENGINE.execute(query).fetchone()
    if logs and len(logs) > 0:
        return True

    return False


def str_pk_equal_from_list(pk):
    """ Наименование колонок из List в str """
    res_str = ""
    for i, pk_part in enumerate(pk):
        if i != 0:
            res_str += " and "
        res_str += f"old.{pk_part} = new.{pk_part}"
    return res_str


def merge_delta_table_with_main_table(path_delta, path_main_table, pk_equal):
    """ Объединение дельта-таблицы с DeltaTable """
    df_delta = spark.read.format("csv") \
        .options(header=True, delimiter=';') \
        .load(path_delta)

    if os.path.exists(path_main_table):
        main_table = DeltaTable.forPath(spark, path_main_table)
        main_table.alias("old") \
            .merge(df_delta.alias("new"), pk_equal) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        main_table.toDF().write.format("delta") \
            .mode("overwrite") \
            .save(path_main_table)
    else:
        df_delta.write.format("delta").save(path_main_table)


def delta_res_to_csv(delta_res_tmp_folder_path, delta_res_csv):
    """ Выгрузка данных из DeltaTable в итоговый csv """
    df = DeltaTable.forPath(spark, delta_res_tmp_folder_path).toDF()
    df.write.format("csv") \
        .mode("overwrite") \
        .options(header=True, delimiter=';') \
        .save(delta_res_csv)
    

def exe_deltas_merge():
    """ Расчёт дельта-таблиц """
    clear()
    # получение списка всех дельт
    delta_folder_names = os.listdir(PATH)
    delta_folder_names.sort()

    #пути для сохранения файлов
    delta_res_tmp_folder_path = f"{RESULT_PATH}/deltas_tmp_{TABLE_NAME}"
    delta_res_csv = f"{RESULT_PATH}/mirr_{TABLE_NAME}"
    
    for delta_folder in delta_folder_names:
        if not find_logs_delta(delta_folder, TABLE_NAME):
            clear()
            print("Запись лога о начале")
            log_id = log_start(delta_folder, TABLE_NAME)

            print(f"Расчёт дельта-таблиц {delta_folder} для {TABLE_NAME}")
            merge_delta_table_with_main_table(
                f"{PATH}/{delta_folder}",
                delta_res_tmp_folder_path, 
                PK_EQUALS
            )
            print("Сохранение изменений в итоговый csv файл")
            delta_res_to_csv(delta_res_tmp_folder_path, delta_res_csv)
        
            print("Запись лога об успешном расчёте")
            log_end(log_id)

    input("Нажмите enter для продолжения ")



def input_main_vals():
    """ Ввод с консоли пути, названия таблиц и PK """
    global PATH, RESULT_PATH, TABLE_NAME, PK_EQUALS
    clear()
    print("Ввод основных параметров")
    PATH = str(input("Введите путь к основной директории >>> "))
    RESULT_PATH = str(input("Введите путь к директории с итоговым файлом >>> "))

    TABLE_NAME = str(input("Введите название таблицы >>> "))
    
    list_pk_parts = [
        str(i) for i in input('Введите PK через запятую, без пробелов >>> ').split(",")]
    PK_EQUALS = str_pk_equal_from_list(list_pk_parts)
    input("Нажмите enter для продолжения ")



def input_db_con_str():
    """ Ввод параметров для подлючения к БД (для логов) """
    global ENGINE, ENGINE_STR

    clear()
    print("Подключение к базе данных, необходимо для логирования")
    host_port = str(input("Введите хост и порт, host:port >>> "))
    db_name = str(input("Введите название базы данных >>> "))
    login = str(input("Введите логин >>> "))
    password = str(getpass("Введите пароль >>> "))
    
    ENGINE_STR = f"postgresql+psycopg2://{login}:{password}@{host_port}/{db_name}"
    ENGINE = create_engine(ENGINE_STR)
    print("Вы успешно подключены к базе данных")
    input("Нажмите enter для продолжения ")


def menu():
    """ Меню в терминале """
    actions = {
        "1": logs_show,
        "2": exe_deltas_merge,
        "3": input_main_vals,
        "0": exit
    }
    while True:
        clear()
        print( "Меню:\n" +
            "1. Просмотр логов (последние 10)\n" +
            "2. Расчёт дельта-таблиц\n" +
            "3. Обновление сведений о пути, дельтах\n" +
            "0. Выход"
        )
        action = str(input("Необходимо выбрать пункт меню >>> "))
        actions[action]()


def main():
    input_main_vals()
    input_db_con_str()
    menu()


if __name__ == '__main__':
    findspark.init('/opt/spark-3.4.1-bin-hadoop3')

    builder = SparkSession \
        .builder \
        .appName('pyspark_neoflex_project_2_2') \
        .enableHiveSupport() \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

main()