import findspark
findspark.init('/opt/spark-3.4.1-bin-hadoop3')

from pyspark.sql import SparkSession 
from pyspark.sql.functions import lower, col

FILES_SRC_PATH = './src'


'''1. Олимпийские дисциплины по сезонам, генерация df'''
def create_disciplines_df():
    print('Генерация df дисциплин')

    schema = "row_id BIGINT, discipline STRING, season STRING"
    data = [
        (1, 'basketball', 'summer'),
        (2, 'judo', 'summer'),
        (3, 'swimming', 'summer'),
        (4, 'tennis', 'summer'),
        (5, 'fencing', 'summer'),
        (6, 'figure skating', 'winter'),
        (7, 'snowboard', 'winter'),
        (8, 'short track', 'winter'),
        (9, 'luge', 'winter'),
        (10, 'ski race', 'winter'),
    ]

    df_disciplines = spark.createDataFrame(data, schema) 
    df_disciplines \
        .repartition(1) \
        .write \
        .mode("overwrite") \
        .options(header=True, delimiter='	') \
        .csv(f'{FILES_SRC_PATH}/disciplines.scv')
    df_disciplines.show(10)
    
    print('df дисциплин был сохранён')
    
    return df_disciplines


'''2. Считывание данных из Athletes.csv'''
def read_stat_athletes_csv():
    print('Считывание данных из Athletes.csv')
    df_athletes = spark.read.options(header=True, delimiter=';') \
        .csv(f'{FILES_SRC_PATH}/Athletes.csv')

    print('Генерация статистики по дисциплинам')
    df_stat_athletes = df_athletes.groupBy('Discipline').count()
    df_stat_athletes.show(5)

    return df_stat_athletes


'''Сохранение статистики по дисциплинам из Athletes.csv'''
def save_stat_athletes_csv(df_stat_athletes):
    print('Сохранение статистики по дисциплинам')
    df_stat_athletes \
          .write \
          .mode("overwrite") \
          .options(header=True) \
          .parquet(f'{FILES_SRC_PATH}/stat_athletes.parquet')
    

'''Статистика по дисциплинам с учётом созданного df'''
def stat_athlts_discipl(df_stat_athletes, df_disciplines):
    print('Расчёт статистики с учётом созданного ранее df дисциплин')
    df_disciplines \
        .join(
            df_stat_athletes,
            lower(df_stat_athletes.Discipline) == df_disciplines.discipline,
            'left'
        ) \
        .fillna(value=0, subset='count') \
        .select(
            'row_id', 
            df_disciplines.discipline.alias('discipline'), 
            'season', col('count').alias('count_athletes')
        ) \
        .show(10)

    print('Сохранение результата')
    df_stat_athletes \
          .write \
          .mode("overwrite") \
          .options(header=True) \
          .parquet(f'{FILES_SRC_PATH}/result_stat.parquet')
    

def main():
    print('Выполнение задания 1')
    df_disciplines = create_disciplines_df()

    print('Выполнение задания 2')
    df_stat_athletes = read_stat_athletes_csv()
    save_stat_athletes_csv(df_stat_athletes)

    print('Выполнение задания 3')
    stat_athlts_discipl(df_stat_athletes, df_disciplines)


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('pyspark_vstu_project_2_1') \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    main()