from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta, timezone

import logging

def get_Snowflake_connection():
    # autocommit is False by default
    hook = SnowflakeHook(snowflake_conn_id='snowflake_dev_db')
    return hook.get_conn().cursor()

@task
def elt(raw_schema, schema, raw_table, country_city_table, table):
    cur = get_Snowflake_connection()

    # 원본 테이블이 없다면 생성
    create_table_sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
                               date date not null,
                               country_code varchar(2) not null,
                               city_code varchar(3) not null,
                               weather_condition varchar(32) not null,
                               max_temp float not null,
                               min_temp float not null,
                               rain_amount float not null
                           );"""

    logging.info(create_table_sql)

    # 임시 테이블 생성
    KST = timezone(timedelta(hours=9))
    today = str(datetime.now(KST))[:10]
    
    create_t_sql = f"""CREATE TEMPORARY TABLE weather_t 
                       AS SELECT weather_date, country, city, weather_condition, max_temp, min_temp, rain_amount, created_date FROM (
                            SELECT *, ROW_NUMBER() OVER (PARTITION BY weather_date, country, city ORDER BY created_date DESC) seq
                            FROM {raw_schema}.{raw_table}
                            )
                          WHERE seq = 1 AND weather_date = '{today}';"""

    logging.info(create_t_sql)
    try:
        cur.execute(create_table_sql)
        cur.execute(create_t_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 기존 테이블 대체
    try:
        cur.execute(f"DELETE FROM {schema}.{table};")
        alter_sql = f"""INSERT INTO {schema}.{table}
                        SELECT weather_date date, c.country_code, c.city_code, weather_condition, max_temp, min_temp, rain_amount
                        FROM weather_t AS t
                        JOIN  {raw_schema}.{country_city_table} AS c
                        ON t.country = c.country_code AND t.city = c.weather_city;"""
        logging.info(alter_sql)
    
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise


with DAG(
    dag_id = 'weather_analytics',
    start_date = datetime(2024,1,8), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 2 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    # default_args = {
    #     'retries': 1,
    #     'retry_delay': timedelta(minutes=3),
    # }
) as dag:
    
    raw_schema = "raw_data"
    schema = "analytics"
    country_city_table = "country_city_code"
    raw_table = table = "weather"

    elt(raw_schema, schema, raw_table, country_city_table, table)