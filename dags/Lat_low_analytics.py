from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime

import logging

def get_Snowflake_connection():
    # autocommit is False by default
    hook = SnowflakeHook(snowflake_conn_id='snowflake_dev_db')
    return hook.get_conn().cursor()

@task
def elt(raw_schema, schema, raw_table, country_city_table, table):
    cur = get_Snowflake_connection()

    # 테이블 생성
    sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} 
                           AS SELECT c.country_code, c.city_code, lat, lon
                              FROM {raw_schema}.{raw_table} as t
                              JOIN {raw_schema}.{country_city_table} as c
                              ON t.country = c.country_code AND t.city = c.weather_city;"""

    logging.info(sql)

    try:
        cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise


with DAG(
    dag_id = 'lat_lon_analytics',
    start_date = datetime(2024,1,8), # 날짜가 미래인 경우 실행이 안됨
    schedule = '@once',  # 적당히 조절
    catchup = False,
) as dag:
    
    raw_schema = "raw_data"
    schema = "analytics"
    country_city_table = "country_city_code"
    raw_table = table = "lat_lon_info"

    elt(raw_schema, schema, raw_table, country_city_table, table)