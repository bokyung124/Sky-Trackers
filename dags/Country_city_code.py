from airflow import DAG
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task

from datetime import datetime

import logging


def get_Snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_dev_db')
    return hook.get_conn().cursor()


@task
def extract(path):
    logging.info(datetime.utcnow())
    with open(path, "r") as f:
        text = f.read()
    return text


@task
def transform(text):
    lines = text.strip().split("\n")[1:]
    records = []
    for l in lines:
        country_code, city_name, city_code, country_name, _, weather_city, country_kor = l.split(",")
        records.append([country_code, city_code, country_name, city_name, weather_city, country_kor])
    logging.info("Transform ended")
    return records


@task
def load(schema, table, records):
    logging.info("load started")    
    cur = get_Snowflake_connection()   
    """
    records = [
      [ "AT", "VIE", "Austria", "Vienna", "Vienna", "오스트리아"],
      [ "CN", "CSX", "China", "Changsha", "Changsha", "중국"],
      ...
    ]
    """

    try:
        cur.execute(f"DELETE FROM {schema}.{table};") 
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        for r in records:
            country_code, city_code, country_name, city_name, weather_city, country_kor = r
            sql = f"""INSERT INTO {schema}.{table} 
                      VALUES ('{country_code}', 
                              '{city_code}', 
                              '{country_name}', 
                              '{city_name}', 
                              '{weather_city}', 
                              '{country_kor}');"""
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;") 
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise   
    logging.info("load done")


with DAG(
    dag_id='country_city_code',
    start_date=datetime(2022, 10, 6),  # 날짜가 미래인 경우 실행이 안됨
    schedule = '@once',  # 적당히 조절
    catchup=False,
) as dag:

    path = Variable.get("country_city_code_path")
    schema = 'raw_data'   ## 자신의 스키마로 변경
    table = 'country_city_code'

    lines = transform(extract(path))
    load(schema, table, lines)