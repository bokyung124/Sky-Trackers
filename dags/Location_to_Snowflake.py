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
        country, city, lat, lon = l.split(",")
        records.append([country, city, lat, lon])
    logging.info("Transform ended")
    return records


@task
def load(schema, table, records):
    logging.info("load started")    
    cur = get_Snowflake_connection()   
    """
    records = [
      [ "TH", "Bangkok", "13.87719", "100.71991"],
      [ "SG", "Singapore", "1.28967", "103.850067" ],
      ...
    ]
    """

    try:
        cur.execute(f"DELETE FROM {schema}.{table};") 
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        for r in records:
            country, city, lat, lon = r
            sql = f"INSERT INTO {schema}.{table} VALUES ('{country}', '{city}', '{lat}', '{lon}');"
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;") 
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise   
    logging.info("load done")


with DAG(
    dag_id='lat_lon_to_snowflack',
    start_date=datetime(2022, 10, 6),  # 날짜가 미래인 경우 실행이 안됨
    schedule = '@once',  # 적당히 조절
    catchup=False,
) as dag:

    path = Variable.get("lat_lon_info_path")
    schema = 'raw_data'   ## 자신의 스키마로 변경
    table = 'lat_lon_info'

    lines = transform(extract(path))
    load(schema, table, lines)