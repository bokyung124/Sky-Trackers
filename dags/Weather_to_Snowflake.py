from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta

import requests
import logging
import json

def get_Snowflake_connection():
    # autocommit is False by default
    hook = SnowflakeHook(snowflake_conn_id='snowflake_dev_db')
    return hook.get_conn().cursor()

def get_lat_lon_info(schema, table):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_dev_db')
    sql = f"SELECT * FROM {schema}.{table};"
    return hook.get_pandas_df(sql)

@task  
def extract(schema, table, api_key):
    df = get_lat_lon_info(schema, table)
    data_list = []
    for _, row in df.iterrows():
        country, city, lat, lon = row["COUNTRY"], row["CITY"], row["LAT"], row["LON"]
        url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&units=metric&appid={api_key}"
        response = requests.get(url)
        data = json.loads(response.text)
        data["country"] = country
        data["city"] = city
        data_list.append(data)
    return data_list

@task
def transform(data_list):
    records = []
    for data in data_list:
        country = data["country"]
        city = data["city"]
        for d in data["daily"]:
            weather_date = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
            try:
                weather_condition = d["weather"][0]["main"]
            except:
                weather_condition = ""
            max_temp = d["temp"]["max"]
            min_temp = d["temp"]["min"]
            rain_amount = d.get("rain", 0)
            records.append((weather_date, country, city, weather_condition, max_temp, min_temp, rain_amount))
    return records

@task
def load(schema, table, records):
    cur = get_Snowflake_connection()

    # 임시 테이블 생성
    create_t_sql = f"""CREATE TEMPORARY TABLE weather_t (
        weather_date date not null default current_date(),
        country varchar(2) not null,
        city varchar(32) not null,
        weather_condition varchar(32) not null,
        max_temp float not null,
        min_temp float not null,
        rain_amount float not null default 0,
        created_date timestamp not null default current_timestamp()
        );"""
    insert_t_sql = f"INSERT INTO weather_t SELECT * FROM {schema}.{table}"

    logging.info(create_t_sql)
    logging.info(insert_t_sql)
    try:
        cur.execute(create_t_sql)
        cur.execute(insert_t_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 임시 테이블 데이터 입력
    try:
        for r in records:
            weather_date, country, city, weather_condition, max_temp, min_temp, rain_amount = r
            insert_sql = f"""INSERT INTO weather_t (weather_date, country, city, weather_condition, max_temp, min_temp, rain_amount) 
                            VALUES ('{weather_date}', 
                                    '{country}', 
                                    '{city}', 
                                    '{weather_condition}',
                                    '{max_temp}',
                                    '{min_temp}',
                                    '{rain_amount}');"""
            logging.info(insert_sql)
    
            cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 기존 테이블 대체
    try:
        cur.execute(f"DELETE FROM {schema}.{table};")
        alter_sql = f"""INSERT INTO {schema}.{table}
                        SELECT weather_date, country, city, weather_condition, max_temp, min_temp, rain_amount, created_date FROM (
                                SELECT *, ROW_NUMBER() OVER (PARTITION BY weather_date, country, city ORDER BY created_date DESC) seq
                                FROM weather_t
                                )
                        WHERE seq = 1;"""
        logging.info(alter_sql)
    
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise


with DAG(
    dag_id = 'weather_to_snowflake',
    start_date = datetime(2024,1,8), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 1 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    
    schema = "raw_data"
    lat_lon_table = "lat_lon_info"
    table = "weather"
    api_key = Variable.get("open_weather_api_key")

    records = transform(extract(schema, lat_lon_table, api_key))
    load(schema, table, records)