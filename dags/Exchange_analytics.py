from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import logging

# snowflake 연결
def get_Snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_dev_db')
    return hook.get_conn().cursor()

# raw_data의 환율 테이블, 나라/도시 코드 테이블 JOIN
@task
def join_table(schema, table):
    cur = get_Snowflake_connection()
    
    try:
        cur.execute("BEGIN;")
        # 기존의 analytics 테이블 drop
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        logging.info("Drop Table Done")
        
        # AS SELECT 문법으로 analytics 테이블 생성
        sql = f"""CREATE TABLE {schema}.{table} AS 
                    SELECT 
                        EXCHANGE_DATE AS DATE,
                        COUNTRY_CODE,
                        EXCHANGE_RATE,
                        CURRENCY_UNIT
                    FROM (SELECT * 
                        FROM EXCHANGE_RATE
                        WHERE EXCHANGE_DATE = (SELECT MAX(EXCHANGE_DATE) FROM exchange_rate)) A
                    JOIN (SELECT DISTINCT COUNTRY_CODE, COUNTRY_KOR
                        FROM COUNTRY_CITY_CODE) B
                    ON A.country = B.COUNTRY_KOR;"""
        cur.execute(sql)
        cur.execute("COMMIT;")
        logging.info("Create Table Done")
        
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;") 
        raise   


with DAG(
    dag_id = 'Exchange_to_Analytics',
    start_date = datetime(2024, 1, 8),
    catchup=False,
    tags=['API', 'EXCHANGE']
    # schedule = '@daily'
) as dag:

    join_table('ANALYTICS', 'EXCHANGE_ANALYTICS')