from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import requests
import pandas as pd
import logging

# snowflake 연결
def get_Snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_dev_db')
    return hook.get_conn().cursor()

# 환율 정보 가져오기
@task
def bring_exchange(api_key):
    url = f"https://www.koreaexim.go.kr/site/program/financial/exchangeJSON?authkey={api_key}&data=AP01"
    response = requests.get(url)
    data = response.json()
    
    return data

# 환율 정보 전처리하기
@task
def transform_exchange(data):
    # 필요한 정보 불러오기
    trans_df = pd.DataFrame(data)[["deal_bas_r", "cur_nm"]]
    trans_df.columns = ['exchange_rate', 'cur_nm']
    trans_df['currency_unit'] = trans_df['cur_nm'].str.split().str[1]
    trans_df['country'] = trans_df['cur_nm'].str.split().str[0]
    trans_df['exchange_rate'] = trans_df['exchange_rate'].str.replace(',', '')   # 쉼표가 있어서 여러개의 문자로 인식됨
    del trans_df['cur_nm']

    # 변환 필요한 나라
    change_country = {'사우디': ['사우디아라비아'],  '위안화' : ['중국'], '덴마아크' : ['덴마크'], '말레이지아' : ['말레이시아'],
                      '유로': ['네덜란드', '스페인', '독일', '아일랜드', '포르투갈', '이탈리아', '프랑스', '오스트리아', '룩셈부르크']}
    
    # 변환이 필요한 나라들 변환
    delete_index = []
    for exchange_rate, currency_unit, country  in trans_df.values:
        if country in change_country.keys():
            country_list = change_country[country]
            for country_name in country_list:
                currency_unit = country if pd.isna(currency_unit) else currency_unit
                trans_df.loc[len(trans_df)] = [exchange_rate, currency_unit, country_name]
            
            delete_index.append(trans_df[trans_df['country']==country].index[0])

    logging.info("진짜로 끝?")
    # 변환 후 필요없는 행 삭제
    trans_df.drop(delete_index, axis=0, inplace=True)
    logging.info("진짜로 끝!")
        
    # dataframe, numpy 형태 모두 안됨. 무조건 list 형태로 반환
    return trans_df.values.tolist()
    
# 테이블 생성 함수
def _create_table(cur, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {table};")
    cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        exchange_date	date	NOT NULL    DEFAULT CURRENT_DATE()	COMMENT '환율 데이터 불러오는 날짜',
                        country	string	NOT NULL,
                        exchange_rate	float	NOT NULL	COMMENT '매매 기준율',
                        currency_unit	string	NOT NULL,
                        created_date	datetime	NOT NULL	DEFAULT CURRENT_DATE()
                    );""")

# DB 에 insert
@task
def insert_data(df, table):    
    cur = get_Snowflake_connection()
    
    try:
        cur.execute("BEGIN;")
        # 원본 테이블이 없으면 생성 - 테이블이 처음 한번 만들어질 때 필요한 코드
        _create_table(cur, table, False)

        # 임시 테이블로 원본 테이블을 복사
        cur.execute(f"CREATE TEMP TABLE t AS SELECT * FROM {table};")
        # 새로운 데이터 임시 테이블에 삽입
        now = datetime.now()
        for exchange_rate, currency_unit, country in df:
            sql = f"INSERT INTO t VALUES ('{now.date()}', '{country}', {exchange_rate}, '{currency_unit}', '{now}');"
            logging.info(sql)
            cur.execute(sql)
            
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;") 
        raise   
    logging.info("TEMP done")

    try:
        cur.execute("BEGIN;")
        # 원본 테이블 생성
        _create_table(cur, table, True)
        
        # 기존 테이블 대체
        delete_sql = f"DELETE FROM {table};"
        cur.execute(delete_sql)
        
        alter_sql = f"""INSERT INTO {table}
        SELECT exchange_date, country, exchange_rate, currency_unit, created_date FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY exchange_date, country ORDER BY created_date DESC) seq
            FROM t
        )
        WHERE seq = 1;"""
        
        logging.info(alter_sql)
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
        
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;") 
        raise    
    logging.info("INSERT done")
    
# 다 실행 후 analytics join
trigger_analytics = TriggerDagRunOperator(
    task_id = "trigger_analytics",
    trigger_dag_id="Exchange_to_Analytics",
    execution_date='{{ds}}',
    reset_dag_run=True
)
    
    
with DAG(
    dag_id = 'Exchange_to_Snowflake',
    start_date = datetime(2024, 1, 8),
    catchup=False,
    tags=['API', 'EXCHANGE'],
    schedule = '@daily',
    default_args={
        "retries": 3,
        "retry_delay": 60*3,
    }
) as dag:

    data = bring_exchange(Variable.get("exchange_api_key"))
    df = transform_exchange(data)
    
    insert_data(df, "EXCHANGE_RATE") >> trigger_analytics