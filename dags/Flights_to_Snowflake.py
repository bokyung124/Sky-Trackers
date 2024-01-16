from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta, timezone

import requests
import logging
import json
import pandas as pd

def get_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_dev_db')
    conn = hook.get_conn()
    cursor = conn.cursor()
    return cursor

@task
def extract_flight():
    access_key = Variable.get('flight_api_key')
    limit = 100
    offset = 0

    flight_list = []
    fail_count = 0

    while True:
        url = 'http://api.aviationstack.com/v1/flights'
        params ={'access_key' : access_key, 'dep_iata':'ICN', 'offset':offset, 'limit': limit}

        try:
            response = requests.get(url, params=params)
            flights = json.loads(response.text)

            logging.info(response.status_code)

            if response.status_code == '429':
                raise ValueError('rate_limit_reached')
            
            if flights.get('data') is not None:
                for flight in flights.get('data'):
                    arrival_city = flight['arrival']['iata']               # 도착 국가
                    arrival_airport = flight['arrival']['airport']            # 도착 공항
                    arrival_sched_time  = flight['arrival']['scheduled']      # 도착 예정 시간
                    departure_airport = flight['departure']['airport']        # 출발 공항 (인천공항)
                    departure_sched_time = flight['departure']['scheduled']   # 출발 예정 시간
                    flight_iata = flight['flight']['iata']                    # 항공편명

                    flight_dict = {'flight_iata':flight_iata, 'departure_sched_time':departure_sched_time, 'departure_airport':departure_airport, \
                            'arrival_city':arrival_city, 'arrival_airport':arrival_airport, 'arrival_sched_time':arrival_sched_time}
                    flight_list.append(flight_dict)
                
                if len(flights.get('data')) < limit:
                    break
                offset += limit
            else:
                logging.error("flights.get('data') returned None. Moving to next iteration.")
                continue
            fail_count = 0
        except Exception as e:
            logging.info(response.status_code)
            logging.info(response.text)
            logging.error(e)
            fail_count += 1  
            if fail_count > 5:  
                logging.error("API 호출 연속 5번 실패로 중단")
                break
    return flight_list

@task
def get_amadeus_token():
    client_id = Variable.get('price_client_id')
    client_secret = Variable.get('price_client_secret')
    url = "https://test.api.amadeus.com/v1/security/oauth2/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,  
        "client_secret": client_secret 
    }

    response = requests.post(url, headers=headers, data=payload)
    
    if response.status_code == 200:
        token_data = response.json()
        token = token_data['access_token']
        return token
    else:
        raise Exception("Token request failed with status code {}".format(response.status_code))

@task
def extract_price(token, flight_list):
    KST = timezone(timedelta(hours=9))
    today = str(datetime.now(KST))[:10]

    logging.info(f'token: {token}')
    url = "https://test.api.amadeus.com/v2/shopping/flight-offers"

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    price_list = []

    if flight_list is None:
        raise ValueError("flight_list is None")

    arrival_country = [flight_list[i]['arrival_country'] for i in range(0, 10)]
    departure_date = today


    for country in arrival_country:
        logging.info(f'departure: {departure_date}, country: {country}')
        params = {
            'originLocationCode': 'ICN',  
            'destinationLocationCode': country, 
            'departureDate': departure_date, 
            'currencyCode': 'KRW',
            'adults': '1', 
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            
            offers = response.json()
            if not offers or not offers.get('data'):  
                raise ValueError("No offer data")
            
            for offer in offers.get('data'):
                first_flight = offer.get('itineraries')[0]['segments'][0]
                flight_iata = first_flight['carrierCode'] + first_flight['number']
                departure_sched_time = first_flight['departure']['at']
                price = offer['price']['total']
                cabin = offer['travelerPricings'][0]['fareDetailsBySegment'][0]['cabin']

                logging.info(f'departure: {departure_sched_time}, price: {price}')
                price_dict = {'flight_iata':flight_iata, 'departure_sched_time':departure_sched_time, 'price':price, 'cabin':cabin}
                price_list.append(price_dict)
        except Exception as e:
            logging.info(response.status_code)
            logging.info(response.text)
            logging.error(e)
            raise
    return price_list


@task
def load_flight(flight_list, schema, table):
    cur = get_snowflake_conn()
    now = str(datetime.now())[:10]

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        flight_iata	            string	    NOT NULL,
        departure_sched_time	datetime	NOT NULL,
        arrival_city	        string,
        arrival_airport	        string,
        departure_airport	    string	    NOT NULL	DEFAULT 'Seoul (Incheon)',
        arrival_sched_time	    datetime,
        created_date	        TIMESTAMP_NTZ(9)	NOT NULL	DEFAULT CURRENT_DATE(),
        PRIMARY KEY (flight_iata, departure_sched_time)
    );
    """

    # 임시테이블 생성
    create_t_sql = f"""CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};"""

    try:
        cur.execute(create_table_sql)
        logging.info(create_table_sql)
        cur.execute(create_t_sql)
        logging.info(create_t_sql)
    except Exception as e:
        logging.error(e)
        raise

    # 임시테이블 적재
    insert_sql = f"""
    INSERT INTO t 
    VALUES (%(flight_iata)s, %(departure_sched_time)s, %(arrival_city)s, %(arrival_airport)s, %(departure_airport)s, %(arrival_sched_time)s, '{now}');
    """ 
    try:
        cur.execute("BEGIN;")
        for flight in flight_list:
            if flight['flight_iata'] is None or flight['departure_sched_time'] is None or flight['departure_airport'] is None:
                logging.info(f"Skipping flight due to NULL values: {flight}")
                continue
            logging.info(f"Inserting flight: {flight}")
            cur.execute(insert_sql, {
                'flight_iata': flight['flight_iata'], 
                'departure_sched_time': flight['departure_sched_time'], 
                'arrival_city': flight['arrival_city'], 
                'arrival_airport': flight['arrival_airport'], 
                'departure_airport': flight['departure_airport'], 
                'arrival_sched_time': flight['arrival_sched_time']
            })
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(e)
        raise
    
    # 기존 테이블 대체
    alter_sql = f"""
    INSERT INTO {schema}.{table} 
    SELECT DISTINCT flight_iata, departure_sched_time, arrival_city, arrival_airport, departure_airport, arrival_sched_time, created_date FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY flight_iata, departure_sched_time ORDER BY created_date DESC) seq
        FROM t
    )
    WHERE seq = 1;
    """
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};")
        cur.execute(alter_sql)
        logging.info(alter_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(e)
        logging.info(alter_sql)
        raise

@task
def load_price(price_list, schema, table):
    cur = get_snowflake_conn()
    now = str(datetime.now())[:10]

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        flight_iata	            string	    NOT NULL,
        departure_sched_time	datetime	NOT NULL,
        price	                number	    NOT NULL,
        cabin	                string	    NOT NULL,
        created_date	        datetime	NOT NULL	DEFAULT CURRENT_DATE(),
        PRIMARY KEY (flight_iata, departure_sched_time)
    );
    """

    # 임시테이블 생성
    create_t_sql = f"""CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};"""

    try:
        cur.execute("BEGIN;")
        cur.execute(create_table_sql)
        logging.info(create_table_sql)
        cur.execute(create_t_sql)
        logging.info(create_t_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(e)
        raise
    
    # 임시테이블 적재
    insert_sql = f"""
    INSERT INTO t (flight_iata, departure_sched_time, price, cabin, created_date) 
    VALUES (%(flight_iata)s, %(departure_sched_time)s, %(price)s, %(cabin)s, '{now});""" 

    try:
        cur.execute("BEGIN;")
        for price in price_list:
            logging.info(f"Insert price: {price}")
            cur.execute(insert_sql, {
                'flight_iata': price['flight_iata'], 
                'departure_sched_time': price['departure_sched_time'], 
                'price': price['price'], 
                'cabin': price['cabin']
            })
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(e)
        raise

    # 기존 테이블 대체
    cur.execute(f"DELETE FROM {schema}.{table};")
    alter_sql = f"""
    INSERT INTO {schema}.{table} 
    SELECT DISTINCT flight_iata, departure_sched_time, price, cabin, created_date FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq
        FROM t
    )
    WHERE seq = 1;
    """
    try:
        cur.execute("BEGIN;")
        cur.execute(alter_sql)
        logging.info(alter_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(e)
        raise

trigger_analytics = TriggerDagRunOperator(
    task_id = 'trigger_analytics',
    trigger_dag_id = 'flights_analytics',
    execution_date = '{{ ds }}',
    reset_dag_run = True
)


with DAG(
    dag_id = 'flights_to_snowflake',
    start_date = datetime(2024,1,8),
    schedule = '0 1 * * *',
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    flight_list = extract_flight()
    # token = get_amadeus_token()
    # price_list = extract_price(token, flight_list)
    load_flight(flight_list, 'raw_data', 'flight_info') >> trigger_analytics
    # load_price(price_list, 'raw_data', 'flight_price')