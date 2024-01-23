from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta, timezone

import requests
import logging
import json
import pandas as pd
from io import StringIO

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
                    departure_sched_time = flight['departure']['scheduled'][:19]   # 출발 예정 시간
                    flight_iata = flight['flight']['iata']                    # 항공편명
                    created_date = flight['departure']['scheduled'][:10]

                    if flight_iata is not None and departure_sched_time is not None:
                        flight_dict = {'flight_iata':flight_iata, 'departure_sched_time':departure_sched_time, 'arrival_city':arrival_city, \
                                'arrival_airport':arrival_airport, 'departure_airport':departure_airport, 'arrival_sched_time':arrival_sched_time, 'created_date':created_date}
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
    today = flight_list[0]['departure_sched_time'][:10]

    logging.info(f'token: {token}')
    url = "https://test.api.amadeus.com/v2/shopping/flight-offers"

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    price_list = []
    arrival_city = set()
    iata_list = []

    if flight_list is None:
        raise ValueError("flight_list is None")

    for flight in flight_list:
        if flight['departure_sched_time'][:10] == today:
            arrival_city.add(flight['arrival_city']) 


    logging.info(f'cities: {len(arrival_city)}')

    arrival_city = list(arrival_city)
    for city in arrival_city:
        logging.info(f'departure: {today}, city: {city}')
        params = {
            'originLocationCode': 'ICN',  
            'destinationLocationCode': city, 
            'departureDate': today, 
            'currencyCode': 'KRW',
            'adults': '1', 
            'nonStop': 'true'
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            
            offers = response.json()
            if not offers or not offers.get('data'):  
                logging.error("No offer data")
                continue
            
            for offer in offers.get('data'):
                first_flight = offer.get('itineraries')[0]['segments'][0]
                flight_iata = first_flight['carrierCode'] + first_flight['number']
                departure_sched_time = first_flight['departure']['at'][:19]
                price = offer['travelerPricings'][0]['price']['total']
                cabin = offer['travelerPricings'][0]['fareDetailsBySegment'][0]['cabin']

                if (flight_iata, departure_sched_time, cabin) not in iata_list:
                    logging.info(f'departure: {departure_sched_time}, price: {price}')
                    price_dict = {'flight_iata':flight_iata, 'departure_sched_time':departure_sched_time, 'price':price, 'cabin':cabin, 'created_date':today}
                    price_list.append(price_dict)
                    iata_list.append((flight_iata, departure_sched_time, cabin))
                else:
                    continue
        except Exception as e:
            logging.info(response.status_code)
            logging.info(response.text)
            logging.error(e)
            raise
    return price_list


@task
def flight_to_s3(flight_list):
    hook = S3Hook('s3_upload')
    today = str(datetime.now())[:10]

    flight_df = pd.DataFrame(flight_list)

    flight_buffer = StringIO()
    flight_df.to_csv(flight_buffer, index=False)

    try:
        hook.load_string(
            string_data=flight_buffer.getvalue(), 
            key=f"info/flight_info_{today}.csv", 
            bucket_name="flights-info", 
            replace=True)
        logging.info('s3 load')
    except Exception as e:
        logging.error(e)
        raise
    

@task
def flight_to_snowflake(schema, table):
    cur = get_snowflake_conn()
    key = Variable.get('s3_access_key')
    secret = Variable.get('s3_access_secret')

    today = str(datetime.now())[:10]

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        flight_iata	            string,
        departure_sched_time	datetime,
        arrival_city	        string,
        arrival_airport	        string,
        departure_airport	    string	    DEFAULT 'Seoul (Incheon)',
        arrival_sched_time	    datetime,
        created_date	        string,
        PRIMARY KEY(flight_iata, departure_sched_time)
    );
    """

    create_t_sql = f"""CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};"""

    copy_sql = f"""
        COPY INTO t
        FROM 's3://flights-info/info/flight_info_{today}.csv'
        CREDENTIALS = (aws_key_id='{key}' aws_secret_key='{secret}')
        ON_ERROR = CONTINUE;
    """

    insert_sql = f"""
        INSERT INTO {schema}.{table}
        SELECT * FROM t;
    """ 

    try:
        cur.execute(create_table_sql)
        logging.info(create_table_sql)
        cur.execute(create_t_sql)
        logging.info(create_t_sql)

        cur.execute("BEGIN;")
        cur.execute(copy_sql)
        logging.info(copy_sql)

        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        logging.error(e)
        cur.execute("ROLLBACK;")
        raise


@task
def price_to_s3(price_list):
    hook = S3Hook('s3_upload')
    today = str(datetime.now())[:10]

    price_df = pd.DataFrame(price_list)

    price_buffer = StringIO()
    price_df.to_csv(price_buffer, index=False)


    try:
        hook.load_string(
            string_data=price_buffer.getvalue(), 
            key=f"price/flight_price_{today}.csv", 
            bucket_name="flights-info", 
            replace=True)
        logging.info('s3 load')
    except Exception as e:
        logging.error(e)
        raise

@task
def price_to_snowflake(schema, table):
    cur = get_snowflake_conn()
    key = Variable.get('s3_access_key')
    secret = Variable.get('s3_access_secret')
    today = str(datetime.now())[:10]

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        flight_iata	            string,
        departure_sched_time	datetime,
        price	                number,
        cabin	                string,
        created_date	        string,
        PRIMARY KEY(flight_iata, departure_sched_time)
    );
    """

    create_t_sql = f"""CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};"""

    copy_sql = f"""
        COPY INTO t
        FROM 's3://flights-info/price/flight_price_{today}.csv'
        CREDENTIALS = (aws_key_id='{key}', aws_secret_key='{secret}')
        ON_ERROR = 'CONTINUE';
    """

    insert_sql = f"""
        INSERT INTO {schema}.{table}
        SELECT *
        FROM t
        WHERE NOT EXISTS (
            SELECT 1
            FROM {schema}.{table}
            WHERE {schema}.{table}.flight_iata = t.flight_iata
                AND {schema}.{table}.departure_sched_time = t.departure_sched_time
                AND {schema}.{table}.cabin = t.cabin
        );
    """ 

    try:
        cur.execute(create_table_sql)
        logging.info(create_table_sql)
        cur.execute(create_t_sql)
        logging.info(create_t_sql)

        cur.execute("BEGIN;")
        cur.execute(copy_sql)
        logging.info(copy_sql)

        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        logging.error(e)
        cur.execute("ROLLBACK;")
        raise


trigger_analytics = TriggerDagRunOperator(
    task_id = 'trigger_analytics',
    trigger_dag_id = 'flights_analytics',
    execution_date = '{{ ds }}',
    reset_dag_run = True
)


with DAG(
    dag_id = 'flights_to_snowflake',
    start_date = datetime(2024,1,17),
    schedule = '0 1 * * *',
    catchup = False,
    default_args = {
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    flight_list = extract_flight()
    token = get_amadeus_token()
    price_list = extract_price(token, flight_list)
    flight_to_s3(flight_list) >> flight_to_snowflake('raw_data', 'flight_info')
    price_to_s3(price_list) >> price_to_snowflake('raw_data', 'flight_price') >> trigger_analytics
