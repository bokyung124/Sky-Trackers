from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models import Connection
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta

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
    access_key = '82361307721bebdf9cdea8caae839a98'
    url = 'http://api.aviationstack.com/v1/flights'
    limit = 100
    params ={'access_key' : access_key, 'limit':limit, 'dep_iata':'ICN'}

    response = requests.get(url, params=params)
    flights = json.loads(response.text)

    flight_list = []

    try:
        for flight in flights.get('data'):
            arrival_country = flight['arrival']['iata']               # 도착 국가
            arrival_airport = flight['arrival']['airport']            # 도착 공항
            arrival_sched_time  = flight['arrival']['scheduled']      # 도착 예정 시간
            departure_airport = flight['departure']['airport']        # 출발 공항 (인천공항)
            departure_sched_time = flight['departure']['scheduled']   # 출발 예정 시간
            flight_iata = flight['flight']['iata']                    # 항공편명

            flight_dict = {'flight_iata':flight_iata, 'departure_sched_time':departure_sched_time, 'departure_airport':departure_airport, \
                    'arrival_country':arrival_country, 'arrival_airport':arrival_airport, 'arrival_sched_time':arrival_sched_time}
            flight_list.append(flight_dict)
        
        return flight_list
    except Exception as e:
        print(response.status_code)
        print(e)

@task
def get_amadeus_token():
    client_id = 'P6uEWTlCtDdZw9KhnZxnfxdl8cZU0PNU'
    client_secret = 'rmBhf9FqINQyhvHJ'
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
    url = "https://test.api.amadeus.com/v2/shopping/flight-offers"

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    price_list = []


    for f in flight_list:
        arrival_country = f['arrival_country']
        params = {
            'originLocationCode': 'ICN',  
            'destinationLocationCode': arrival_country, 
            'departureDate': '2024-01-11', 
            'currencyCode': 'KRW',
            'adults': '1', 
        }

        response = requests.get(url, headers=headers, params=params)

        try:
            offers = response.json()
            for offer in offers.get('data'):
                first_flight = offer.get('itineraries')[0]['segments'][0]
                flight_iata = first_flight['carrierCode'] + first_flight['number']
                departure_sched_time = first_flight['departure']['at']
                price = offer['price']['total']
                cabin = offer['travelerPricings'][0]['fareDetailsBySegment'][0]['cabin']

                price_dict = {'flight_iata':flight_iata, 'departure_sched_time':departure_sched_time, 'price':price, 'cabin':cabin}
                price_list.append(price_dict)
            return price_list
        except Exception as e:
            print(response.status_code)
            print(e)


@task
def load_flight(flight_list, schema, table):
    cur = get_snowflake_conn()
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        flight_iata	            string	    NOT NULL,
        departure_sched_time	datetime	NOT NULL,
        arrival_country	        string	    NOT NULL,
        arrival_airport	        string	    NOT NULL,
        departure_airport	    string	    NOT NULL	DEFAULT 'Seoul (Incheon)',
        arrival_sched_time	    datetime	NOT NULL,
        execution_date	        datetime	NOT NULL	DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (flight_iata, departure_sched_time)
    );
    """
    
    insert_sql = f"""
    INSERT INTO {schema}.{table} (flight_iata, departure_sched_time, arrival_country, arrival_airport, departure_airport, arrival_sched_time) 
    VALUES (%(flight_iata)s, %(departure_sched_time)s, %(arrival_country)s, %(arrival_airport)s, %(departure_airport)s, %(arrival_sched_time)s);
    """ 

    logging.info(create_table_sql)

    try:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(create_table_sql)
        for flight in flight_list:
            logging.info(f"Inserting flight: {flight}")
            cur.execute(insert_sql, {
                'flight_iata': flight['flight_iata'], 
                'departure_sched_time': flight['departure_sched_time'], 
                'arrival_country': flight['arrival_country'], 
                'arrival_airport': flight['arrival_airport'], 
                'departure_airport': flight['departure_airport'], 
                'arrival_sched_time': flight['arrival_sched_time']
            })
    except Exception as e:
        logging.error(e)
        raise

@task
def load_price(price_list, schema, table):
    cur = get_snowflake_conn()
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        flight_iata	            string	    NOT NULL,
        departure_sched_time	datetime	NOT NULL,
        price	                number	    NOT NULL,
        cabin	                string	    NOT NULL,
        execution_date	        datetime	NOT NULL	DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (flight_iata, departure_sched_time)
    );
    """
    

    insert_sql = f"""
    INSERT INTO {schema}.{table} (flight_iata, departure_sched_time, price, cabin) 
    VALUES (%(flight_iata)s, %(departure_sched_time)s, %(price)s, %(cabin)s)""" 

    logging.info(create_table_sql)
    try:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(create_table_sql)
        for price in price_list:
            logging.info(f"Insert price: {price}")
            cur.execute(insert_sql, {
                'flight_iata': price['flight_iata'], 
                'departure_sched_time': price['departure_sched_time'], 
                'price': price['price'], 
                'cabin': price['cabin']
            })
    except Exception as e:
        logging.error(e)
        raise


with DAG(
    dag_id = 'Airport_API',
    start_date = datetime(2023,1,8),
    schedule = '@daily',
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    flight_list = extract_flight()
    token = get_amadeus_token()
    price_list = extract_price(token, flight_list)
    load_flight(flight_list, 'raw_data', 'flight_info')
    load_price(price_list, 'raw_data', 'flight_price')