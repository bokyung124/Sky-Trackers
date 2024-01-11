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

            flight_list.append("('{}','{}','{}','{}','{}','{}')".format(flight_iata, departure_sched_time, arrival_country, \
                                                                        arrival_airport, departure_airport, arrival_sched_time))
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
        access_token = token_data['access_token']
        return access_token
    else:
        raise Exception("Token request failed with status code {}".format(response.status_code))

@task
def extract_price(flight_data):
    token = get_amadeus_token()
    url = "https://test.api.amadeus.com/v2/shopping/flight-offers"

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    price_list = []


    for f in flight_data:
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

                price_list.append("('{}','{}','{}','{}')".format(flight_iata, departure_sched_time, price, cabin))
            return price_list
        except Exception as e:
            print(response.status_code)
            print(e)

# @task
# def transform_flight(flight_data):
#     flight_df = pd.DataFrame(flight_data)
#     return flight_df

# @task
# def transform_price(price_data):
#     price_df = pd.DataFrame(price_data)
#     return price_df


@task
def load_flight(flight_list, schema, table):
    cur = get_snowflake_conn()
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        flight_iata	            string	    NOT NULL	PRIMARY KEY,
        departure_sched_time	datetime	NOT NULL    PRIMARY KEY,
        arrival_country	        string	    NOT NULL,
        arrival_airport	        string	    NOT NULL,
        departure_airport	    string	    NOT NULL	DEFAULT 인천,
        arrival_sched_time	    datetime	NOT NULL,
        execution_date	        datetime	NOT NULL	DEFAULT CURRENT_DATE()
    );
    """
    
    insert_sql = f"""
    INSERT INTO {schema}.{table} (flight_iata, departure_sched_time, arrival_country, arrival_airport, departure_airport, arrival_sched_time)
    VALUES """ + ",".join(flight_list)

    logging.info(create_table_sql)
    logging.info(insert_sql)

    try:
        cur.execute(create_table_sql)
        cur.execute(insert_sql)
        cur.execute("Commit;")
    except Exception as e:
        cur.execute("Rollback;")
        raise

@task
def load_price(price_list, schema, table):
    cur = get_snowflake_conn()
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        flight_iata	            string	    NOT NULL    PRIMARY KEY,
        departure_sched_time	datetime	NOT NULL    PRIMARY KEY,
        price	                number	    NOT NULL,
        cabin	                string	    NOT NULL,
        execution_date	        datetime	NOT NULL	DEFAULT CURRENT_DATE()
    );
    """
    
    insert_sql = f"""
    INSERT INTO {schema}.{table} (flight_iata, departure_sched_time, price, cabin) VALUES """ + ",".join(price_list)

    logging.info(create_table_sql)
    logging.info(insert_sql)
    try:
        cur.execute(create_table_sql)
        cur.execute(insert_sql)
        cur.execute("Commit;")
    except Exception as e:
        cur.execute("Rollback;")
        raise


with DAG(
    dag_id = 'Airport_API',
    start_date = days_ago(1),
    schedule = '@once',
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    flight_list = extract_flight()
    price_list = extract_price()
    load_flight(flight_list, 'raw_data', 'flight_info')
    load_price(price_list, 'raw_data', 'flight_price')



