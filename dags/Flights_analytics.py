from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timezone, timedelta
import logging
import requests
import json


def get_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_dev_db')
    conn = hook.get_conn()
    cursor = conn.cursor()
    return cursor


@task
def flight_join(schema, table):
    cur = get_snowflake_conn()
    KST = timezone(timedelta(hours=9))
    today = str(datetime.now(KST))[:10]

    create_table_sql = f"""
    CREATE TABLE {schema}.{table} AS
        SELECT 
            A.flight_iata,
            A.departure_sched_time AS departure_datetime,
            SUBSTR(A.departure_sched_time, 12, 8) AS departure_sched_time,
            A.arrival_city as city_code,
            B.country_code,
            B.country_kor as arrival_country,
            B.city_name as arrival_city,
            A.arrival_airport,
            SUBSTR(A.arrival_sched_time, 12, 8) AS arrival_sched_time,
            SUBSTR(A.departure_sched_time, 1, 10) as today
        FROM (SELECT * 
            FROM raw_data.flight_info
            WHERE SUBSTR(departure_sched_time, 1, 10) = '{today}') A
        JOIN raw_data.country_city_code B 
        ON A.arrival_city = B.city_code;
    """
    
    try:
        cur.execute("BEGIN;")

        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        logging.info('drop table')

        cur.execute(create_table_sql)
        logging.info('create table')

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(e)
        raise


@task
def flight_lat_lon(schema, table):
    cur = get_snowflake_conn()

    create_sql = f"""
    CREATE TABLE {schema}.{table} AS
        SELECT 
            A.flight_iata,
            A.DEPARTURE_SCHED_TIME,
            A.city_code,
            A.country_code,
            B.lat as arrival_lat,
            B.lon as arrival_lon,
            A.today
        FROM analytics.flight_info A 
        JOIN analytics.lat_lon_info B 
        ON A.city_code = B.city_code;      
    """

    update_depart_sql = f"""
        UPDATE {schema}.{table}
        SET departure_lat = (SELECT lat
                            FROM analytics.lat_lon_info
                            WHERE city_code = 'ICN'),
            departure_lon = (SELECT lon
                            FROM analytics.lat_lon_info
                            WHERE city_code = 'ICN');
    """

    try:
        cur.execute("BEGIN;")

        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        logging.info('drop table')

        cur.execute(create_sql)
        logging.info('create table')

        cur.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN departure_lat real;")
        cur.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN departure_lon real;")

        cur.execute(update_depart_sql)
        logging.info('update departure')

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(e)
        raise

@task
def flight_price_join(schema, table):

    merge_price_sql = f"""
    MERGE INTO {schema}.{table} f
    USING (
        SELECT 
            flight_iata,
            departure_sched_time,
            price,
            ROW_NUMBER() OVER(PARTITION BY flight_iata, departure_sched_time, cabin ORDER BY flight_iata) as rn
        FROM
            raw_data.flight_price
    ) p
    ON f.flight_iata = p.flight_iata AND f.departure_datetime = p.departure_sched_time
    WHEN MATCHED AND p.rn = 1 THEN
        UPDATE SET f.price = p.price;
    """

    merge_cabin_sql = f"""
    MERGE INTO {schema}.{table} f
    USING (
        SELECT 
            flight_iata,
            departure_sched_time,
            cabin,
            ROW_NUMBER() OVER(PARTITION BY flight_iata, departure_sched_time, cabin ORDER BY flight_iata) as rn
        FROM
            raw_data.flight_price
    ) p
    ON f.flight_iata = p.flight_iata AND f.departure_datetime = p.departure_sched_time
    WHEN MATCHED AND p.rn = 1 THEN
        UPDATE SET f.cabin = p.cabin;
    """


    try:
        cur = get_snowflake_conn()
        cur.execute("BEGIN;")

        cur.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN price number;")
        cur.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN cabin string;")
        cur.execute(merge_price_sql)
        cur.execute(merge_cabin_sql)
        cur.execute(f"DELETE FROM {schema}.{table} WHERE price IS NULL OR cabin IS NULL;")

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(e)



with DAG (
    dag_id = 'flights_analytics',
    start_date = datetime(2024, 1, 14),
    catchup = False,
    default_args = {
        'retries': 2,
        'retry_delay': timedelta(minutes=2),
    }
) as dag:

    flight_join('analytics', 'flight_info') >> flight_lat_lon('analytics', 'flight_lat_lon') >> flight_price_join('analytics', 'flight_info')