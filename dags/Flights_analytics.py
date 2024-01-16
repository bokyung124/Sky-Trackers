from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timezone, timedelta
import logging

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
            TO_CHAR(DATE_TRUNC('SECOND', A.departure_sched_time), 'HH24:MI:SS') AS departure_sched_time,
            A.arrival_city as city_code,
            B.country_code,
            B.country_kor as arrival_country,
            B.city_name as arrival_city,
            A.arrival_airport,
            TO_CHAR(DATE_TRUNC('SECOND', A.arrival_sched_time), 'HH24:MI:SS') AS arrival_sched_time,
            TO_CHAR(A.departure_sched_time, 'YYYY-MM-DD') as today
        FROM (SELECT * 
            FROM raw_data.flight_info
            WHERE TO_CHAR(departure_sched_time, 'YYYY-MM-DD') = '{today}') A
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
    cur = get_snowflake_conn()

    add_price_sql = f"""
    ALTER TABLE {schema}.{table} ADD COLUMN price number;
    """

    update_price_sql = f"""
    UPDATE {schema}.{table}
    SET price = (
        SELECT price, cabin
        FROM raw_data.flight_price p
        JOIN analytics.flight_info f
        ON p.flight_iata = f.flight_iata
        AND p.departure_sched_time = f.departure_sched_time
    );
    """

    try:
        cur.execute("BEGIN;")

        cur.execute(add_price_sql)
        cur.execute(update_price_sql)

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(e)
        raise


with DAG (
    dag_id = 'flights_analytics',
    start_date = datetime(2024, 1, 8),
    catchup = False,
) as dag:

    flight_join('analytics', 'flight_info') >> flight_lat_lon('analytics', 'flight_lat_lon')