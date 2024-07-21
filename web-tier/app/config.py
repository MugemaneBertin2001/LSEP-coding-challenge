import psycopg2

import os

DB_CONFIG = {
    'dbname': 'tweeter_etl',
    'user': 'postgres',
    'password': 'Mine@123',
    'host': 'localhost',
    'port': 7777,
}


def connect_to_db(db_config):
    try:
        conn = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error connecting to the database: {e}")
        return None