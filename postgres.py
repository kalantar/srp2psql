import logging

import psycopg2

def connect(db_options:dict):
    '''
    Connect to (target) Postgres database.
    '''
    logging.info(f"connecting to postgres database: {db_options}")

    try:
        connection = psycopg2.connect(**db_options)
        return connection

    except psycopg2.Error as ex:
        logging.exception(f"ERROR: unable to connect to postgress database", ex)
        return None

def execute(connection, sql : str) -> None:
    logging.debug(f"postgres.execute() called with: {sql}")

    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
    except Exception as e:
        logging.error("error executing sql statement", e)
    finally:
        cursor.close()