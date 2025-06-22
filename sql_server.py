import logging

import pyodbc

def connect(server:str, db:str) -> pyodbc.Connection | None:
    '''
    Get a connection to SRP database via ODBC driver.
    The current implementation assumes a local database.
    '''
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};" \
        f"SERVER={server};" \
        f"DATABASE={db};" \
        f"Trusted Connection=yes;"
    logging.info(f"connecting to ODBC database: {connection_string}")

    try:
        connection = pyodbc.connect(connection_string)
        return connection

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        if sqlstate == '28000':
            logging.error(f"ERROR: login failed due to windows authentication issue")
        else:
            logging.error(f"ERROR: unable to connect to ODBC database: {ex}")
        return None

def get_tables(connection : pyodbc.Connection) -> list:
    '''
    Get list of all tables from SQL Server database.
    '''
    logging.debug("sql_server.get_tables called")

    try:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES;")
        tables = [r.TABLE_NAME for r in cursor]
        logging.debug(f"sql_server.get_tables returning {tables}")
        return tables
    
    except Exception as ex:
        logging.exception("unable to read tables", ex)
        return []
        
    finally:
        cursor.close()

def toPostgressType(t, length):
    if t == "bigint":
        return "BIGINT"
    elif t == "nvarchar" or t == "varchar":
        if length == -1:
            return "TEXT"
        else:
            return f"VARCHAR({length})"
    elif t == "uniqueidentifier":
        return "UUID"
    elif t == "datetime":
        return "TIMESTAMP(3)"
    elif t == "tinyint":
        return "SMALLINT"
    elif t == "smallint":
        return "SMALLINT"
    elif t == "int":
        return "INT"
    elif t == "integer":
        return "INT"
    elif t == "bit":
        return "BOOLEAN"
    else:
        return None

def get_column_definition(column_info):
    logging.debug(f"sql_server.get_column_definition called for {column_info.COLUMN_NAME}")

    name = column_info.COLUMN_NAME
    typ = toPostgressType(column_info.DATA_TYPE, column_info.CHARACTER_MAXIMUM_LENGTH)
    nullable = ""
    if column_info.IS_NULLABLE == 'NO':
        nullable = " NOT NULL"
    if name == "Order":
        name = '"Order"'
    defn = f"{name} {typ}{nullable}"

    logging.debug(f"sql_server.get_column_definition returning {defn}")
    return defn

def get_table_definition(connection : pyodbc.Connection, table : str) -> str:
    logging.debug(f"sql_server.get_table_definition called for table = {table}")

    cursor = connection.cursor()
    sql = f"""
SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME='{table}';
"""
    cursor.execute(sql)

    defn = f"CREATE TABLE IF NOT EXISTS {table} ("

    first_column = True
    for column_info in cursor:
        defn = f"{defn}{"" if first_column else ","}\n{get_column_definition(column_info)}"
        first_column = False

    defn = f"""
{defn}
);"""
    
    cursor.close()
    logging.debug(f"sql_server.get_table_definition returning {defn}")
    return defn

def get_pk_cursor(connection : pyodbc.Connection, table : str) -> str:
    '''
    Find the column (name) of the primary key for the table.
    Assumes the primary key is just 1 field. This may not be the case.
    '''
    logging.debug(f"sql_server.get_pk_cursor called for table = {table}")

    cursor = connection.cursor()
    sql = f"""
SELECT 
    KCU.CONSTRAINT_NAME AS CONSTRAINT_NAME, 
    KCU.TABLE_NAME AS TABLE_NAME, 
    KCU.COLUMN_NAME AS COLUMN_NAME
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU
ON  KCU.CONSTRAINT_NAME = TC.CONSTRAINT_NAME

WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
AND KCU.TABLE_NAME='{table}';
"""
    cursor.execute(sql)
    return cursor

def get_pk(connection : pyodbc.Connection, table : str) -> str:
    '''
    Find the column (name) of the primary key for the table.
    Assumes the primary key is just 1 field. This may not be the case.
    '''
    logging.debug(f"sql_server.get_pk called for {table}")

    try:
        key = None
        cursor = get_pk_cursor(connection, table)
        for row in cursor:
            key = row.COLUMN_NAME
            break

        if key == None:
            logging.warning(f"no primary key found for table {table}")

        logging.debug(f"sql_server.get_pk returning {key}")
        return key
    except Exception as e:
        logging.error(f"unable to identify primary key for {table}", e)
        return None
    finally:
        cursor.close()

def get_pk_definition(connection : pyodbc.Connection, table : str) -> str:
    logging.debug(f"sql_server.get_pk_definition called for {table}")

    try:
        defn = ""
        first_row = True
        cursor = get_pk_cursor(connection, table)
        for row in cursor:
            if first_row:
                defn = f"""{defn}
    ALTER TABLE {table}
    ADD CONSTRAINT {row.CONSTRAINT_NAME}
    PRIMARY KEY ({row.COLUMN_NAME}"""
                first_row = False
            else:
                defn = f"{defn},{row.COLUMN_NAME}"
        defn=f"{defn});"

        logging.debug(f"sql_server.get_pk_definition returning {defn}")
        return defn
    except Exception as e:
        logging.error(f"unable to get primary key definition for {table}", e)
        return None
    finally:
        cursor.close()

def get_fk_definitions(connection, table) -> str:
    '''
    Generate SQL to define foreign keys for a given table.

    Reference: # https://stackoverflow.com/questions/3907879/sql-server-howto-get-foreign-key-reference-from-information-schema

    '''
    logging.debug(f"sql_server.get_foreign_keys called for table = {table}")
    defn = ""
    cursor = connection.cursor()
    sql = f"""
SELECT 
     KCU1.CONSTRAINT_SCHEMA AS FK_CONSTRAINT_SCHEMA 
    ,KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME 
    ,KCU1.TABLE_SCHEMA AS FK_TABLE_SCHEMA 
    ,KCU1.TABLE_NAME AS FK_TABLE_NAME 
    ,KCU1.COLUMN_NAME AS FK_COLUMN_NAME 
    ,KCU1.ORDINAL_POSITION AS FK_ORDINAL_POSITION 
    ,KCU2.CONSTRAINT_SCHEMA AS REFERENCED_CONSTRAINT_SCHEMA 
    ,KCU2.CONSTRAINT_NAME AS REFERENCED_CONSTRAINT_NAME 
    ,KCU2.TABLE_SCHEMA AS REFERENCED_TABLE_SCHEMA 
    ,KCU2.TABLE_NAME AS REFERENCED_TABLE_NAME 
    ,KCU2.COLUMN_NAME AS REFERENCED_COLUMN_NAME 
    ,KCU2.ORDINAL_POSITION AS REFERENCED_ORDINAL_POSITION 
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC 

INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1 
    ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
    AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
    AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 

INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU2 
    ON KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG  
    AND KCU2.CONSTRAINT_SCHEMA = RC.UNIQUE_CONSTRAINT_SCHEMA 
    AND KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME 
    AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION

WHERE KCU1.TABLE_NAME = '{table}'
"""
    cursor.execute(sql)
    for row in cursor:
        defn = f"""{defn}
ALTER TABLE {table}
ADD CONSTRAINT {row.FK_CONSTRAINT_NAME}
FOREIGN KEY ({row.FK_COLUMN_NAME})
REFERENCES {row.REFERENCED_TABLE_NAME}({row.REFERENCED_COLUMN_NAME});"""

    cursor.close()
    logging.debug(f"sql_server.get_foreign_keys returning {defn}")
    return defn