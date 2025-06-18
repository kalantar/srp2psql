import datetime
import sys

import pyodbc
import psycopg2

SRP_SERVER = "(LocalDB)\\SRP"
SRP_DATABASE = "C:\\USERS\\NABIL\\APPDATA\\ROAMING\\SRP\\SRP.MDF"
SRP_USER = "KALANTAR-N-T480\\nabil"

TARGET_DATABASE = {
    'dbname': 'kalantar_test',
    'user': 'kalantar',
    'host': '192.168.1.198',
    'port': '5432'
}

def connect_to_srp():
    '''
    Get a connection to SRP database via ODBC driver.
    The current implementation assumes a local database.
    '''
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};" \
        f"SERVER={SRP_SERVER};" \
        f"DATABASE={SRP_DATABASE};" \
        f"Trusted Connection=yes;"
    # print(f"srp_connection_string = {connection_string}")

    try:
        connection = pyodbc.connect(connection_string)
        return connection

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        if sqlstate == '28000':
            print(f"login failed due to windows authentication issue")
        else:
            print(f"srp_connection error: {ex}")
        return None

def connect_to_postgress():
    '''
    Connect to (target) Postgres database.
    '''
    try:
        connection = psycopg2.connect(**TARGET_DATABASE)
        return connection

    except psycopg2.Error as e:
        print(f"postgress error: {e}")
        return None

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

def escape_pg(value):
    '''
    Escape values for PostgresSQL syntax.
    '''
    if value is None:
        return 'NULL'
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)

def get_tables(connection):
    '''
    Get list of all tables from database.
    '''
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES;")
        tables = [r.TABLE_NAME for r in cursor]

        return tables
    except Exception as ex:
        sys.exit()
        
    finally:
        cursor.close()

def process_row(row):
    # print(f"process_row called for {row}")
    name = row.COLUMN_NAME
    typ = toPostgressType(row.DATA_TYPE, row.CHARACTER_MAXIMUM_LENGTH)
    nullable = ""
    if row.IS_NULLABLE == 'NO':
        nullable = " NOT NULL"
    if name == "Order":
        name = '"Order"'
    return f"{name} {typ}{nullable}"
    # print("process_row completed")

def define_table(connection, table: str) -> str:
    cursor = connection.cursor()
    sql = f"""
SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME='{table}';
"""
    cursor.execute(sql)

    defn = f"CREATE TABLE IF NOT EXISTS {table} ("

    first_row = True
    for row in cursor:
        defn = f"{defn}{"" if first_row else ","}\n{process_row(row)}"
        first_row = False

    defn = f"""
{defn}
);"""
    
    cursor.close()
    return defn

def get_primary_key(connection, table: str) -> str:
    '''
    Find the column (name) of the primary key for the table.
    Assumes the primary key is just 1 field. This may not be the case.
    '''
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
    for row in cursor:
        key = row.COLUMN_NAME
        break
    cursor.close()
    return key
    

def define_primary_key(connection, table: str) -> str:
    '''
    Generate SQL to define primary keys for a given table.
    '''
    defn = ""
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
    first_row = True
    cursor.execute(sql)
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

    cursor.close()
    return defn

def define_foreign_keys(connection, table) -> str:
    '''
    Generate SQL to define foreign keys for a given table.

    Reference: # https://stackoverflow.com/questions/3907879/sql-server-howto-get-foreign-key-reference-from-information-schema

    '''
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
    return defn

def get_database_defintion(connection):
    defn = ""

    tables = get_tables(connection)
    for table in tables:
        defn = f"""{defn}
{define_table(connection, table)}
{define_primary_key(connection, table)}"""
    for table in tables:
        defn = f"""{defn}
{define_foreign_keys(connection, table)}"""
        
    return defn

def create_target_database(connection, defn):
    cursor = connection.cursor()
    print("Creating")
    cursor.execute(defn)
    connection.commit()
    cursor.close()

def generate_insert_statements_for_table(connection, table_name : str) -> str:
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table_name};")
    column_names = [column[0] for column in cursor.description]
    primary_key = get_primary_key(connection, table_name)
    # rows = cursor.fetchall()

    insert_statements = []
    columns_str = ', '.join(column_names)

    for row in cursor:
        try:
            values = ',\n   '.join(escape_pg(val) for val in row)
            sql = f"""INSERT INTO {table_name} (
    {columns_str}
    ) VALUES (
    {values}
    )
    ON CONFLICT {primary_key} DO NOTHING;
    """
            target_cursor.execute(sql)
            
            insert_statements.append(sql)
        except Exception as e:
            print(f"==== STMT ERROR ({table_name}): {e}")

    return insert_statements

def transfer_table(source_connection, target_connection, table_name : str) -> str:
    try:
        primary_key = get_primary_key(source_connection, table_name)
        print(f"------ primary key = {primary_key}")
    except Exception as e:
        print(f"ERROR looking for primary key: {e}")
        return

    cursor = source_connection.cursor()
    cursor.execute(f"SELECT * FROM {table_name};")
    column_names = [column[0] for column in cursor.description]
    # rows = cursor.fetchall()

    columns_str = ', '.join(column_names)
    for row in cursor:
        try:
            values = ',\n   '.join(escape_pg(val) for val in row)
            sql = f"""INSERT INTO {table_name} (
{columns_str}
) VALUES (
{values}
)
ON CONFLICT {primary_key} DO NOTHING;
"""
            print("------")
            print(sql)
            print("------")
            target_cursor = target_connection.cursor()
            target_cursor.execute(sql)
            target_connection.commit()
            target_cursor.close()
        except Exception as e:
            print(f"==== STMT ERROR ({table_name}): {e}")

if __name__ == "__main__":
    srp_connection = None
    target_connection = None
    target_cursor = None

    srp_connection = connect_to_srp()
    if srp_connection == None:
        sys.exit()
    target_connection = connect_to_postgress()
    if target_connection == None:
        sys.exit()

    try:
        # defn = get_database_defintion(srp_connection)
        # print(defn)
        # create_target_database(target_connection, defn)

        for table in [
            "LoadDataFiles", 
            # "LocalizedStudyItems", 
            # "DBScriptHistories",
            # "ApplicationHistories",
            # "ApplicationConfigurations",
            # "Lists",
            # "ListColumns",
            # "ListDisplayColumns",
            # "ListSortColumns",
            # "ListFilterColumns",
            # "NationalCommunities",
            # "GroupOfRegions",
            # "Regions",
            # "Subregions",
            # "GroupOfClusters",
            # "Clusters",
            # "ElectoralUnits",
            # "Localities",
            # "Subdivisions",
            # "ClusterAuxiliaryBoardMembers",
            # "Cycles",
            # "Individuals",
            # "IndividualEmails",
            # "IndividualPhones",
            # "StudyItems",
            # "Activities",
            # "ActivityStudyItems",
            # "ActivityStudyItemIndividuals",
        ]:
            try:
                # insert_statements = generate_insert_statements_for_table(srp_connection, table)
                print("--")
                print(f"-- {table}")
                # print(f"-- {table} ({len(insert_statements)} items)")
                print("--")
                transfer_table(srp_connection, target_connection, table)
            except Exception as e:
                print(f"==== TABLE ERROR ({table}): {e}")


    except Exception as ex:
            print(f"error: {ex}")

    finally:
        if srp_connection:
            srp_connection.close()
        if target_connection:
            target_connection.close()