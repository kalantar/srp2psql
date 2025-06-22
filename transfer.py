import datetime
import logging
import sys

import click
from dataclasses import dataclass

import postgres as pg
import sql_server as ss

@dataclass
class Options:
    dry_run: bool
    table: str | None
    include_data: bool
    data_only: bool

SRP_SERVER = "(LocalDB)\\SRP"
SRP_DATABASE = "C:\\USERS\\NABIL\\APPDATA\\ROAMING\\SRP\\SRP.MDF"
SRP_USER = "KALANTAR-N-T480\\nabil"

TARGET_DATABASE = {
    'dbname': 'kalantar_test',
    'user': 'kalantar',
    'host': '192.168.1.198',
    'port': '5432'
}

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def get_table_definition(connection, table):
    '''
    Retrieve the table definition (SQL to create a copy) including primary and foreign keys.
    '''
    logging.debug(f"get_table_definition called for {table}")

    table_defn = ss.get_table_definition(connection, table)
    pk_defn = ss.get_pk_definition(connection, table)
    fk_defn = "" #ss.get_fk_definitions(connection, table)

    defn = f"""
{table_defn}
{pk_defn}
{fk_defn}
"""
    
    logging.debug(f"get_table_definition returning {defn}")
    return defn

def get_table_values(connection, table):
    '''
    Get SQL to insert all values from a table.
    '''
    logging.debug(f"get_table_values called for {table}")

    pk = ss.get_pk(connection, table)
    values_cursor = connection.cursor()
    values_cursor.execute(f"SELECT * FROM {table};")
    column_names = [column[0] for column in values_cursor.description]
    columns_str = ', '.join(column_names)
    for row in values_cursor:
        try:
            values = ',\n   '.join(escape_pg(val) for val in row)
            values_sql = f"""INSERT INTO {table} (
{columns_str}
) VALUES (
{values}
)
ON CONFLICT ({pk}) DO NOTHING;
"""

        except Exception as e:
            print(f"==== STMT ERROR ({table}): {e}")
    
    logging.debug(f"get_table_values returning {values_sql}")
    return values_sql


def escape_pg(value):
    '''
    Escape values for PostgresSQL syntax.
    '''
    if value is None:
        return 'NULL'
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, datetime.datetime):
        return "'" + value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "'"
    return str(value)

def generate_insert_statements_for_table(connection, table_name : str) -> str:
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table_name};")
    column_names = [column[0] for column in cursor.description]
    primary_key = ss.get_pk_definition(connection, table_name)

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
            # target_cursor.execute(sql)
            
            insert_statements.append(sql)
        except Exception as e:
            print(f"==== STMT ERROR ({table_name}): {e}")

    return insert_statements

def transfer_table(source_connection, target_connection, table : str, options : Options) -> str:

    defn_sql = ""
    values_sql = ""

    try:
        # get table
        if not options.data_only:
            defn_sql = get_table_definition(source_connection, table)

        # get data
        if options.include_data or options.data_only:
            values_sql = get_table_values(source_connection, table)

        if options.dry_run:
            print( defn_sql )
            print (values_sql)
        else:
            logging.info("executing sql")
            pg.execute(target_connection, defn_sql)
            pg.execute(target_connection, values_sql)

    except Exception as e:
        logging.exception(f"error transfering table {table}", e)

def main(options: Options):

    logging.info('main called with {options}')

    source_connection = None
    target_connection = None

    try:

        # always need to have connection to source database
        source_connection = ss.connect(SRP_SERVER, SRP_DATABASE)
        if source_connection == None:
            sys.exit()

        # need target database only if not dry run
        if not options.dry_run:    
            target_connection = pg.connect(TARGET_DATABASE)
            if target_connection == None:
                sys.exit()

        if options.table != None:
            logging.debug(f"main transferring a single table")
            transfer_table(source_connection, target_connection, options.table, options)

        else:
            logging.debug("main transferring all tables")

            for table in [
                # "LoadDataFiles", 
                "StudyItems",
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
                # "Activities",
                # "ActivityStudyItems",
                # "ActivityStudyItemIndividuals",
            ]:
                transfer_table(source_connection, target_connection, table, options)

    except Exception as ex:
            print(f"error: {ex}")

    finally:
        if source_connection:
            source_connection.close()
        if target_connection:
            target_connection.close()

@click.command()
@click.option('-d', '--dry-run', is_flag=True, help='Run without making changes')
@click.option('-t', '--table', help='Name of the table to operate on')
@click.option('-i', '--include-data', is_flag=True, help='Include data')
@click.option('-o', '--data-only', is_flag=True, help='Include only data -- no table definition')
def cli(dry_run, table, include_data, data_only):
    options = Options(
        dry_run = dry_run,
        table = table,
        include_data = include_data,
        data_only = data_only,
    )
    main(options)
    
if __name__ == "__main__":

    cli()
