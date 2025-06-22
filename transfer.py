import logging
import sys

import click
from dataclasses import dataclass
import pyodbc


import postgres as pg
import sql_server as ss

@dataclass
class Options:
    dry_run: bool
    table: str | None
    include_data: bool

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

def escape_pg(value):
    '''
    Escape values for PostgresSQL syntax.
    '''
    if value is None:
        return 'NULL'
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
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

def transfer_table(source_connection, target_connection, table_name : str) -> str:
    try:
        primary_key = ss.get_pk(source_connection, table_name)
        logging.info(f"------ primary key = {primary_key}")
    except Exception as e:
        logging.exception("ERROR looking for primary key", e)
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

def transfer_table(connection, table, options : Options):
    '''
    Handle single table requests
    '''
    table_defn = ss.get_table_definition(connection, table)
    pk_defn = ss.get_pk_definition(connection, table)
    fk_defn = ss.get_fk_definitions(connection, table)

    defn = f"""
{table_defn}
{pk_defn}
{fk_defn}
"""
    
    if options.include_data:
        logging.info("TBD -- include_data")

    if options.dry_run:
        print( defn )
    else:
        pg.execute(defn)


def main(options: Options):

    logging.info('__main__ called')
    logging.info(f'__main__: {options}')

    source_connection = None
    target_connection = None
    target_cursor = None

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
            transfer_table(source_connection, options.table, options)

        else:
            logging.info("TBD: handle multiple tables")

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
                logging.info(f"TBD - handle table {table}")

    #         try:
    #             # insert_statements = generate_insert_statements_for_table(srp_connection, table)
    #             print("--")
    #             print(f"-- {table}")
    #             # print(f"-- {table} ({len(insert_statements)} items)")
    #             print("--")
    #             transfer_table(source_connection, target_connection, table)
    #         except Exception as e:
    #             print(f"==== TABLE ERROR ({table}): {e}")


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
@click.option('-i', '--include-data', is_flag=True, help='Include data in the operation')
def cli(dry_run, table, include_data):
    options = Options(
        dry_run = dry_run,
        table = table,
        include_data = include_data,
    )
    main(options)
    
if __name__ == "__main__":

    cli()
