
import sys
import traceback

from pandas import DataFrame
import pandasql as psql

from Python.Project.CommonUtils import print_log, check_get_key
from Python.Project.DatabaseUtils import get_mysql_connection, execute_select_query, get_config_object, \
    write_to_database, create_database
from Python.Project.custom_transformations import common_func_execute_without_callable, \
    common_func_execute_with_callable

cmd_line_arguments = sys.argv
config_file_path = cmd_line_arguments[1]


def extract(dbconnection, databasename, datasource) -> DataFrame:
    select_query = f"select * from {databasename}.{datasource}"
    # print(select_query)
    return execute_select_query(dbconnection, select_query)


def transform(df):
    return df


def load(database_config, databasename, tablename, df):
    write_to_database(database_config, databasename, tablename, df)


def main():
    mysql_con = None
    raw_datasource_dataframes = []
    try:
        print_log("MAIN_INIT", "Started the RAW layer execution", functionality=__functionality__)
        print_log("DEBUG1", f"Read the config file :{config_file_path} and creating config object",
                  functionality=__functionality__)
        configs = get_config_object(config_file_path)
        database_config = check_get_key(configs, "mysql", True)
        mysql_con = get_mysql_connection(database_config)
        print_log("DEBUG3", f"Successfully acquired the database connection", functionality=__functionality__)

        rawdatabasename = check_get_key(database_config, "raw_database")

        unified_database_name = check_get_key(database_config, "unified_database")
        print_log("DEBUG4", f"Checking and creating database:{unified_database_name} if not exist",
                  functionality=__functionality__)
        create_database(mysql_con, unified_database_name)
        datasources_to_create = str(check_get_key(database_config, "datasources")).split(",")
        print_log("DEBUG5", f"Extracting required tables and creating respective dataframes ",
                  functionality=__functionality__)
        for datasource in datasources_to_create:
            raw_datasource_dataframes.append(extract(mysql_con, rawdatabasename, datasource))
            print_log("EXTRACT", f"Extracted the table :{datasource}", custom_tag=datasource.upper(),
                      functionality=__functionality__)
        gdp, cpi, inflation, population, cash_surplus_deficit = raw_datasource_dataframes

        unified_config = check_get_key(configs, "unified", True)
        sql_files = str(check_get_key(unified_config, "sqlfiles", False)).split(",")
        sql_files_table = str(check_get_key(unified_config, "sqlfile_tablename", False)).split(",")
        sql_file_table_tuple = zip(sql_files, sql_files_table)
        #print(list(sql_file_table_tuple))
        for sql_file in sql_file_table_tuple:
            with open(sql_file[0], 'r') as file:
                select_query = file.read()
                print_log("CUSTOM_QUERY", f"Executing query : {select_query} ", custom_tag=sql_file[1],
                          functionality=__functionality__)
                df = psql.sqldf(select_query)
                print_log("TRANSFORM", f"Applying Transformations for : {select_query} ", custom_tag=sql_file[1],
                          functionality=__functionality__)
                transformed_df = transform(df)
                row, col = df.shape
                if row > 0:
                    print_log("LOAD", f"Loading transformed data into table : {sql_file[1]} ",
                              custom_tag=sql_file[1],
                              functionality=__functionality__)
                    #print(transformed_df)
                    load(database_config, unified_database_name, sql_file[1], transformed_df)
    except Exception as ex:
        print_log("MAIN_ERROR", f"Error in processing main program due to :  {traceback.format_exc()} ",
                  functionality=__functionality__)
    finally:
        mysql_con.close()
        print_log("MAIN_END", f"Completed ", functionality=__functionality__)


if __name__ == '__main__':
    main()
