
__functionality__ = "ETL_RAW"

import sys
import traceback

from Python.Project.CommonUtils import print_log, check_get_key
from Python.Project.DatabaseUtils import get_mysql_connection, execute_select_query, get_config_object, \
    write_to_database, create_database
from Python.Project.custom_transformations import common_func_execute_without_callable, \
    common_func_execute_with_callable

cmd_line_arguments = sys.argv
config_file_path = cmd_line_arguments[1]


def extract(dbconnection, databasename, datasource):
    select_query = f"select * from {databasename}.{datasource}"
    #print(select_query)
    return execute_select_query(dbconnection, select_query)


def transform(df):
    try:
        # df = common_func_execute_without_callable(df)
        df = common_func_execute_with_callable(df)
        """
            # You can some more transformations here
        """
        return df
    except Exception as ex:
        print(f"Unable to apply transformations due to below error")
        raise ex


def load(database_config,databasename,tablename,df):
    write_to_database(database_config, databasename, tablename, df)


def main():
    mysql_con  = None
    try:
        print_log("MAIN_INIT", "Started the RAW layer execution", functionality=__functionality__)
        # Step1
        print_log("DEBUG1", f"Read the config file :{config_file_path} and creating config object", functionality=__functionality__)
        configs = get_config_object(config_file_path)
        database_config = check_get_key(configs, "mysql", True)
        mysql_con = get_mysql_connection(database_config)
        print_log("DEBUG3", f"Successfully acquired the database connection", functionality=__functionality__)
        datapackage_databasename = check_get_key(database_config, "datapackage_database")
        rawdatabasename = check_get_key(database_config, "raw_database")
        print_log("DEBUG4", f"Checking and creating database:{rawdatabasename} if not exist", functionality=__functionality__)
        create_database(mysql_con, rawdatabasename)
        datasources_to_create = str(check_get_key(database_config, "datasources")).split(",")
        # Step1
        for datasource in datasources_to_create:
            try:
                print_log("EXTRACT", f"Extracting data from {datapackage_databasename}.{datasource}",custom_tag=datasource.upper(),functionality=__functionality__)
                # Step3
                datasource = datasource.strip()
                extract_df = extract(mysql_con, datapackage_databasename, datasource)
                # Step4
                print_log("TRANSFORM", f"Applying transformations on  {datapackage_databasename}.{datasource} dataframe",custom_tag=datasource.upper(), functionality=__functionality__)
                transformed_df = transform(extract_df)
                #print(transformed_df.head(10))
                # adding datasource column
                transformed_df["datasource"] = datasource
                # Step5
                print_log("LOAD", f"Loading transformed data into   {rawdatabasename}.{datasource} table",custom_tag=datasource.upper(), functionality=__functionality__)
                load(database_config,  rawdatabasename,datasource, transformed_df)
            except Exception as ex:
                print_log("DATASOURCE_ERROR", f"Unable to process datasource successfully due to {traceback.format_exc()} ",
                          custom_tag=datasource.upper(), functionality=__functionality__)
    except Exception as ex:
        print_log("MAIN_ERROR", f"Error in processing main program due to :  {traceback.format_exc()} ", functionality=__functionality__)
    finally:
        mysql_con.close()
        print_log("MAIN_END", f"Completed ",functionality=__functionality__)


if __name__ == '__main__':
    main()
