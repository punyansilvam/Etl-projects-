import sys

import plotly.express as px
import pandas as pd
from Python.Project.CommonUtils import check_get_key
from Python.Project.DatabaseUtils import execute_select_query, get_config_object, get_mysql_connection

cmd_line_arguments = sys.argv
config_file_path = cmd_line_arguments[1]

configs = get_config_object(config_file_path)
database_config = check_get_key(configs, "mysql", True)
mysql_con = get_mysql_connection(database_config)
unified_database_name = check_get_key(database_config, "unified_database")


def show_bar(df, title, x_axis_col, y_axis_col, color_column="", hover_column_list=[]):
    # df = px.data.gapminder()
    fig = px.bar(df, x=x_axis_col, y=y_axis_col, color=color_column, labels={'y': y_axis_col},
                 hover_data=hover_column_list,
                 title=title)
    fig.show()


def show_line(df, title, x_axis_col, y_axis_col_list=[]):
    fig = px.line(df, x=x_axis_col, y=y_axis_col_list, title=title)
    fig.show()


df = execute_select_query(mysql_con, "select * from unified.query2")
df1 = execute_select_query(mysql_con, "select * from unified.query1")

show_bar(df, "Sample bar chart", 'year', 'value', color_column="datasource", hover_column_list=['country_name'])

show_line(df1, title="Sample line chart", x_axis_col='Year',
          y_axis_col_list=["gdp_value", "cpi_value", "inflation_value", "population_value",
                           "cash_surplus_deficit_value"])

