from DB import db_cursor, db_close
import os
import json
import pickle
import pandas as pd

def create_table(name, cursor, path = None, type_='json'):
    '''
    python design pattern: include cursor in function explicity
    '''
    #if type_=='CSV':

    if type_=='json':
        #create_table = "create or replace table myjsontable (json_data variant)"
        table_sql = 'create table {} IF NOT EXISTS (json_data variant)'.format(str(name))


    table_result = cursor.execute(table_sql)
    print(table_result.fetchall())

def create_landing_table(df, table_name, cursor, data_type='VARCHAR(100)'):
    '''
    - for csv or json
    - loads data into a table named as data frame
    '''

    sql_statement = 'CREATE OR REPLACE TABLE ' + str(table_name) + ' ( '
    for _, name in enumerate(df.columns):
        sql_statement += str(name) + ' ' + data_type + ','

    sql_statement = sql_statement[:-1] + ')'

    print(sql_statement)
    result = cursor.execute(sql_statement)
    print(result.fetchall())

def create_stage(name, cursor, format_):
    create_sql = "create stage IF NOT EXISTS {}  file_format = {};".format(str(name), str(format_))
    stage_result = cursor.execute(create_sql)
    print(stage_result.fetchone())


def create_format(name, cursor, type_='JSON'):
    # Create Stage:
    if type_=='JSON':
        format_sql = "create file format IF NOT EXISTS {} type = {} strip_outer_array = true;".format(str(name), str(type_))
    if type_=='CSV':
        format_sql = "create file format IF NOT EXISTS {} type = {} ;".format(str(name), str(type_))

    format_result = cursor.execute(format_sql)
    print(format_result.fetchall())
