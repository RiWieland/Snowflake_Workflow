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

def stage_file(stage, cursor, path):
    path_name = path
    load_sql = 'put file://{} @{} auto_compress=true;'.format(str(path_name), str(stage))
    load_result = cursor.execute(load_sql)
    print(load_result.fetchone())

def list_stage(stage,cursor):
    list_sql = "list @{}".format(str(stage))
    list_result = cursor.execute(list_sql)
    print(list_result.fetchall())


def copy_files(table, cusor, stage, format_, files=None):
    # Loop not used at moment
    #files_list = list(files)
    #for _, i in enumerate(files):
    #    new_name = stage + '/' + str(i) + '.gz'
    #    files_list.append(new_name)

    #copy_sql = "copy into {} from @{} FILES ={} file_format = (format_name = {});".format(str(table), str(stage), files_list, str(format))
    copy_sql = "copy into {} from @{} file_format = (format_name = {});".format(str(table), str(stage), str(format_))
    print(copy_sql)
    result = cusor.execute(copy_sql)
    print(result.fetchall())

def remove_stage(stage, cursor):
    remove_sql = "remove @{};".format(str(stage))
    result = cursor.execute(remove_sql)
    print(result.fetchall())

def check_table(table):
    select_sql = 'select * from {};'.format(str(table))
    check_results = cs.execute(select_sql)
    print(check_results.fetchall())
