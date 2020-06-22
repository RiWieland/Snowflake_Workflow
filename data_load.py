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
