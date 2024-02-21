import mysql.connector
from datetime import datetime
import numpy as np
from astropy.io import fits
import sys
import os

host = '192.168.11.226'
user = 'suitpoc2'
passwd = '@ditya$uitR00t'
database = 'suitDatabase'

def get_table_names():
    dbConnection = mysql.connector.connect(
        host=host,
        user=user,
        password=passwd,
        database=database
    )

    cursor = dbConnection.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    dbConnection.close()
    return tables
def get_columns_from_database(table_name):
    dbConnection = mysql.connector.connect(
        host=host,
        user=user,
        password=passwd,
        database=database
    )

    cursor = dbConnection.cursor()
    cursor.execute("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = %s AND EXTRA NOT LIKE '%auto_increment%'", (table_name,))
    columns = [column[0] for column in cursor.fetchall()]
    dbConnection.close()
    return columns

def addDataToTable(table, columns, values):
    try:
        dbConnection = mysql.connector.connect(
            host=host,
            user=user,
            password=passwd,
            database=database
        )

        if len(columns) != len(values):
            print("Columns and values do not match count")
            return
        
        input_datetime = datetime.strptime(values[columns.index('T_OBS')].split('.')[0], '%Y-%m-%dT%H:%M:%S') 
        formatted_date_str = input_datetime.strftime('%Y-%m-%d %H:%M:%S')
        
        select_sql = f"SELECT COUNT(*) FROM {table} WHERE {table}.T_OBS = STR_TO_DATE('{formatted_date_str}','%Y-%m-%d %H:%i:%s')"
        
        with dbConnection.cursor(buffered=True) as cursor:
            cursor.execute(select_sql)
            row_count = cursor.fetchone()[0]
         
        if row_count > 0:
            select_sql = f"SELECT T_OBS FROM {table} WHERE {table}.T_OBS = STR_TO_DATE('{formatted_date_str}','%Y-%m-%d %H:%i:%s')"
            
            with dbConnection.cursor(buffered=True) as cursor:
                cursor.execute(select_sql)
                existing_t_obs = cursor.fetchone()[0]

            if existing_t_obs != values[columns.index('T_OBS')]:
                update_sql = f"UPDATE {table} SET {','.join([f'{column}=%s' for column in columns])} WHERE T_OBS = %s"
                with dbConnection.cursor(buffered=True) as cursor:
                    cursor.execute(update_sql, values + [existing_t_obs])
        else: 
            insert_sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join(['%s'] * len(values))})"
            with dbConnection.cursor(buffered=True) as cursor:
                cursor.execute(insert_sql, values)

        dbConnection.commit()
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        if 'dbConnection' in locals() and dbConnection.is_connected():
            dbConnection.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='SUIT database fill script')
    parser.add_argument('inFolder',help='input folder')
    args =  parser.parse_args()
 
    inFiles = [os.path.join(args.inFolder,inF) for inF in os.listdir(args.inFolder) if inF.endswith('.fits')]
    
    for inF in inFiles:
        headerDetails = fits.getheader(inF)
        
        table_names = get_table_names()
                
        for table_name in table_names:
            table_columns = get_columns_from_database(table_name)
            values = []
            for column in table_columns:
                values.append(headerDetails.get(column, 'NA'))
            
            addDataToTable(table_name,table_columns, values)

if __name__ == '__main__':
    main()
