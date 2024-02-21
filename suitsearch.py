#!/usr/bin/env python3
import mysql.connector
import argparse

database_name='suitDatabase'
connection = mysql.connector.connect(
        host="192.168.11.226",
        user="suitpoc2",
        password="@ditya$uitR00t",
        database=database_name
    )

cursor = connection.cursor()



img_type_dict = {
    0:'Normal',
    1:'starcal1',
    2:'starcal2',
    3:'offpoint',
    4:'engg4',
    5:'engg5',
    6:'engg6',
    7:'engg7',
    8:'eeprom',
    9:'engg9',
    10:'led355',
    11:'led255',
    12:'closedak',
    13:'opendark',
    14:'openbias',
    15:'closebias'
}


def get_table_info(database_name):

    query = f"""
        SELECT 
            table_name, 
            column_name 
        FROM 
            information_schema.columns 
        WHERE 
            table_schema = '{database_name}';
    """

    cursor.execute(query)

    table_info = cursor.fetchall()

    table_columns = {}
    for table, column in table_info:
        if table in table_columns:
            table_columns[table].append(column)
        else:
            table_columns[table] = [column]

    return table_columns

def queryData(timestart, timestop, other_conditions,database_name='suitDatabase'):
    table_columns = get_table_info(database_name)
    join_clauses = []
    joined_tables = set()
    
    selected_columns = []
    
    if 'quickLookTable' in table_columns:
        if 'quickLookTable' not in joined_tables:
            joined_tables.add('quickLookTable')
        selected_columns.append("quickLookTable.F_NAME")
    #print(selected_columns)
    #input()
    where_clause = ""
    for condition in other_conditions:
        param, operator, value = condition.split(' ')
        for table, columns in table_columns.items():
            if param in columns:
                if table not in joined_tables:
                    join_clauses.append(f"LEFT JOIN {table} ON quickLookTable.T_OBS = {table}.T_OBS")
                    joined_tables.add(table)
                break
        if operator == 'contains':
            where_clause += f"{param.strip()} LIKE '%{value.strip()}%' AND "
        elif operator == 'between':
            where_clause += f"{param.strip()} BETWEEN '{value.split(',')[0].strip()}' AND '{value.split(',')[1].strip()}' AND "
        else:
            where_clause += f"{param.strip()} {operator.strip()} '{value.strip()}' AND "
    
    where_clause = where_clause[:-5]

    time_where_clause = f"quickLookTable.T_OBS BETWEEN '{timestart}' AND '{timestop}'"
    
    where_clause = f"{where_clause} AND {time_where_clause}" if where_clause else time_where_clause
    query = f"SELECT {', '.join(selected_columns)} FROM quickLookTable {' '.join(join_clauses)} WHERE {where_clause};"
    print("Generated SQL Query:", query)
    cursor = connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    connection.close()

    return [row[0] for row in rows]

def suitsearch():
    parser = argparse.ArgumentParser(description='SUIT database search script')
    parser.add_argument('level',type=float,help='Level of the file.[0.5, 1.0]"')
    parser.add_argument('startTime',help='starttime. Format = "%Y-%m-%d %H:%M:%S"')
    parser.add_argument('endTime',help='endtime. Format = "%Y-%m-%d %H:%M:%S"')
    parser.add_argument('conditions',nargs='+',help='All other conditions. Give as array for multiple parameters. Eg: [ param1 = value1, param2 >= value2, param3 contains value3]')
    args =  parser.parse_args()
    

    database_name = 'suitDatabase' 
    output = queryData(args.startTime,args.endTime,args.conditions)
    fullPaths = []
    if output:
        for outFile in output:
            year,month,day = outFile.split('_')[5].split('T')[0].split('-')
            imgType = int(outFile.split('_')[6][0])
            imgSize = int(outFile.split('_')[6][3])
            if imgType == 0:
                if imgSize == 1:
                    imtype = 'normal_4k'
                elif imgSize == 2:
                    imtype = 'normal_2k'
                elif imgSize  == 3:
                    imtype = 'normal_roi'
            else:
                imtype = img_type_dict[imgType]
            if args.level == 0.5:
               fullPath = f'/scratch/suit_data/level0fits/{year}/{month}/{day}/{imtype}/{outFile}' 
            if args.level == 1.0:
               fullPath = f'/scratch/suit_data/level1.5fits/{year}/{month}/{day}/{imgtype}/{outFile}'
            fullPaths.append(fullPath)
    print(fullPaths)
    return fullPaths


if __name__ == '__main__':
    suitsearch()

