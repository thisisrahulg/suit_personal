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

def queryData(database_name, timestart, timestop, other_conditions):
    table_columns = get_table_info(database_name)
    join_clauses = []
    joined_tables = set()
    
    selected_columns = []
    
    if 'quickLookTable' in table_columns:
        if 'quickLookTable' not in joined_tables:
            joined_tables.add('quickLookTable')
#        selected_columns.append("quickLookTable.T_OBS")
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
        else:
            where_clause += f"{param.strip()} {operator.strip()} '{value.strip()}' AND "
    
    where_clause = where_clause[:-5]

    time_where_clause = f"quickLookTable.T_OBS BETWEEN '{timestart}' AND '{timestop}'"
    
    where_clause = f"{where_clause} AND {time_where_clause}" if where_clause else time_where_clause
    query = f"SELECT {', '.join(selected_columns)} FROM quickLookTable {' '.join(join_clauses)} WHERE {where_clause};"
    #print("Generated SQL Query:", query)
    cursor = connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    connection.close()

    return [row[0] for row in rows]

def suitsearch():
    parser = argparse.ArgumentParser(description='SUIT database search script')
    parser.add_argument('startTime',help='starttime. Format = "%Y-%m-%d %H:%M:%S"')
    parser.add_argument('endTime',help='endtime. Format = "%Y-%m-%d %H:%M:%S"')
    parser.add_argument('conditions',nargs='+',help='All other conditions. Give as array for multiple parameters. Eg: [ param1 = value1, param2 >= value2, param3 contains value3]')
    args =  parser.parse_args()
    

    database_name = 'suitDatabase' 
    output = queryData(database_name, args.startTime, args.endTime,args.conditions)
    print(output)
    return output


if __name__ == '__main__':
    suitsearch()

