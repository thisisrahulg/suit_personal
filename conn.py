import mysql.connector

host = '10.10.10.36'
user = 'suitpoc2'
password = '@ditya$uitR00t'
database = 'limbfitDatabase'

dbConnection = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

