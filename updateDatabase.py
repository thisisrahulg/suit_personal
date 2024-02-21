#!/usr/bin/env python3

import numpy as np
from astropy.io import fits
import os,sys
import sqlite3
import pandas as pd
from tqdm import tqdm
import argparse
from conn import *


RADIUS = 1412

#dbConnection = sqlite3.connect("limbfit_database.db")
dbCursor = dbConnection.cursor()

def create_table():
    
    sqlFile = 'limbfit.sql'
    with open(sqlFile,'r') as tableFile:
        sqlQuery = tableFile.read()

    dbCursor.execute(sqlQuery)


#    dbCursor.execute("""
#        CREATE TABLE IF NOT EXISTS limbfit (
#            id INTEGER PRIMARY KEY AUTOINCREMENT,
#            datetime DATETIME,
#            modification_date DATETIME,
#            xpos INTEGER,
#            xerr REAL,
#            ypos INTEGER,
#            yerr REAL,
#            rpos INTEGER,
#            rerr REAL
#        )
#    """)
    dbConnection.commit()

def insertData(datetime,modificationTime,xpos,xerr,ypos,yerr,rpos,rerr):
    dbCursor.execute("""
        INSERT INTO limbfit (TIME,MOD_TIME, XC, XERROR, YC, YERROR, RC, RERROR)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (datetime,modificationTime,xpos,xerr,ypos, yerr, rpos, rerr))
    dbConnection.commit()


def checkModificationTime(lastModified):
    dbCursor.execute('SELECT * FROM limbfit WHERE MOD_TIME = %s', ( lastModified,))
    exists = dbCursor.fetchone()
    return exists is not None


def checkDateTime(datetime):
    dbCursor.execute('SELECT * FROM limbfit WHERE TIME = %s', (datetime,))
    exists = dbCursor.fetchone()
    return exists is not None

def processFolder(folderPath):
    create_table()
    fileList = os.listdir(folderPath)
    filePaths = [os.path.join(folderPath,file) for file in fileList]
    for filePath in tqdm(filePaths):
        if os.path.exists(filePath):
            modTime = os.path.getmtime(filePath)
            print(modTime)
            if not checkModificationTime(modTime):
                limbData = pd.read_csv(filePath,header=None)
                for index,row in limbData.iterrows():
                    datetime = row[0]
                    xpos = row[1]
                    xerr = row[2]
                    ypos = row[3]
                    yerr = row[4]
                    rpos = row[5]
                    rerr = row[6]
                    print('radius',rpos)
                    if (rpos-20) <= RADIUS <= (rpos+20):
                        if not checkDateTime(datetime):
                            insertData(datetime,modTime,xpos,xerr,ypos,yerr,rpos,rerr)
                        else:
                            print('Datetime already exists')
                    else: print('Radius not makes sense')
            else:
                print('AlreadyExists')

def main():
    parser = argparse.ArgumentParser(description='Update the database')
    parser.add_argument('masterFolder',help='masterlimbfit folder path')
    args = parser.parse_args()

    processFolder(args.masterFolder)

main()
