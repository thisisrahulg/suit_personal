#!/usr/bin/env python3

import numpy as np
from astropy.io import fits
import os,sys
import sqlite3
import pandas as pd
from tqdm import tqdm
import argparse

RADIUS = 1412

dbConnection = sqlite3.connect("limbfit_database.db")
dbCursor = dbConnection.cursor()

def create_table():
    dbCursor.execute("""
        CREATE TABLE IF NOT EXISTS limbfit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            datetime DATETIME,
            modification_date DATETIME,
            xpos INTEGER,
            xerr REAL,
            ypos INTEGER,
            yerr REAL,
            rpos INTEGER,
            rerr REAL
        )
    """)
    dbConnection.commit()

def insertData(datetime,modificationTime,xpos,xerr,ypos,yerr,rpos,rerr):
    dbCursor.execute("""
        INSERT INTO limbfit (datetime, modification_date, xpos, xerr, ypos, yerr, rpos, rerr)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (datetime,modificationTime,xpos,xerr,ypos, yerr, rpos, rerr))
    dbConnection.commit()


def checkModificationTime(lastModified):
    dbCursor.execute('SELECT * FROM limbfit WHERE modification_date = ?', ( lastModified,))
    exists = dbCursor.fetchone()
    return exists is not None


def checkDateTime(datetime):
    dbCursor.execute('SELECT * FROM limbfit WHERE datetime = ?', (datetime,))
    exists = dbCursor.fetchone()
    return exists is not None

def processFolder(folderPath):
    create_table()
    fileList = os.listdir(folderPath)
    filePaths = [os.path.join(folderPath,file) for file in fileList]
    for filePath in tqdm(filePaths):
        if os.path.exists(filePath):
            modTime = os.path.getmtime(filePath)
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
