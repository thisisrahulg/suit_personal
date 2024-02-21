#!/usr/bin/env python3

import sqlite3
import sys
import os,argparse
from astropy.io import fits
from datetime import datetime
from conn import *

#dbConnection = sqlite3.connect('limbfit_database.db')
dbCursor = dbConnection.cursor()

def findClosestTime(targetTime):
    dbCursor.execute('''
        SELECT * FROM limbfit
    ''')
    all_records = dbCursor.fetchall()

    if not all_records:
        return None
    target_datetime = datetime.strptime(targetTime, '%Y-%m-%dT%H.%M.%S.%f')
    closestRecord = min(all_records, key=lambda record: abs(target_datetime - record[1]))
    print(closestRecord)
    return closestRecord

def addSunCentre(inFile):
    fileName = os.path.basename(inFile)
    time = fileName.split('_')[5]
    print(time)

    closestRecord = findClosestTime(time)

    if closestRecord:
        t, xVal, yVal, rVal = closestRecord[1].strftime("%Y-%m-%dT%H:%M:%S"), closestRecord[2], closestRecord[4], closestRecord[6]
        print(t, xVal, yVal, rVal)

        with fits.open(inFile, mode='update') as inFitsFile:
            headers = inFitsFile[0].header
            headers['CRTIME'] = t
            headers.comments['CRTIME'] = 'Time of sun centre information'
            headers['CRPIX1'] = xVal
            headers.comments['CRPIX1'] = 'Sun Centre X position in Pixels'
            headers['CRPIX2'] = yVal
            headers.comments['CRPIX2'] = 'Sun Centre Y position in Pixels'
            headers['RSUN_OBS'] = rVal
            headers.comments['RSUN_OBS'] = 'Calculated Sun radius in pixels'
            inFitsFile.flush()
    else:
        print("No matching record found in the database.")


def main():
    parser = argparse.ArgumentParser(description='Sun centre information adding module')
    parser.add_argument('inFolder',help='Input folder')
    args = parser.parse_args()

    inFiles = os.listdir(args.inFolder)
    inF = [os.path.join(args.inFolder,f) for f in inFiles if f.endswith('.fits')]

    for inFits in inF:
        addSunCentre(inFits)

if __name__ == '__main__':
    main()
