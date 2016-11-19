#!/usr/bin/python2

import pandas as pd
import sqlite3 as sq
import sys
import re
import json

db = sq.connect('gojek.db')
c = db.cursor()

def pp_tables():
    # get table names
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    names = c.fetchall()
    # function to parse rows
    p = re.compile(r'(?<![0-9]|:|,|\{)"(?![0-9]|:|,"|\})')
    #parse = lambda r: eval(p.sub('', r[r.find('{'):r.rfind('}')+1].replace(':""',':None').replace(':null',':None').replace(':true',':True').replace(':false',':False')))
    parse = json.loads

    tables = {}
    errors = {}
    for t in names:
        c.execute("SELECT * FROM %s" % t)
        print >> sys.stderr, "table: %s" % t[0]
        row = []
        err = []
        for n, r in c.fetchall():
            try:
                row.append(parse(p.sub('',r.replace('\t',' '))))
            except:
                err.append(n)
        print('len: ' + str(len(row)) + ' err: ' + str(len(err)))
        errors[t[0]] = err
        tables[t[0]] = pd.DataFrame(row)
        tables[t[0]].dropna(axis=1, how='all', inplace=True)
    del row, err
    return tables, errors
    
def pp_bookings(keys=['dispatchTime', 'arrivalTime', 'closingTime', 'driverCalledTime', 'cancelTime', 'timeField', 'driverLatitude', 'driverLongitude', 'driverPickupLocation', 'driverCloseLocation']):
    p = re.compile(r'(?<![0-9]|:|,|\{)"(?![0-9]|:|,"|\})')
    c.execute(u'SELECT * FROM bookings')
    bookings = []
    for n, r in c.fetchall():
        try:
            row = json.loads(p.sub('', r.replace('\t',' ')))
            if keys:
                [row.pop(k) for k in row.keys() if k not in keys]
            bookings.append(row)
        except:
            print(n)
    return bookings

def pp_customers():
    db = sq.connect('/home/cilsat/dat/gojek.db')
    c = db.cursor()

    parse = lambda r: eval(r.replace(':null',':None').replace(':false',':False').replace(':true',':True'))

    c.execute(u'SELECT * FROM customers')
    return pd.DataFrame([parse[n] for _,n in c.fetchall()])

