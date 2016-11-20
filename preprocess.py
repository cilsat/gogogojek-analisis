#!/usr/bin/python2

import pandas as pd
import numpy as np
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
    
def get_bookings(keys=['dispatchTime', 'arrivalTime', 'closingTime', 'driverCalledTime', 'cancelTime', 'timeField', 'driverLatitude', 'driverLongitude', 'driverPickupLocation', 'driverCloseLocation']):
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

def pp_bookings():
    book = get_bookings(keys=['driverPickupLocation'])
    book = [b.values()[0] for b in book]
    loc = np.array([b.split(',') for b in book if b], dtype=float)
    jkt = [-6.21462, 106.84513]
    loc = loc[np.all(loc - jkt < 1., axis=-1)]
    return pd.DataFrame(loc, columns=['lat', 'long'])

def agg_bookings(df_in, json_out='heat.json', ncell=2000):
    cell = ncell**0.5
    
    # compare each location to a grid and calc which cell it's closest to
    x = np.abs(np.subtract.outer(df_in.lat.values,
        np.linspace(df_in.lat.min(), df_in.lat.max(), cell)))
    y = np.abs(np.subtract.outer(df_in.long.values,
        np.linspace(df_in.long.min(), df_in.long.max(), cell)))

    df_in['x'] = x.argmin(axis=-1)
    df_in['y'] = y.argmin(axis=-1)

    count = df_in.groupby(['x','y']).lat.count()

    if json_out:
        count.to_json(json_out)
    else:
        return count

