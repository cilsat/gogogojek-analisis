#!/usr/bin/python2

import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime
from django.utils.dateparse import parse_datetime
import sys
import re
import json

client = MongoClient()
bookings = client['gojek']['bookings']

default_keys=['closingTime', 'cancelTime', 'addresses']

def pp_tables():
    # get table names
    c.execute("select name from sqlite_master where type='table';")
    names = c.fetchall()
    # function to parse rows
    p = re.compile(r'(?<![0-9]|:|,|\{)"(?![0-9]|:|,"|\})')
    #parse = lambda r: eval(p.sub('', r[r.find('{'):r.rfind('}')+1].replace(':""',':None').replace(':null',':None').replace(':true',':True').replace(':false',':False')))
    parse = json.loads

    tables = {}
    errors = {}
    for t in names:
        c.execute("select * from %s" % t)
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
    
def get_bookings(keys=None, chunksize=100000):
    import MySQLdb as sq

    db = sq.connect(host='localhost', user='root', passwd='root', db='gojek', unix_socket='/run/mysqld/mysqld.sock')
    c = db.cursor()

    p = re.compile(r'(?<![0-9]|:|,|\{)"(?![0-9]|:|,"|\})')
    datetimes = ['timeField', 'cancelTime', 'feedbackTime']
    offset = 0
    while True:
        book = []
        success = 0
        fail = 0
        sql = "select * from bookings limit %d offset %d;" % (chunksize, offset)
        c.execute(sql)
        for n, r in c.fetchall():
            try:
                assert r.find('Internal Server Error') < 0
                start = r.find('routePolyline')
                end = r.find('driverCloseLocation')
                r = r[:start] + r[end:]
                row = json.loads(p.sub('', r), strict=False)

                if keys:
                    [row.pop(k) for k in row.keys() if k not in keys]

                for k in row.keys():
                    if k == 'timeField' and row[k]:
                        row[k] = parse_datetime(row[k])
                    elif k.endswith('Time') and row[k]:
                        try: row[k] = datetime.fromtimestamp(row[k]/1000)
                        except: row[k] = parse_datetime(row[k])
                    elif k == 'addresses':
                        add = row[k][0]
                        if add['closeTime']:
                            add['closeTime'] = parse_datetime(add['closeTime'])
                        if add['latLongDestination']:
                            lat, lon = add['latLongDestination'].split(',')
                            row['latDestination'] = float(lat)
                            row['longDestination'] = float(lon)
                            row.pop(k)

                book.append(row)
                success += 1
            except:
                fail += 1
        try:
            bookings.insert_many(book)
            print(str(offset) + ': success')
            print('parsed / failed')
            print(str(success) + ' / ' + str(fail))
        except:
            print(str(offset) + ': failed')

        if success+fail < chunksize: break
        offset += chunksize

def pp_addresses():
    add = pd.DataFrame([a for a in addresses.find({}, {
        '_id':0, 'closeTime':1, 'latDestination':1, 'longDestination':1})])

    jkt = [-6.21462, 106.84513]


def pp_bookings():
    loc = []
    cancelled = 0
    for b in book:
        if b:
            try:
                loc.append(b.values()[0].split(','))
            except:
                cancelled += 1
    print('booked / cancelled')
    print(str(len(loc)) + ' / ' + str(len(cancelled)))

    loc = np.array(loc, dtype=float)
    jkt = [-6.21462, 106.84513]
    loc = loc[np.all(loc - jkt < 1., axis=-1)]
    return pd.DataFrame(loc, columns=['lat', 'long'])

def agg_bookings(req, res):
    lat_from = req['lat_from']
    long_from = req['long_from']
    lat_to = req['lat_to']
    long_to = req['long_to']
    time_from = req['time_from']
    time_to = req['time_to']
    cell = req['n_items']**0.5

    day = datetime(2015,11,23)
    start = day + timedelta(hours=time_from)
    end = day + timedelta(hours=time_to)
    df = self.data.loc[(self.data.idTime > start) &
            (self.data.idTime < end) &
            (self.data.latOrigin > lat_from) &
            (self.data.latOrigin < lat_to) &
            (self.data.longOrigin > long_from) &
            (self.data.longOrigin < long_to),
            ['latOrigin', 'longOrigin']]

    # compare each location to a grid and calc which cell it's closest to
    x = np.linspace(lat_from, lat_to, cell)
    y = np.linspace(long_from, long_to, cell)
    dx = x[np.abs(np.subtract.outer(df.latOrigin, x)).argmin(axis=-1)]
    dy = y[np.abs(np.subtract.outer(df.longOrigin, y)).argmin(axis=-1)]

    count = df.groupby([dx, dy]).latOrigin.count()
    count[:] = (count.astype(float)/count.max())**0.5
    return [[k[0], k[1], v] for k, v in count.iteritems()]

